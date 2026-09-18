"""
L3 reports — a GENERIC, REGISTRY-DRIVEN Slack-poster shell. ONE file builds every L3 report DAG from a
static REGISTRY list below. ALL per-report logic + layout + timing live in Redash (config + message
queries), so a *content* change never touches this file; adding a whole new L3 report is a one-line
entry in REGISTRY + a PR.

Each generated DAG: read its config -> gate on time (+ optional day-of-week) -> run its message query -> post.
  * l3_eod_report     (daily)   -> config #48522 -> message #47886  ([L3 Report] table, 23:30 IST)
  * l3_morning_report (daily)   -> config #47887 -> message #47886  ([L3 Report] table, 11:30 IST)
  * l3_weekly_report  (weekly)  -> config #47574 -> message #47573  ([L3 Weekly] table, Sun 11:30 IST)
  (morning + eod share ONE message query #47886 — same 'L3 Report' table: Open/Pending + Closed 24h/48h.)

Config columns (edit in Redash, no code push): channel_id, trigger_hour, trigger_minute, message_query_id,
and trigger_dow — isoweekday 1=Mon..7=Sun; when set the DAG fires only that weekday. The message query
builds the ENTIRE Slack message (title + table) in pure SQL.

REGISTRY is a plain in-file list (no Variable / DB read at parse time — Airflow parses this every ~30s,
so DAG top level must stay DB/API-free). All DAGs tick every 15 min; a per-DAG in-task gate fires
ONCE/day at the config time (guarded by state_var). Env L3_EOD_SLACK_CHANNEL overrides the channel for
testing. Ships paused.
"""

from datetime import timedelta
import logging, os, json

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

ENV_CHANNEL      = os.getenv('L3_EOD_SLACK_CHANNEL')   # test override; unset in prod
FALLBACK_CHANNEL = 'C0B4CHB1PRD'
TRIG_HOUR, TRIG_MIN = 23, 30

# The L3 reports this file builds. Add a report = one entry here (its config + message queries live in
# Redash). Kept as a plain list on purpose: read at parse time with zero metadata-DB hit.
REGISTRY = [
    {"dag_id": "l3_eod_report",     "config_query_id": 48522, "state_var": "L3_EOD_STATE",     "tags": []},
    {"dag_id": "l3_weekly_report",  "config_query_id": 47574, "state_var": "L3_WEEKLY_STATE",  "tags": ["weekly"]},
    {"dag_id": "l3_morning_report", "config_query_id": 47887, "state_var": "L3_MORNING_STATE", "tags": []},
]


def _cfg(cfg, key, default=None):
    return next((r.get(key) for r in (cfg or []) if r.get(key) not in (None, '')), default)


def _int(cfg, key, default):
    try:
        return int(_cfg(cfg, key, default))
    except Exception:
        return default


def _load_state(V, state_var):
    """Return the gate state dict. A genuinely-absent Variable = fresh ({}); any read or
    JSON error RAISES so a transient metadata-DB / parse blip can't reopen the gate and
    re-post an already-delivered report (see PR #1454 idempotency review)."""
    raw = V.get(state_var, default_var=None)   # only a missing key -> None
    if raw is None:
        return {}
    state = json.loads(raw)                     # bad JSON -> raise, don't reset
    if not isinstance(state, dict):
        raise ValueError('state var %s is not a dict: %r' % (state_var, state))
    return state


def run_report(config_query_id, state_var, **context):
    # Heavy / credential-bearing deps imported lazily so a DAG parse mid plugin-sync
    # can't ImportError the whole file (AGENTS.md: keep runtime-only SDKs in the task).
    from airflow.models import Variable as V
    from utils.slack.slack_config import (
        REDASH_API_KEY, REDASH_BASE_URL, SLACK_BOT_TOKEN_ALERTS,
    )
    from utils.slack.redash_client import RedashClient
    from utils.slack.slack_client import SlackNotifier

    logger.info('L3 REPORT (config #%s)', config_query_id)
    redash = RedashClient(REDASH_API_KEY, REDASH_BASE_URL)

    cfg = []
    try:
        cfg = redash.fetch_query_results(config_query_id) or []
    except Exception as e:
        logger.warning('config query %s fetch failed, using defaults: %s', config_query_id, e)
    channel = ENV_CHANNEL or _cfg(cfg, 'channel_id', FALLBACK_CHANNEL)
    trig_hour = _int(cfg, 'trigger_hour', TRIG_HOUR)
    trig_min  = _int(cfg, 'trigger_minute', TRIG_MIN)
    trig_dow  = _cfg(cfg, 'trigger_dow')          # optional; isoweekday 1..7, None = every day

    state = _load_state(V, state_var)

    now = pendulum.now('Asia/Kolkata')
    today_key = now.format('YYYY-MM-DD')
    dow_ok = (trig_dow is None) or (now.isoweekday() == int(trig_dow))
    time_reached = (now.hour * 60 + now.minute) >= (trig_hour * 60 + trig_min)
    already = (state.get('_last_fire_date') == today_key)
    fire = dow_ok and time_reached and not already
    logger.info('[gate] IST %s dow=%d %02d:%02d target=%02d:%02d dow_req=%s dow_ok=%s reached=%s fired=%s -> fire=%s',
                today_key, now.isoweekday(), now.hour, now.minute, trig_hour, trig_min, trig_dow, dow_ok, time_reached, already, fire)
    if not fire:
        logger.info('gate closed, exiting')
        return

    # Resolved past the gate: it has no default, so an unreachable config query must
    # not blow up a non-trigger tick.
    msg_qid = int(_cfg(cfg, 'message_query_id'))

    rows = redash.fetch_query_results(msg_qid) or []
    message = (rows[0].get('message') if rows else None)
    if not message:
        raise Exception('message query %s returned no `message`' % msg_qid)

    SlackNotifier(SLACK_BOT_TOKEN_ALERTS).send_message(
        message, channel_id=channel, unfurl_links=False, unfurl_media=False)

    # Marked only AFTER a confirmed post, so a failed message-query/post reopens the gate
    # on the next tick (never a silent skip). Residual dup window = a worker crash in the
    # sub-second gap before this write; irreducible without Slack-side dedup.
    state['_last_fire_date'] = today_key
    V.set(state_var, json.dumps(state))
    logger.info('posted to %s (message query %s)', channel, msg_qid)


default_args = {
    'owner': 'rishav.k@emergent.sh',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 9, 1, tz='Asia/Kolkata'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


def _build(entry):
    dag = DAG(
        entry['dag_id'],
        default_args=default_args,
        description='Generic poster (registry-driven): L3 table built in Redash, config #%s' % entry['config_query_id'],
        schedule_interval='*/15 * * * *',
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=['slack', 'trinity', 'l3', 'report', 'cs_team'] + list(entry.get('tags') or []),
    )
    PythonOperator(
        task_id='run_report', python_callable=run_report,
        op_kwargs={'config_query_id': int(entry['config_query_id']), 'state_var': entry['state_var']},
        dag=dag,
    )
    return dag


# Airflow discovers DAG objects that live in module globals — emit one per registry entry.
for _entry in REGISTRY:
    globals()[_entry['dag_id']] = _build(_entry)
