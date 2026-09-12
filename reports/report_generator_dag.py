"""
Report generator — ONE file that builds EVERY Redash-driven Slack report DAG from a registry.

Add a new report with NO code change / NO PR: create its config + message queries in Redash, then
add a row to the REPORT_REGISTRY Airflow Variable. This file loops the registry and emits one DAG
per entry; each DAG just: read its config -> gate on time (+ optional day-of-week) -> run its
message query -> post. ALL logic + layout + timing live in Redash.

Registry = JSON list; each entry:
  {"dag_id": "...", "config_query_id": 123, "state_var": "...", "tags": ["..."]?}
  * config query columns: channel_id, trigger_hour, trigger_minute, message_query_id, trigger_dow?
    (trigger_dow = isoweekday 1=Mon..7=Sun; when set the DAG fires only that weekday)
  * message query returns a single `message` column = the ENTIRE Slack text (title + table).

IMPORTANT: REPORT_REGISTRY MUST be an ENV-BACKED Variable (provision as AIRFLOW_VAR_REPORT_REGISTRY)
so this file parses cheaply every ~30s with no metadata-DB hit. If it is unset/blank/invalid the
built-in DEFAULT_REGISTRY below is used, so the DAGs still build. Never point REPORT_REGISTRY at a
plain (DB-backed) Variable — that would hit the metadata DB on every parse.

Env REPORT_TEST_CHANNEL overrides the destination channel for ALL generated reports (testing only;
unset in prod). Each DAG ticks every 15 min and fires ONCE/day at its config time, guarded by a
per-DAG Airflow Variable (state_var). Ships paused.
"""

from datetime import timedelta
import logging, os, json

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)

# Built-in fallback (used when REPORT_REGISTRY Variable is unset/invalid). Adding a report in prod
# should be done by editing the Variable, not this list.
DEFAULT_REGISTRY = [
    {"dag_id": "l3_eod_report",      "config_query_id": 47194, "state_var": "L3_EOD_STATE",       "tags": ["l3"]},
    {"dag_id": "l3_weekly_report",   "config_query_id": 47574, "state_var": "L3_WEEKLY_STATE",    "tags": ["l3", "weekly"]},
    {"dag_id": "l3_morning_report",  "config_query_id": 47887, "state_var": "L3_MORNING_STATE",   "tags": ["l3"]},
    {"dag_id": "social_weekly_report", "config_query_id": 47674, "state_var": "SOCIAL_WEEKLY_STATE", "tags": ["social", "weekly"]},
]

TEST_CHANNEL_ENV = 'REPORT_TEST_CHANNEL'   # when set, ALL reports post here (testing)


def _registry():
    """Read REPORT_REGISTRY (env-backed) at parse time; fall back to DEFAULT_REGISTRY on
    missing/blank/invalid so a bad Variable can never break DAG parsing."""
    try:
        raw = Variable.get('REPORT_REGISTRY', default_var=None)
    except Exception:
        raw = None
    if not raw:
        return DEFAULT_REGISTRY
    try:
        reg = json.loads(raw)
    except Exception:
        logger.warning('REPORT_REGISTRY is not valid JSON; using DEFAULT_REGISTRY')
        return DEFAULT_REGISTRY
    if not isinstance(reg, list) or not reg:
        return DEFAULT_REGISTRY
    return reg


def _cfg(cfg, key, default=None):
    return next((r.get(key) for r in (cfg or []) if r.get(key) not in (None, '')), default)


def _int(cfg, key, default):
    try:
        return int(_cfg(cfg, key, default))
    except Exception:
        return default


def _load_state(Variable, state_var):
    """Gate state dict. A genuinely-absent Variable = fresh ({}); any read or JSON error RAISES so a
    transient blip can't reopen the gate and re-post an already-delivered report (PR #1454 review)."""
    raw = Variable.get(state_var, default_var=None)
    if raw is None:
        return {}
    state = json.loads(raw)
    if not isinstance(state, dict):
        raise ValueError('state var %s is not a dict: %r' % (state_var, state))
    return state


def run_report(config_query_id, state_var, **context):
    # Heavy / credential-bearing deps imported lazily so a DAG parse mid plugin-sync can't
    # ImportError the file (AGENTS.md: keep runtime-only SDKs in the task).
    from airflow.models import Variable as V
    from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL, SLACK_BOT_TOKEN_ALERTS
    from utils.slack.redash_client import RedashClient
    from utils.slack.slack_client import SlackNotifier

    logger.info('REPORT (config #%s, state %s)', config_query_id, state_var)
    redash = RedashClient(REDASH_API_KEY, REDASH_BASE_URL)

    cfg = redash.fetch_query_results(config_query_id) or []
    channel = os.getenv(TEST_CHANNEL_ENV) or _cfg(cfg, 'channel_id')
    if not channel:
        raise Exception('config %s has no channel_id and %s is unset' % (config_query_id, TEST_CHANNEL_ENV))
    msg_qid = int(_cfg(cfg, 'message_query_id'))
    trig_hour = _int(cfg, 'trigger_hour', 23)
    trig_min  = _int(cfg, 'trigger_minute', 30)
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

    rows = redash.fetch_query_results(msg_qid) or []
    message = (rows[0].get('message') if rows else None)
    if not message:
        raise Exception('message query %s returned no `message`' % msg_qid)

    SlackNotifier(SLACK_BOT_TOKEN_ALERTS).send_message(
        message, channel_id=channel, unfurl_links=False, unfurl_media=False)

    # Marked only AFTER a confirmed post, so a failed message-query/post reopens the gate on the
    # next tick (never a silent skip). Residual dup window = a worker crash before this write.
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
    dag_id = entry['dag_id']
    dag = DAG(
        dag_id,
        default_args=default_args,
        description='Generic poster (registry-driven): config #%s -> message query in Redash' % entry['config_query_id'],
        schedule_interval='*/15 * * * *',
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=['slack', 'report', 'cs_team'] + list(entry.get('tags') or []),
    )
    PythonOperator(
        task_id='run_report', python_callable=run_report,
        op_kwargs={'config_query_id': int(entry['config_query_id']), 'state_var': entry['state_var']},
        dag=dag,
    )
    return dag


# Airflow discovers DAG objects that live in module globals — emit one per registry entry.
for _entry in _registry():
    try:
        globals()[_entry['dag_id']] = _build(_entry)
    except Exception:
        logger.exception('skipping bad registry entry: %r', _entry)
