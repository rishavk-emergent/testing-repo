"""
Social Support — WEEKLY report poster. A generic Slack-poster shell: read config -> gate on
day-of-week + time -> run the message query -> post. ALL logic + layout live in Redash, so the
DAG never changes per report.
  config  #47674  ([Social] Weekly report config)   -> Monday 10:00 IST, channel, message_query_id
  message #47673  ([Social] Weekly report message)  -> builds the ENTIRE Slack table (last Mon–Sun)

Config columns (edit in Redash, no code push): channel_id, trigger_hour, trigger_minute,
trigger_dow (isoweekday 1=Mon..7=Sun), message_query_id. The window (last complete Mon–Sun) is
computed inside the message query. Env SOCIAL_WEEKLY_SLACK_CHANNEL overrides the channel for testing.

Ticks every 15 min; an in-task gate fires ONCE on the configured weekday+time, guarded by an
Airflow Variable (SOCIAL_WEEKLY_STATE). Posts via the shared alerts bot. Ships paused.
"""

from datetime import timedelta
import logging, os, json

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

CONFIG_QUERY_ID  = 47674
STATE_VAR        = 'SOCIAL_WEEKLY_STATE'
ENV_CHANNEL      = os.getenv('SOCIAL_WEEKLY_SLACK_CHANNEL')   # test override; unset in prod
FALLBACK_CHANNEL = 'C0AHXA20DHS'                              # #social-support
TRIG_HOUR, TRIG_MIN, TRIG_DOW = 10, 0, 1                      # Monday 10:00 IST


def _cfg(cfg, key, default=None):
    return next((r.get(key) for r in (cfg or []) if r.get(key) not in (None, '')), default)


def _int(cfg, key, default):
    try:
        return int(_cfg(cfg, key, default))
    except Exception:
        return default


def _load_state(Variable, state_var):
    """Return the gate state dict. A genuinely-absent Variable = fresh ({}); any read or JSON
    error RAISES so a transient blip can't reopen the gate and re-post an already-delivered
    report. (Same idempotency contract as the L3 reports; PR #1454 review.)"""
    raw = Variable.get(state_var, default_var=None)
    if raw is None:
        return {}
    state = json.loads(raw)
    if not isinstance(state, dict):
        raise ValueError('state var %s is not a dict: %r' % (state_var, state))
    return state


def run_report(**context):
    # Heavy / credential-bearing deps imported lazily so a DAG parse mid plugin-sync can't
    # ImportError the file (AGENTS.md: keep runtime-only SDKs in the task).
    from airflow.models import Variable
    from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL, SLACK_BOT_TOKEN_ALERTS
    from utils.slack.redash_client import RedashClient
    from utils.slack.slack_client import SlackNotifier

    logger.info('SOCIAL WEEKLY REPORT (config #%s)', CONFIG_QUERY_ID)
    redash = RedashClient(REDASH_API_KEY, REDASH_BASE_URL)

    cfg = redash.fetch_query_results(CONFIG_QUERY_ID) or []
    channel = ENV_CHANNEL or _cfg(cfg, 'channel_id', FALLBACK_CHANNEL)
    msg_qid = int(_cfg(cfg, 'message_query_id'))
    trig_hour = _int(cfg, 'trigger_hour', TRIG_HOUR)
    trig_min  = _int(cfg, 'trigger_minute', TRIG_MIN)
    trig_dow  = _int(cfg, 'trigger_dow', TRIG_DOW)   # isoweekday 1..7

    state = _load_state(Variable, STATE_VAR)

    now = pendulum.now('Asia/Kolkata')
    today_key = now.format('YYYY-MM-DD')
    dow_ok = (now.isoweekday() == trig_dow)
    time_reached = (now.hour * 60 + now.minute) >= (trig_hour * 60 + trig_min)
    already = (state.get('_last_fire_date') == today_key)
    fire = dow_ok and time_reached and not already
    logger.info('[gate] IST %s dow=%d %02d:%02d target=dow%d %02d:%02d dow_ok=%s reached=%s fired=%s -> fire=%s',
                today_key, now.isoweekday(), now.hour, now.minute, trig_dow, trig_hour, trig_min,
                dow_ok, time_reached, already, fire)
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
    Variable.set(STATE_VAR, json.dumps(state))
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

dag = DAG(
    'social_weekly_report',
    default_args=default_args,
    description='Generic poster: #social-support weekly table built in Redash (#47673), Mon 10:00 IST (#47674)',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['slack', 'social', 'brand24', 'report', 'weekly', 'cs_team'],
)
PythonOperator(task_id='run_report', python_callable=run_report, dag=dag)
