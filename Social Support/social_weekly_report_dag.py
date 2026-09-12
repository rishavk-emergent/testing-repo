"""
Social weekly report — DB-FREE, fully config-driven. Reads #social-support live at report time and
posts a per-platform reaction table. EVERYTHING (knobs + the channel-scan/bucket/render logic) lives
in the Redash config query #47889 — the DAG is a thin shell that gates on time and exec()s the `code`
column, so behaviour changes with no code push (the L3-style "logic in the query" model, adapted:
Slack isn't queryable by Redash, so the query ships the *code* the DAG runs against Slack).

Flow: read config #47889 -> gate (Mon 10:00 IST, once/day) -> exec `code` with an injected context
{cfg, slack_call(method,params), now, json} -> the code sets `message` -> post via the alerts bot.
Window (previous week Mon–Sun) + emoji->bucket mapping are computed inside `code`.

NOTE: this DAG runs Python fetched from Redash via exec(). That is a deliberate design choice for this
report (all logic in the query); it also means the code in #47889 is NOT covered by this repo's
ruff/pytest/PR review — edit it with the same care as shipping code. Env SOCIAL_WEEKLY_SLACK_CHANNEL
overrides the destination for testing. Ships paused.
"""

from datetime import timedelta
import logging, os, json

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

CONFIG_QUERY_ID = 47889
STATE_VAR       = 'SOCIAL_WEEKLY_STATE'
ENV_CHANNEL     = os.getenv('SOCIAL_WEEKLY_SLACK_CHANNEL')   # test override; unset in prod
TRIG_HOUR, TRIG_MIN, TRIG_DOW = 10, 0, 1                     # Mon 10:00 IST


def _load_state(V, state_var):
    """Gate state dict. Absent Variable = fresh ({}); any read/JSON error RAISES so a transient blip
    can't reopen the gate and re-post an already-delivered report (same contract as the L3 posters)."""
    raw = V.get(state_var, default_var=None)
    if raw is None:
        return {}
    state = json.loads(raw)
    if not isinstance(state, dict):
        raise ValueError('state var %s is not a dict: %r' % (state_var, state))
    return state


def run_report(**context):
    from airflow.models import Variable
    from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL, SLACK_BOT_TOKEN_ALERTS
    from utils.slack.redash_client import RedashClient
    from utils.slack.slack_client import SlackNotifier
    import urllib.request
    import urllib.parse

    redash = RedashClient(REDASH_API_KEY, REDASH_BASE_URL)
    rows = redash.fetch_query_results(CONFIG_QUERY_ID) or []
    if not rows:
        raise Exception('config query %s returned no rows' % CONFIG_QUERY_ID)
    cfg = rows[0]

    def _int(key, default):
        try:
            return int(cfg.get(key))
        except Exception:
            return default

    trig_hour = _int('trigger_hour', TRIG_HOUR)
    trig_min  = _int('trigger_minute', TRIG_MIN)
    trig_dow  = cfg.get('trigger_dow')
    channel = ENV_CHANNEL or cfg.get('report_channel_id')
    if not channel:
        raise Exception('no report_channel_id in config and SOCIAL_WEEKLY_SLACK_CHANNEL unset')

    state = _load_state(Variable, STATE_VAR)
    now = pendulum.now('Asia/Kolkata')
    today_key = now.format('YYYY-MM-DD')
    dow_ok = (trig_dow in (None, '')) or (now.isoweekday() == int(trig_dow))
    time_reached = (now.hour * 60 + now.minute) >= (trig_hour * 60 + trig_min)
    already = (state.get('_last_fire_date') == today_key)
    fire = dow_ok and time_reached and not already
    logger.info('[gate] IST %s dow=%d %02d:%02d target=dow%s %02d:%02d dow_ok=%s reached=%s fired=%s -> fire=%s',
                today_key, now.isoweekday(), now.hour, now.minute, trig_dow, trig_hour, trig_min,
                dow_ok, time_reached, already, fire)
    if not fire:
        logger.info('gate closed, exiting')
        return

    def slack_call(method, params):
        url = 'https://slack.com/api/%s?%s' % (method, urllib.parse.urlencode(params))
        req = urllib.request.Request(url, headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN_ALERTS})
        return json.loads(urllib.request.urlopen(req, timeout=30).read())

    # Behaviour lives in the config query (per design): exec its `code`, which scans the channel and
    # sets `message`. Injected context is the ONLY surface the code may rely on.
    ns = {'cfg': cfg, 'slack_call': slack_call, 'now': now, 'json': json}
    exec(cfg['code'], ns)  # noqa: S102 — intentional: report logic ships in Redash config #47889
    message = ns.get('message')
    if not message:
        raise Exception('config %s `code` did not set `message`' % CONFIG_QUERY_ID)

    SlackNotifier(SLACK_BOT_TOKEN_ALERTS).send_message(
        message, channel_id=channel, unfurl_links=False, unfurl_media=False)

    state['_last_fire_date'] = today_key
    Variable.set(STATE_VAR, json.dumps(state))
    logger.info('posted to %s (exec config %s)', channel, CONFIG_QUERY_ID)


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
    description='DB-free social weekly report: exec channel-scan code from Redash #47889, Mon 10:00 IST',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['slack', 'social', 'brand24', 'report', 'weekly', 'cs_team'],
)
PythonOperator(task_id='run_report', python_callable=run_report, dag=dag)
