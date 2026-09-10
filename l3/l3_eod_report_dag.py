"""
L3 reports — a GENERIC Slack-poster shell driving TWO DAGs (daily + weekly). ALL logic + layout live
in Redash, so the DAG code never changes per report.

Each DAG just: read its config -> gate on time (+ optional day-of-week) -> run its message query -> post.
  * l3_eod_report     (daily)   -> config #47194 -> message #47195  ([L3 EOD] table)
  * l3_weekly_report  (weekly)  -> config #47574 -> message #47573  ([L3 Weekly] table, Sun 11:30 IST)

Config columns (edit in Redash, no code push): channel_id, trigger_hour, trigger_minute, message_query_id,
and (weekly) trigger_dow — isoweekday 1=Mon..7=Sun; when set, the DAG only fires on that weekday.
The message query builds the ENTIRE Slack message (title + table) in pure SQL.

Both DAGs tick every 15 min; an in-task gate fires ONCE/day at the config time (guarded by a per-DAG
Airflow Variable). Env L3_EOD_SLACK_CHANNEL overrides the channel for testing. Ship paused.
"""

from datetime import timedelta
import logging, os, json, time

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack.slack_config import (
    REDASH_API_KEY, REDASH_BASE_URL,
    SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN,
)

logger = logging.getLogger(__name__)

ENV_CHANNEL      = os.getenv('L3_EOD_SLACK_CHANNEL')   # test override; unset in prod
FALLBACK_CHANNEL = 'C0B4CHB1PRD'
TRIG_HOUR, TRIG_MIN = 23, 30


def redash_run(query_id, max_wait=120):
    h = {'Authorization': 'Key %s' % REDASH_API_KEY, 'Content-Type': 'application/json'}
    job = requests.post('%s/api/queries/%s/results' % (REDASH_BASE_URL, query_id),
                        json={'parameters': {}, 'max_age': 0}, headers=h, timeout=60).json()
    if 'query_result' in job:
        return job['query_result']['data']['rows']
    jid = job['job']['id']
    for _ in range(max_wait):
        jr = requests.get('%s/api/jobs/%s' % (REDASH_BASE_URL, jid), headers=h, timeout=30).json()['job']
        if jr['status'] in (3, 4):
            if jr['status'] == 4:
                raise Exception('Redash query %s failed: %s' % (query_id, jr.get('error')))
            rid = jr['query_result_id']
            return requests.get('%s/api/query_results/%s.json' % (REDASH_BASE_URL, rid),
                                headers=h, timeout=30).json()['query_result']['data']['rows']
        time.sleep(2)
    raise Exception('Redash query %s timed out' % query_id)


def _cfg(cfg, key, default=None):
    return next((r.get(key) for r in (cfg or []) if r.get(key) not in (None, '')), default)


def _int(cfg, key, default):
    try:
        return int(_cfg(cfg, key, default))
    except Exception:
        return default


def slack_post(channel, text):
    d = requests.post('https://slack.com/api/chat.postMessage',
                      headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN,
                               'Content-Type': 'application/json; charset=utf-8'},
                      json={'channel': channel, 'text': text, 'unfurl_links': False, 'unfurl_media': False},
                      timeout=30).json()
    if not d.get('ok'):
        raise Exception('chat.postMessage failed: %s' % d.get('error'))
    return d['ts']


def run_report(config_query_id, state_var, **context):
    from airflow.models import Variable
    logger.info('L3 REPORT (config #%s)', config_query_id)

    cfg = redash_run(config_query_id) or []
    channel = ENV_CHANNEL or _cfg(cfg, 'channel_id', FALLBACK_CHANNEL)
    msg_qid = int(_cfg(cfg, 'message_query_id'))
    trig_hour = _int(cfg, 'trigger_hour', TRIG_HOUR)
    trig_min  = _int(cfg, 'trigger_minute', TRIG_MIN)
    trig_dow  = _cfg(cfg, 'trigger_dow')          # optional; isoweekday 1..7, None = every day

    try:
        state = json.loads(Variable.get(state_var))
        if not isinstance(state, dict):
            state = {}
    except Exception:
        state = {}

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

    rows = redash_run(msg_qid) or []
    message = (rows[0].get('message') if rows else None)
    if not message:
        raise Exception('message query %s returned no `message`' % msg_qid)
    slack_post(channel, message)

    state['_last_fire_date'] = today_key
    Variable.set(state_var, json.dumps(state))
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

# ---- daily (config #47194, fires every day at its config time) ----
dag_daily = DAG(
    'l3_eod_report',
    default_args=default_args,
    description='Generic poster: L3 EOD table built in Redash (#47195), once/day at config #47194 time',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['slack', 'trinity', 'l3', 'report', 'cs_team'],
)
PythonOperator(task_id='run_report', python_callable=run_report,
               op_kwargs={'config_query_id': 47194, 'state_var': 'L3_EOD_STATE'}, dag=dag_daily)

# ---- weekly (config #47574, fires only on trigger_dow, e.g. Sunday, at its config time) ----
dag_weekly = DAG(
    'l3_weekly_report',
    default_args=default_args,
    description='Generic poster: L3 Weekly table built in Redash (#47573), once on config day-of-week+time (#47574)',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['slack', 'trinity', 'l3', 'report', 'weekly', 'cs_team'],
)
PythonOperator(task_id='run_report', python_callable=run_report,
               op_kwargs={'config_query_id': 47574, 'state_var': 'L3_WEEKLY_STATE'}, dag=dag_weekly)
