"""
L3 EOD Report — a GENERIC Slack-poster shell. ALL logic + layout live in Redash; the DAG changes
never (no per-report code). It just: read config -> gate on time -> run the message query -> post.

Redash (edit freely, no code push):
  * config  #47194  [L3 EOD] config  -> channel_id, trigger_hour, trigger_minute, message_query_id
  * message #47195  [L3 EOD] message -> a single column `message` = the ENTIRE Slack text
                     (title + code-block table, all built in SQL). Change filters/columns/labels/layout there.

The DAG ticks every 15 min; an in-task gate fires ONCE/day at trigger_hour:trigger_minute IST
(guarded by an Airflow Variable) — so the fire time is changeable in Redash, not code.
Env L3_EOD_SLACK_CHANNEL overrides the destination for testing. Ships paused.
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

CONFIG_QUERY_ID  = 47194
STATE_VAR        = 'L3_EOD_STATE'
ENV_CHANNEL      = os.getenv('L3_EOD_SLACK_CHANNEL')
FALLBACK_CHANNEL = 'C0B4CHB1PRD'
FALLBACK_MSG_QID = 47195
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


def slack_post(channel, text):
    d = requests.post('https://slack.com/api/chat.postMessage',
                      headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN,
                               'Content-Type': 'application/json; charset=utf-8'},
                      json={'channel': channel, 'text': text, 'unfurl_links': False, 'unfurl_media': False},
                      timeout=30).json()
    if not d.get('ok'):
        raise Exception('chat.postMessage failed: %s' % d.get('error'))
    return d['ts']


def run_report(**context):
    from airflow.models import Variable
    logger.info('L3 EOD REPORT (generic shell)')

    cfg = redash_run(CONFIG_QUERY_ID) or []
    channel = ENV_CHANNEL or _cfg(cfg, 'channel_id', FALLBACK_CHANNEL)
    msg_qid = int(_cfg(cfg, 'message_query_id', FALLBACK_MSG_QID))
    try:
        trig_hour = int(_cfg(cfg, 'trigger_hour', TRIG_HOUR))
    except Exception:
        trig_hour = TRIG_HOUR
    try:
        trig_min = int(_cfg(cfg, 'trigger_minute', TRIG_MIN))
    except Exception:
        trig_min = TRIG_MIN

    try:
        state = json.loads(Variable.get(STATE_VAR))
        if not isinstance(state, dict):
            state = {}
    except Exception:
        state = {}

    now = pendulum.now('Asia/Kolkata')
    today_key = now.format('YYYY-MM-DD')
    time_reached = (now.hour * 60 + now.minute) >= (trig_hour * 60 + trig_min)
    already = (state.get('_last_fire_date') == today_key)
    fire = time_reached and not already
    logger.info('[gate] IST %s %02d:%02d target=%02d:%02d reached=%s fired_today=%s -> fire=%s',
                today_key, now.hour, now.minute, trig_hour, trig_min, time_reached, already, fire)
    if not fire:
        logger.info('gate closed, exiting')
        return

    rows = redash_run(msg_qid) or []
    message = (rows[0].get('message') if rows else None)
    if not message:
        raise Exception('message query %s returned no `message`' % msg_qid)
    slack_post(channel, message)

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
    'l3_eod_report',
    default_args=default_args,
    description='Generic Slack poster: posts the message built entirely in Redash (#47195), once/day at config time',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['slack', 'trinity', 'l3', 'report', 'cs_team'],
)
PythonOperator(task_id='run_report', python_callable=run_report, dag=dag)
