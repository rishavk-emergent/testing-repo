"""
L3 EOD Report — one Slack code-block table, once a day (config-driven time, IST).

A single monospace table posted to Slack at end of day: OPEN / PENDING / CLOSED-TODAY counts for
L3 tickets tagged needs_review and real_l3, with real_l3 broken out by owning team beneath it.
OPEN matches the live Trinity buckets (status=OPEN); pending + closed-today are separate columns.

ALL data logic (filters, tags, team resolution, open/pending/closed-today) lives in Redash so it is
editable with no code push:
  * data   #47167  [L3 EOD] Daily report data   -> section/label/open_count/pending_count/closed_today
  * config #47194  [L3 EOD] config              -> channel_id, trigger_hour, trigger_minute, data_query_id

The DAG ticks every 15 min and an in-task gate fires ONCE/day at trigger_hour:trigger_minute IST
(guarded by an Airflow Variable), so the fire time is changeable in Redash without touching code.
Env L3_EOD_SLACK_CHANNEL overrides the destination channel for testing. Ships paused.
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

CONFIG_QUERY_ID = 47194
DATA_QUERY_ID   = 47167                     # fallback; config.data_query_id overrides
STATE_VAR       = 'L3_EOD_STATE'            # Airflow Variable: {'_last_fire_date': 'YYYY-MM-DD'}
ENV_CHANNEL     = os.getenv('L3_EOD_SLACK_CHANNEL')   # test override; unset in prod
FALLBACK_CHANNEL = 'C0B4CHB1PRD'            # #daily-report-l3-escalations
TRIG_HOUR, TRIG_MIN = 23, 30                # fallbacks (23:30 IST)

# real_l3 team display order (others appended after, alphabetical)
TEAM_ORDER = ['Conversion Team', 'Retention Team', 'Expo Team', 'Deployment Team', 'Wingman Team', 'Team Untagged']
COLS = ['Open', 'Pending', 'Closed Today']


def redash_run(query_id, parameters=None, max_wait=90):
    h = {'Authorization': 'Key %s' % REDASH_API_KEY, 'Content-Type': 'application/json'}
    job = requests.post('%s/api/queries/%s/results' % (REDASH_BASE_URL, query_id),
                        json={'parameters': parameters or {}, 'max_age': 0}, headers=h, timeout=60).json()
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


def _cfg_val(cfg, key, default=None):
    return next((r.get(key) for r in (cfg or []) if r.get(key) not in (None, '')), default)


def build_table(rows):
    """Single code-block table: Needs Review, Real L3, then real_l3 teams indented beneath."""
    nr = next((r for r in rows if r.get('section') == 'a_needs_review'), {}) or {}
    rl = next((r for r in rows if r.get('section') == 'b_real_l3'), {}) or {}
    teams = [r for r in rows if r.get('section') == 'c_team'
             and ((r.get('open_count') or 0) or (r.get('pending_count') or 0) or (r.get('closed_today') or 0))]
    teams.sort(key=lambda x: (TEAM_ORDER.index(x['label']) if x['label'] in TEAM_ORDER else 90, x.get('label') or ''))

    def vals(r):
        return [r.get('open_count') or 0, r.get('pending_count') or 0, r.get('closed_today') or 0]

    labels = ['Needs Review', 'Real L3'] + ['  · ' + (t.get('label') or '-') for t in teams]
    w0 = max([len(x) for x in labels] + [len('Segment')])

    def line(lab, v):
        return '  '.join([lab.ljust(w0)] + [str(x).rjust(len(c)) for x, c in zip(v, COLS)])

    header = '  '.join(['Segment'.ljust(w0)] + COLS)
    out = [header, '─' * len(header), line('Needs Review', vals(nr)), line('Real L3', vals(rl))]
    for t in teams:
        out.append(line('  · ' + (t.get('label') or '-'), vals(t)))
    return '\n'.join(out)


def slack_post(channel, text):
    d = requests.post('https://slack.com/api/chat.postMessage',
                      headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN,
                               'Content-Type': 'application/json; charset=utf-8'},
                      json={'channel': channel, 'text': text, 'unfurl_links': False, 'unfurl_media': False},
                      timeout=30).json()
    if not d.get('ok'):
        raise Exception('chat.postMessage failed: %s' % d.get('error'))
    return d['ts']


def run_l3_eod(**context):
    from airflow.models import Variable
    logger.info('L3 EOD REPORT')

    cfg = redash_run(CONFIG_QUERY_ID) or []
    channel = ENV_CHANNEL or _cfg_val(cfg, 'channel_id', FALLBACK_CHANNEL)
    data_qid = int(_cfg_val(cfg, 'data_query_id', DATA_QUERY_ID))
    try:
        trig_hour = int(_cfg_val(cfg, 'trigger_hour', TRIG_HOUR))
    except Exception:
        trig_hour = TRIG_HOUR
    try:
        trig_min = int(_cfg_val(cfg, 'trigger_minute', TRIG_MIN))
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
        logger.info('L3 EOD: gate closed, exiting')
        return

    rows = redash_run(data_qid) or []
    table = build_table(rows)
    msg = ':clipboard: *L3 — EOD Report · %s (IST)*\n\n```\n%s\n```' % (now.format('dddd, DD MMM YYYY'), table)
    slack_post(channel, msg)

    state['_last_fire_date'] = today_key   # mark done only after a successful post
    Variable.set(STATE_VAR, json.dumps(state))
    logger.info('L3 EOD: posted to %s', channel)


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
    description='EOD L3 report (needs_review + real_l3 open/pending/closed-today, real_l3 by team) to Slack',
    schedule_interval='*/15 * * * *',   # ticks every 15 min IST; in-task gate fires once/day at config time
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,        # unpause after first validated run
    tags=['slack', 'trinity', 'l3', 'report', 'cs_team'],
)
PythonOperator(task_id='run_l3_eod', python_callable=run_l3_eod, dag=dag)
