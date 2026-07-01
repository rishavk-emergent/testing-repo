"""
RealL3 Escalation Hygiene Nag - Slack DAG (every 30 min, IST)

When an agent escalates a ticket to L3 and tags it `real_l3`, they are expected to, within 1
hour of that escalation: (1) set the team, (2) add the Slack link, (3) reply to the customer
themselves. This DAG runs every 30 min and, for each real_l3 ticket whose escalation was
SLA..WINDOW min ago and that still has ANY of those 3 missing, posts a Slack message
@-mentioning the escalator listing the missing item(s). Each ticket is pinged only once
(support.real_l3_hygiene_pinged).

ARCHITECTURE (per IST date, like prod_sos):
  * The first hygiene ping of the day posts a MASTER message to the channel:
        :rotating_light: *real_l3 escalation hygiene* (1 Jul 2026)
  * Every ping is posted as a THREADED REPLY under that day's master, @-mentioning the escalator:
        @escalator · #12345 — missing: *team, slack link*
        Please complete: set the team, add the Slack link, and reply to the customer.

WHERE THE LOGIC LIVES:
  * QUALIFYING FILTER + SLA/WINDOW/GRACE + ROUTING -> Redash query #HYGIENE_QUERY_ID
    (channel_id / channel_name / channel_tag are columns there; edit filters/routing there,
     no DAG change needed).
  * dedup + master/thread plumbing + escalator Slack lookup -> this DAG.

Schedule: '*/30 * * * *' Asia/Kolkata. Channel: from the query (community-builders-l2-l1);
override REAL_L3_HYGIENE_SLACK_CHANNEL env to force the test channel.
"""

from datetime import datetime, timedelta, timezone
import logging, os, json, urllib.request, urllib.parse

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack import RedashClient
from utils.slack.slack_config import (
    REDASH_API_KEY, REDASH_BASE_URL,
    SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN,
)
from utils.slack.bigquery_client import get_bigquery_client

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
# Channel + optional master @-mention come from the Redash query (channel_id / channel_name /
# channel_tag). Individual escalators are @-mentioned per-reply (resolved by email); channel_tag
# is an optional group ping on the master (blank by default = none).
#   * ENV override forces ALL pings to one channel (testing).
#   * FALLBACK_CHANNEL / DEFAULT_TAG used only if the query row leaves them blank.
ENV_CHANNEL_OVERRIDE = os.getenv('REAL_L3_HYGIENE_SLACK_CHANNEL')  # testing override; prod leaves unset
FALLBACK_CHANNEL     = 'C0937QNFJEM'   # community-builders-l2-l1
DEFAULT_TAG          = ''              # blank = no group @-mention on the master
HYGIENE_QUERY_ID     = 38914           # Redash: "[Real L3] escalation hygiene feed"
PING_TABLE           = 'emergent-default.support.real_l3_hygiene_pinged'   # per-ticket dedup
MASTER_TABLE         = 'emergent-default.support.real_l3_hygiene_master'   # one master per (IST date, channel)

# ==================== STATE TABLES ====================
DDL_PING = f"""
CREATE TABLE IF NOT EXISTS `{PING_TABLE}` (
  ticket_id STRING,
  num INT64,
  escalator_email STRING,
  missing STRING,
  pinged_at TIMESTAMP
)
"""
DDL_MASTER = f"""
CREATE TABLE IF NOT EXISTS `{MASTER_TABLE}` (
  ist_date STRING,
  channel STRING,
  thread_ts STRING,
  created_at TIMESTAMP
)
"""


# ==================== SLACK ====================

def slack_post(channel, text, thread_ts=None):
    """Post via chat.postMessage; returns the message ts. Raises on failure."""
    payload = {'channel': channel, 'text': text, 'unfurl_links': False, 'unfurl_media': False}
    if thread_ts:
        payload['thread_ts'] = thread_ts
    resp = requests.post(
        'https://slack.com/api/chat.postMessage',
        headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN,
                 'Content-Type': 'application/json; charset=utf-8'},
        json=payload, timeout=30,
    )
    data = resp.json()
    if not data.get('ok'):
        raise Exception('chat.postMessage failed: %s' % data.get('error'))
    return data['ts']


def _slack_uid(email, token):
    """Resolve an email to a Slack user id (needs users:read.email). None if not found."""
    if not email:
        return None
    try:
        url = 'https://slack.com/api/users.lookupByEmail?' + urllib.parse.urlencode({'email': email})
        req = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token})
        d = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
        return d['user']['id'] if d.get('ok') else None
    except Exception as e:
        logger.warning('lookupByEmail failed for %s: %s', email, e)
        return None


def _missing_list(row):
    miss = []
    if not row.get('team_done'):  miss.append('team')
    if not row.get('slack_done'): miss.append('slack link')
    if not row.get('reply_done'): miss.append('customer response')
    return miss


def build_master_text(ist_date, tag):
    """Day's master header; tag (from query) is blank by default -> no group @-mention."""
    try:
        label = datetime.strptime(ist_date, '%Y-%m-%d').strftime('%-d %b %Y')
    except Exception:
        label = ist_date
    prefix = (tag + ' ') if tag else ''
    return '%s:rotating_light: *real_l3 escalation hygiene* (%s)' % (prefix, label)


def build_reply_text(row, uid):
    """Threaded hygiene ping for one ticket (@-mentions the escalator)."""
    who  = '<@%s>' % uid if uid else (row.get('escalator_name') or row.get('escalator_email') or 'escalator')
    miss = ', '.join(_missing_list(row))
    link = '<%s|#%s>' % (row['ticket_url'], row['num'])
    return ('%s · %s — missing: *%s*\n'
            'Please complete: set the team, add the Slack link, and reply to the customer.'
            % (who, link, miss))


# ==================== MAIN TASK ====================

def run_real_l3_hygiene(**context):
    logger.info('=' * 60)
    logger.info('REAL_L3 HYGIENE: FETCH (Redash #%s) & PING', HYGIENE_QUERY_ID)
    logger.info('=' * 60)

    client = get_bigquery_client()

    logger.info('[1] Ensuring state tables exist...')
    client.query(DDL_PING).result()
    client.query(DDL_MASTER).result()

    logger.info('[2] Fetching hygiene candidates from Redash #%s ...', HYGIENE_QUERY_ID)
    redash = RedashClient(api_key=REDASH_API_KEY, base_url=REDASH_BASE_URL)
    rows = redash.fetch_query_results(query_id=HYGIENE_QUERY_ID, max_retries=3) or []
    logger.info('      %d candidate(s) from Redash', len(rows))
    if not rows:
        logger.info('REAL_L3 HYGIENE: nothing to ping')
        return

    # [3] dedup against tickets already pinged
    already = {r.ticket_id for r in client.query(
        f"SELECT ticket_id FROM `{PING_TABLE}`").result()}
    new_rows = [r for r in rows if r.get('ticket_id') not in already]
    logger.info('      %d new ticket(s) after dedup (%d already pinged)',
                len(new_rows), len(rows) - len(new_rows))
    if not new_rows:
        logger.info('REAL_L3 HYGIENE: nothing new')
        return

    # [4] load existing master threads: {(ist_date, channel): thread_ts}
    masters = {(r.ist_date, r.channel): r.thread_ts for r in client.query(
        f"SELECT ist_date, channel, thread_ts FROM `{MASTER_TABLE}`").result()}

    pinged, new_masters = [], []
    now_iso = datetime.now(timezone.utc).isoformat()

    for r in new_rows:
        ist_date = r.get('esc_date_ist')
        channel  = ENV_CHANNEL_OVERRIDE or r.get('channel_id') or FALLBACK_CHANNEL
        tag      = r.get('channel_tag') or DEFAULT_TAG
        chan_name = r.get('channel_name') or channel   # log clarity only
        key      = (ist_date, channel)

        # ensure a master message exists for this IST date + channel
        thread_ts = masters.get(key)
        if not thread_ts:
            try:
                thread_ts = slack_post(channel, build_master_text(ist_date, tag))
                masters[key] = thread_ts
                new_masters.append({'ist_date': ist_date, 'channel': channel,
                                    'thread_ts': thread_ts, 'created_at': now_iso})
                logger.info('      posted master for %s in %s [%s] (ts=%s)', ist_date, chan_name, channel, thread_ts)
            except Exception as e:
                logger.error('      failed to post master for %s/%s: %s — skipping its tickets',
                             ist_date, channel, e)
                continue

        # post the hygiene ping as a threaded reply (@-mentions the escalator)
        uid = _slack_uid(r.get('escalator_email'), SLACK_BOT_TOKEN)
        try:
            slack_post(channel, build_reply_text(r, uid), thread_ts=thread_ts)
            pinged.append({'ticket_id': r.get('ticket_id'), 'num': r.get('num'),
                           'escalator_email': r.get('escalator_email'),
                           'missing': ', '.join(_missing_list(r)), 'pinged_at': now_iso})
            logger.info('      pinged #%s (%s) under %s', r.get('num'), r.get('escalator_email'), ist_date)
        except Exception as e:
            logger.error('      failed to post for #%s: %s', r.get('num'), e)

    # [5] persist state
    if new_masters:
        errs = client.insert_rows_json(client.get_table(MASTER_TABLE), new_masters)
        if errs:
            logger.error('      master-table insert errors: %s', errs)
    if pinged:
        errs = client.insert_rows_json(client.get_table(PING_TABLE), pinged)
        if errs:
            logger.error('      ping-table insert errors: %s', errs)

    logger.info('=' * 60)
    logger.info('REAL_L3 HYGIENE: COMPLETE (%d pinged, %d master(s) posted)', len(pinged), len(new_masters))
    logger.info('=' * 60)


# ==================== DAG DEFINITION ====================

default_args = {
    'owner': 'cs_team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'real_l3_hygiene_slack',
    default_args=default_args,
    description='Ping the escalator (in a daily thread) if a real_l3 ticket is missing team / slack link / reply after escalation',
    schedule_interval='*/30 * * * *',  # every 30 min, Asia/Kolkata
    catchup=False,
    is_paused_upon_creation=False,
    tags=['slack', 'trinity', 'real_l3', 'hygiene', 'sla', 'cs_team'],
)

push_real_l3_hygiene_task = PythonOperator(
    task_id='push_real_l3_hygiene',
    python_callable=run_real_l3_hygiene,
    dag=dag,
)
