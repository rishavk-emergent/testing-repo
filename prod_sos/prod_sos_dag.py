"""
Prod SOS Alert - Slack DAG (every 5 min, IST)

Posts a Slack alert for every NEW ticket that qualifies as a Prod SOS on a live production site:
  * carries the `Prod SOS` tag, AND
  * `custom_fields_live_production = TRUE`, AND
  * unassigned (`assigned_agent_id IS NULL`), AND
  * status OPEN or PENDING, AND
  * the Prod SOS tag was added within the last LOOKBACK_MINUTES (NEW only; backlog excluded).

ARCHITECTURE (per IST date):
  * The FIRST qualifying ticket of the day posts a MASTER message to the channel WITH @channel:
        <!channel> :rotating_light: *Prod SOS — Live Production* (29 Jun 2026)
  * EVERY qualifying ticket (including that first one) is then posted as a THREADED REPLY under
    that day's master message (no @channel on the replies, so the channel is pinged once/day):
        #27709 · L3 · OPEN · weekly visitors: 30
        Production MongoDB data deletion & credit consumption issue
        tagged 15:52 IST · 4 min ago

WHERE THE LOGIC LIVES:
  * QUALIFYING FILTER LOGIC  -> Redash query #PROD_SOS_QUERY_ID  (edit there, no DAG change needed)
  * dedup + master/thread plumbing -> this DAG (support.prod_sos_pinged + support.prod_sos_master)

Each ticket is alerted exactly once (dedup state table support.prod_sos_pinged). The DAG runs every
5 min, so a ticket is alerted within ~5 min of being tagged. Because only recently-tagged tickets
fire, the DAG is safe to run active immediately on merge (no backlog flood).

Schedule: '*/5 * * * *' Asia/Kolkata.
Channel:  community-builders-l2-l1 (C0937QNFJEM); override PROD_SOS_SLACK_CHANNEL env for testing.
"""

from datetime import datetime, timezone, timedelta
import logging, os

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
# Channel + @-mention come from the Redash query (channel_id / channel_name / channel_tag), so
# rerouting or changing the POC is a query edit, not a code change.
#   * ENV_CHANNEL_OVERRIDE: set PROD_SOS_SLACK_CHANNEL to force ALL alerts to one channel (testing).
#   * FALLBACK_CHANNEL / DEFAULT_TAG: used only if the query row leaves them blank.
ENV_CHANNEL_OVERRIDE = os.getenv('PROD_SOS_SLACK_CHANNEL')  # testing override; prod leaves this unset
FALLBACK_CHANNEL     = 'C0937QNFJEM'   # community-builders-l2-l1
DEFAULT_TAG          = '<!channel>'
PROD_SOS_QUERY_ID  = 38606          # Redash: "[Prod SOS] Live Production alert feed" (edit filters there)
PING_TABLE         = 'emergent-default.support.prod_sos_pinged'   # per-ticket dedup
MASTER_TABLE       = 'emergent-default.support.prod_sos_master'   # one master message per (IST date, channel)

# ==================== STATE TABLES ====================
DDL_PING = f"""
CREATE TABLE IF NOT EXISTS `{PING_TABLE}` (
  ticket_id STRING,
  num INT64,
  level STRING,
  status STRING,
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
    """Post a message via chat.postMessage; returns the message ts. Raises on failure."""
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


def build_master_text(ist_date, tag):
    """ist_date is 'YYYY-MM-DD' (IST); tag is the @-mention (from the query). -> master text."""
    try:
        label = datetime.strptime(ist_date, '%Y-%m-%d').strftime('%-d %b %Y')
    except Exception:
        label = ist_date
    return '%s :rotating_light: *Prod SOS — Live Production* (%s)' % (tag or DEFAULT_TAG, label)


def build_alert_text(row):
    """Threaded reply for a single ticket (no @channel)."""
    link = '<%s|#%s>' % (row['ticket_url'], row['num'])
    subj = (row['subject'] or '').strip()
    if len(subj) > 90:
        subj = subj[:90] + '...'
    visitors = row.get('weekly_visitors')
    vis_txt = (' · weekly visitors: *%s*' % f'{visitors:,}') if visitors else ''
    mins = row.get('mins_since_tagged')
    when = row.get('sos_tagged_ist')
    when_txt = ''
    if when is not None:
        ago = ('%d min ago' % mins) if mins is not None else ''
        when_txt = '\ntagged %s IST%s' % (when, (' · ' + ago) if ago else '')
    return ('%s · %s · %s%s\n%s%s'
            % (link, row['level'] or '—', row['status'] or '—', vis_txt, subj, when_txt))


# ==================== MAIN TASK ====================

def run_prod_sos(**context):
    logger.info('=' * 60)
    logger.info('PROD SOS ALERT: FETCH (Redash #%s) & POST', PROD_SOS_QUERY_ID)
    logger.info('=' * 60)

    client = get_bigquery_client()

    logger.info('[1] Ensuring state tables exist...')
    client.query(DDL_PING).result()
    client.query(DDL_MASTER).result()

    logger.info('[2] Fetching qualifying tickets from Redash #%s ...', PROD_SOS_QUERY_ID)
    redash = RedashClient(api_key=REDASH_API_KEY, base_url=REDASH_BASE_URL)
    rows = redash.fetch_query_results(query_id=PROD_SOS_QUERY_ID, max_retries=3) or []
    logger.info('      %d qualifying ticket(s) from Redash', len(rows))
    if not rows:
        logger.info('PROD SOS ALERT: nothing qualifying')
        return

    # [3] dedup against tickets already alerted
    already = {r.ticket_id for r in client.query(
        f"SELECT ticket_id FROM `{PING_TABLE}`").result()}
    new_rows = [r for r in rows if r.get('ticket_id') not in already]
    logger.info('      %d new ticket(s) after dedup (%d already alerted)',
                len(new_rows), len(rows) - len(new_rows))
    if not new_rows:
        logger.info('PROD SOS ALERT: nothing new')
        return

    # [4] load existing master threads: {(ist_date, channel): thread_ts}
    masters = {(r.ist_date, r.channel): r.thread_ts for r in client.query(
        f"SELECT ist_date, channel, thread_ts FROM `{MASTER_TABLE}`"
    ).result()}

    pinged, new_masters = [], []
    now_iso = datetime.now(timezone.utc).isoformat()

    for r in new_rows:
        ist_date = r.get('sos_tagged_date_ist')
        # channel + tag come from the query row; env override forces a single channel (testing)
        channel = ENV_CHANNEL_OVERRIDE or r.get('channel_id') or FALLBACK_CHANNEL
        tag     = r.get('channel_tag') or DEFAULT_TAG
        chan_name = r.get('channel_name') or channel   # for log clarity only
        key     = (ist_date, channel)

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

        # post the ticket as a threaded reply (no @-mention on replies)
        try:
            slack_post(channel, build_alert_text(r), thread_ts=thread_ts)
            pinged.append({'ticket_id': r.get('ticket_id'), 'num': r.get('num'),
                           'level': r.get('level'), 'status': r.get('status'), 'pinged_at': now_iso})
            logger.info('      alerted #%s under %s in %s', r.get('num'), ist_date, channel)
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
    logger.info('PROD SOS ALERT: COMPLETE (%d alerted, %d master(s) posted)', len(pinged), len(new_masters))
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
    'prod_sos_slack',
    default_args=default_args,
    description='Alert when a NEW unassigned OPEN/PENDING ticket is Prod SOS + Live Production (master+thread, Redash-backed)',
    schedule_interval='*/5 * * * *',  # every 5 min, Asia/Kolkata
    catchup=False,
    is_paused_upon_creation=False,  # active on merge; only recently-tagged tickets fire, so no backlog flood
    tags=['slack', 'trinity', 'prod_sos', 'alert', 'cs_team'],
)

run_prod_sos_task = PythonOperator(
    task_id='run_prod_sos',
    python_callable=run_prod_sos,
    dag=dag,
)
