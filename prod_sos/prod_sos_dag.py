"""
Prod SOS Alert - Slack DAG (every 5 min, IST)

Posts ONE Slack message (with @channel) for every ticket that qualifies as a Prod SOS
on a live production site and needs pickup, i.e. ALL of:
  * it carries the `Prod SOS` tag  (v_tags label 'Prod SOS', tag_id 6a0c44a4c272432bd9f53bf1), AND
  * `custom_fields_live_production = TRUE`, AND
  * the ticket is unassigned (`assigned_agent_id IS NULL`), AND
  * status is OPEN or PENDING (closed tickets are already handled).

There is NO time window on qualification - a ticket is alerted whenever it first meets all
of the above, no matter when the tag was added. Each ticket is alerted exactly once, tracked
in the dedup state table support.prod_sos_pinged.

Go-live backlog: on the very first run (detected via a SEED_MARKER row), all currently
qualifying tickets are written to the state table WITHOUT posting - this suppresses the
existing backlog so we don't flood @channel once. From then on only newly-qualifying tickets
are alerted.

Sources (all Trinity):
  Prod SOS tag        -> v_tickets.tag_ids contains the Prod SOS tag_id
  Live Production     -> v_tickets.custom_fields_live_production (BOOL)
  Assignment / status -> v_tickets.assigned_agent_id / status
  Tag time (display)  -> v_ticket_events (action='tag_added', 'Prod SOS'); LEFT JOIN, last 7d

Schedule: '*/5 * * * *' Asia/Kolkata.
Channel:  community-builders-l2-l1 (C0937QNFJEM); override PROD_SOS_SLACK_CHANNEL env for testing.
"""

from datetime import datetime, timedelta, timezone
import logging, os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN
from utils.slack.slack_client import SlackNotifier
from utils.slack.bigquery_client import get_bigquery_client

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
# Live channel: community-builders-l2-l1. For testing, override PROD_SOS_SLACK_CHANNEL to the test channel.
SLACK_CHANNEL_ID = os.getenv('PROD_SOS_SLACK_CHANNEL', 'C0937QNFJEM')  # community-builders-l2-l1
PROD_SOS_TAG_ID  = '6a0c44a4c272432bd9f53bf1'
STATE_TABLE      = 'emergent-default.support.prod_sos_pinged'
SEED_MARKER      = '__seed_marker__'   # sentinel row that marks the one-time backlog seed as done

# ==================== STATE TABLE (dedup) ====================
DDL = f"""
CREATE TABLE IF NOT EXISTS `{STATE_TABLE}` (
  ticket_id STRING,
  num INT64,
  level STRING,
  status STRING,
  pinged_at TIMESTAMP
)
"""

# ==================== BIGQUERY QUERY ====================
# Qualification is purely on current ticket state (tag + live + unassigned + open/pending) and
# the dedup table. sos_evt is a LEFT JOIN used only to show the tag time, so it never affects
# whether a ticket qualifies; it is bounded to the last 7 days for cost.
QUERY = f"""
WITH
sos_evt AS (  -- tag time for display only (last 7d, for cost)
  SELECT ticket_id, MIN(created_at) AS sos_tagged_at
  FROM `emergent-default.trinity_database.v_ticket_events`
  WHERE action='tag_added' AND JSON_VALUE(new_value)='Prod SOS'
    AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  GROUP BY 1
),
lt AS (
  SELECT _id, num, level, status, subject, tag_ids, assigned_agent_id,
         custom_fields_live_production AS live_prod,
         custom_fields_weekly_average_visitors AS weekly_visitors
  FROM `emergent-default.trinity_database.v_tickets`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1
)
SELECT
  lt._id AS ticket_id,
  CAST(lt.num AS INT64) AS num,
  lt.level, lt.status, lt.subject,
  CAST(lt.weekly_visitors AS INT64) AS weekly_visitors,
  FORMAT_TIMESTAMP('%H:%M', s.sos_tagged_at, 'Asia/Kolkata') AS sos_tagged_ist,
  CONCAT('https://trinity-base.internal.emergent.host/tickets/', lt._id) AS ticket_url,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), s.sos_tagged_at, MINUTE) AS mins_since_tagged
FROM lt
LEFT JOIN sos_evt s ON s.ticket_id=lt._id
LEFT JOIN `{STATE_TABLE}` p ON p.ticket_id=lt._id
WHERE '{PROD_SOS_TAG_ID}' IN UNNEST(lt.tag_ids)   -- Prod SOS tag
  AND lt.live_prod IS TRUE                         -- Live Production
  AND lt.assigned_agent_id IS NULL                 -- unassigned
  AND UPPER(lt.status) IN ('OPEN','PENDING')       -- active only
  AND p.ticket_id IS NULL                          -- not already alerted/seeded
ORDER BY s.sos_tagged_at
"""


# ==================== HELPERS ====================

def build_message(row):
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
    return (
        '<!channel> :rotating_light: *Prod SOS — Live Production*\n'
        '%s · %s · %s%s\n'
        '%s%s'
        % (link, row['level'] or '—', row['status'] or '—', vis_txt, subj, when_txt)
    )


# ==================== MAIN TASK ====================

def run_prod_sos(**context):
    logger.info('=' * 60)
    logger.info('PROD SOS ALERT: QUERY & POST')
    logger.info('=' * 60)

    client   = get_bigquery_client()
    notifier = SlackNotifier(SLACK_BOT_TOKEN, SLACK_CHANNEL_ID)

    logger.info('[1] Ensuring state table exists...')
    client.query(DDL).result()

    # First run? (seed marker absent) -> seed the backlog silently, don't post.
    seeded = list(client.query(
        f"SELECT COUNT(*) AS c FROM `{STATE_TABLE}` WHERE ticket_id='{SEED_MARKER}'"
    ).result())[0].c > 0
    mode = 'ALERT' if seeded else 'SEED (first run - suppressing backlog, no posts)'

    logger.info('[2] Querying qualifying Prod SOS + Live Production + unassigned tickets... mode=%s', mode)
    rows = list(client.query(QUERY).result())
    logger.info('      %d qualifying ticket(s)', len(rows))

    pinged = []
    now_iso = datetime.now(timezone.utc).isoformat()
    for r in rows:
        row = {
            'ticket_id': r.ticket_id, 'num': r.num, 'level': r.level, 'status': r.status,
            'subject': r.subject, 'weekly_visitors': r.weekly_visitors, 'ticket_url': r.ticket_url,
            'sos_tagged_ist': r.sos_tagged_ist, 'mins_since_tagged': r.mins_since_tagged,
        }
        if seeded:
            try:
                notifier.send_message(build_message(row), mrkdwn=True, unfurl_links=False, unfurl_media=False)
                logger.info('      alerted #%s', row['num'])
            except Exception as e:
                logger.error('      failed to post for #%s: %s', row['num'], e)
                continue
        pinged.append({
            'ticket_id': row['ticket_id'], 'num': row['num'],
            'level': row['level'], 'status': row['status'], 'pinged_at': now_iso,
        })

    if not seeded:
        # mark the seed as done so future runs alert instead of seeding
        pinged.append({'ticket_id': SEED_MARKER, 'num': 0, 'level': None, 'status': 'SEED', 'pinged_at': now_iso})
        logger.info('[3] First run: seeded %d existing qualifier(s) into state, no posts', len(rows))

    if pinged:
        table = client.get_table(STATE_TABLE)
        errors = client.insert_rows_json(table, pinged)
        if errors:
            logger.error('      state-table insert errors: %s', errors)

    logger.info('=' * 60)
    logger.info('PROD SOS ALERT: COMPLETE (mode=%s, %d alerted)', mode, (0 if not seeded else len(pinged)))
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
    description='Alert @channel when an unassigned OPEN/PENDING ticket is Prod SOS + Live Production',
    schedule_interval='*/5 * * * *',  # every 5 min, Asia/Kolkata
    catchup=False,
    is_paused_upon_creation=True,  # keep paused until verified + live channel set
    tags=['slack', 'trinity', 'prod_sos', 'alert', 'cs_team'],
)

run_prod_sos_task = PythonOperator(
    task_id='run_prod_sos',
    python_callable=run_prod_sos,
    dag=dag,
)
