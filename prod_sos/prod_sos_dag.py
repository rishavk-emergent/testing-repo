"""
Prod SOS Alert - Slack DAG (every 5 min, IST)

Posts ONE Slack message as soon as a ticket qualifies as a Prod SOS on a live
production site, i.e.:
  * it carries the `Prod SOS` tag  (v_tags label 'Prod SOS', tag_id 6a0c44a4c272432bd9f53bf1), AND
  * `custom_fields_live_production = TRUE` on the ticket, AND
  * the ticket is unassigned (`assigned_agent_id IS NULL`).

"Comes to us" = the moment the Prod SOS tag is added. The DAG runs every 5 min and
fires for any qualifying ticket whose Prod SOS tag was added in the last LOOKBACK_MINUTES,
so at go-live we don't flood the channel with the whole historical backlog. Each ticket is
alerted only once (tracked in a state table).

Sources (all Trinity):
  Prod SOS tag added  -> v_ticket_events (action='tag_added', JSON_VALUE(new_value)='Prod SOS')
  Live Production     -> v_tickets.custom_fields_live_production (BOOL)
  Ticket fields       -> v_tickets (num, level, status, subject, weekly_average_visitors)

Schedule: '*/5 * * * *' Asia/Kolkata.
Channel:  PROD_SOS_SLACK_CHANNEL env; defaults to the TEST channel until the live channel is set.
Dedup state: support.prod_sos_pinged (created on first run if absent).
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
# Defaults to the TEST channel for now; set to the real channel (and/or override via env) once decided.
SLACK_CHANNEL_ID = os.getenv('PROD_SOS_SLACK_CHANNEL', 'C0B4J9RBWDC')  # test channel
PROD_SOS_TAG_ID  = '6a0c44a4c272432bd9f53bf1'
STATE_TABLE      = 'emergent-default.support.prod_sos_pinged'
LOOKBACK_MINUTES = 20    # only alert tickets tagged Prod SOS within this many minutes (+overlap vs the 5-min schedule)

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
QUERY = f"""
WITH
sos_evt AS (  -- the "comes to us" moment: first time the Prod SOS tag was added (recent only, for cost)
  SELECT ticket_id, MIN(created_at) AS sos_tagged_at
  FROM `emergent-default.trinity_database.v_ticket_events`
  WHERE action='tag_added' AND JSON_VALUE(new_value)='Prod SOS'
    AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 HOUR)
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
  s.sos_tagged_at,
  FORMAT_TIMESTAMP('%H:%M', s.sos_tagged_at, 'Asia/Kolkata') AS sos_tagged_ist,
  CONCAT('https://trinity-base.internal.emergent.host/tickets/', lt._id) AS ticket_url,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), s.sos_tagged_at, MINUTE) AS mins_since_tagged
FROM lt
JOIN sos_evt s ON s.ticket_id=lt._id
LEFT JOIN `{STATE_TABLE}` p ON p.ticket_id=lt._id
WHERE '{PROD_SOS_TAG_ID}' IN UNNEST(lt.tag_ids)
  AND lt.live_prod IS TRUE
  AND lt.assigned_agent_id IS NULL              -- only unassigned tickets
  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), s.sos_tagged_at, MINUTE) <= {LOOKBACK_MINUTES}
  AND p.ticket_id IS NULL
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

    logger.info('[2] Querying new Prod SOS + Live Production tickets (last %d min, not yet alerted)...', LOOKBACK_MINUTES)
    rows = list(client.query(QUERY).result())
    logger.info('      %d ticket(s) to alert', len(rows))
    if not rows:
        logger.info('PROD SOS ALERT: nothing new')
        return

    pinged = []
    for r in rows:
        row = {
            'ticket_id': r.ticket_id, 'num': r.num, 'level': r.level, 'status': r.status,
            'subject': r.subject, 'weekly_visitors': r.weekly_visitors, 'ticket_url': r.ticket_url,
            'sos_tagged_ist': r.sos_tagged_ist, 'mins_since_tagged': r.mins_since_tagged,
        }
        msg = build_message(row)
        try:
            notifier.send_message(msg, mrkdwn=True, unfurl_links=False, unfurl_media=False)
            pinged.append({
                'ticket_id': row['ticket_id'], 'num': row['num'],
                'level': row['level'], 'status': row['status'],
                'pinged_at': datetime.now(timezone.utc).isoformat(),
            })
            logger.info('      alerted #%s', row['num'])
        except Exception as e:
            logger.error('      failed to post for #%s: %s', row['num'], e)

    if pinged:
        logger.info('[3] Recording %d alerted ticket(s) in state table...', len(pinged))
        table = client.get_table(STATE_TABLE)
        errors = client.insert_rows_json(table, pinged)
        if errors:
            logger.error('      state-table insert errors: %s', errors)

    logger.info('=' * 60)
    logger.info('PROD SOS ALERT: COMPLETE (%d alerted)', len(pinged))
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
    description='Alert in Slack when a ticket gets the Prod SOS tag while Live Production is true',
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
