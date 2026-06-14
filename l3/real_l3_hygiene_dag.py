"""
RealL3 Escalation Hygiene Nag - Slack DAG (every 30 min, IST)

When an agent escalates a ticket to L3 and tags it `real_l3`, they are expected to,
within 1 hour of that escalation:
  1. set the team,
  2. add the Slack link,
  3. reply to the customer themselves.

This DAG runs every 30 min and, for each real_l3 ticket whose escalation was 1-3 hours
ago and that still has ANY of those 3 missing, posts ONE Slack message @-mentioning the
escalator (the person who added the real_l3 tag) listing the missing item(s). Each ticket
is pinged only once (tracked in a state table).

Who/when escalated  -> the `tag_added` 'real_l3' audit event (actor_agent_id + created_at).
Team done?          -> a team_assigned / team_changed event exists (any actor).
Slack link done?    -> a slack_link_updated event exists (any actor).
Customer reply done?-> a public outbound message authored by the ESCALATOR after escalation.
                       (Intentionally escalator-specific: if someone else took over and
                        replied, the escalator is still pinged.)
Escalator -> Slack   -> v_agents.email -> Slack users.lookupByEmail (@mention).

Window: only tickets escalated 1-3 h ago are considered, so at go-live we don't flood the
channel with the whole existing backlog; combined with the state table this gives each new
escalation a single ping ~1-1.5 h after it happens.

Schedule: '*/30 * * * *' in Asia/Kolkata. Channel: cs-associates (override via env for tests).
Dedup state: support.real_l3_hygiene_pinged (created on first run if absent).
"""

from datetime import datetime, timedelta, timezone
import logging, os, json, urllib.request, urllib.parse

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN
from utils.slack.slack_client import SlackNotifier
from utils.slack.bigquery_client import get_bigquery_client

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
# Target channel is cs-associates; override to the test channel via env for dry runs.
SLACK_CHANNEL_ID = os.getenv('REAL_L3_HYGIENE_SLACK_CHANNEL', 'C0B075CBPS7')  # cs-associates
REAL_L3_TAG_ID   = '6a1f2e835ad901b459b7665f'
STATE_TABLE      = 'emergent-default.support.real_l3_hygiene_pinged'
SLA_MINUTES      = 60    # ping only once the escalation is at least this old
WINDOW_MINUTES   = 180   # ...and at most this old, so we never nag stale backlog

# ==================== STATE TABLE (dedup) ====================
DDL = f"""
CREATE TABLE IF NOT EXISTS `{STATE_TABLE}` (
  ticket_id STRING,
  num INT64,
  escalator_email STRING,
  missing STRING,
  pinged_at TIMESTAMP
)
"""

# ==================== BIGQUERY QUERY ====================
# Candidates: real_l3, OPEN/PENDING, escalated SLA_MINUTES..WINDOW_MINUTES ago, with a gap,
# and not already pinged (LEFT JOIN the state table).
QUERY = f"""
WITH
lt AS (
  SELECT _id, num, level, status, tag_ids
  FROM `emergent-default.trinity_database.v_tickets`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1
),
esc AS (  -- escalation event: who added the real_l3 tag, and when
  SELECT ticket_id, actor_agent_id AS escalator_id, created_at AS esc_ts
  FROM `emergent-default.trinity_database.v_ticket_events`
  WHERE type='audit' AND action='tag_added' AND JSON_VALUE(new_value)='real_l3'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY created_at)=1
),
team_ok AS (SELECT DISTINCT ticket_id FROM `emergent-default.trinity_database.v_ticket_events` WHERE action IN ('team_assigned','team_changed')),
slack_ok AS (SELECT DISTINCT ticket_id FROM `emergent-default.trinity_database.v_ticket_events` WHERE action='slack_link_updated'),
reply AS (  -- the escalator's own public outbound reply after escalation
  SELECT e.ticket_id
  FROM `emergent-default.trinity_database.v_ticket_events` e
  JOIN esc ON esc.ticket_id=e.ticket_id
  WHERE e.type='message' AND e.direction='outbound' AND e.visibility='public'
    AND e.actor_kind='AGENT' AND e.actor_agent_id=esc.escalator_id AND e.created_at>esc.esc_ts
  GROUP BY e.ticket_id
),
ag AS (
  SELECT _id, email, first_name, last_name
  FROM `emergent-default.trinity_database.v_agents`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1
)
SELECT
  lt._id AS ticket_id,
  CAST(lt.num AS INT64) AS num,
  CONCAT('https://trinity-base.internal.emergent.host/tickets/', lt._id) AS ticket_url,
  ag.email AS escalator_email,
  NULLIF(TRIM(CONCAT(IFNULL(ag.first_name,''),' ',IFNULL(ag.last_name,''))),'') AS escalator_name,
  (team_ok.ticket_id IS NOT NULL)  AS team_done,
  (slack_ok.ticket_id IS NOT NULL) AS slack_done,
  (reply.ticket_id IS NOT NULL)    AS reply_done,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), esc.esc_ts, MINUTE) AS mins_since_esc
FROM lt
JOIN esc ON esc.ticket_id=lt._id
LEFT JOIN team_ok  ON team_ok.ticket_id=lt._id
LEFT JOIN slack_ok ON slack_ok.ticket_id=lt._id
LEFT JOIN reply    ON reply.ticket_id=lt._id
LEFT JOIN ag       ON ag._id=esc.escalator_id
LEFT JOIN `{STATE_TABLE}` p ON p.ticket_id=lt._id
WHERE lt.level='L3'
  AND UPPER(lt.status) IN ('OPEN','PENDING')
  AND '{REAL_L3_TAG_ID}' IN UNNEST(lt.tag_ids)
  AND TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), esc.esc_ts, MINUTE) BETWEEN {SLA_MINUTES} AND {WINDOW_MINUTES}
  AND NOT ((team_ok.ticket_id IS NOT NULL) AND (slack_ok.ticket_id IS NOT NULL) AND (reply.ticket_id IS NOT NULL))
  AND p.ticket_id IS NULL
ORDER BY esc.esc_ts
"""


# ==================== HELPERS ====================

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
    if not row['team_done']:  miss.append('team')
    if not row['slack_done']: miss.append('slack link')
    if not row['reply_done']: miss.append('customer response')
    return miss


def build_message(row, uid):
    who  = '<@%s>' % uid if uid else (row.get('escalator_name') or row.get('escalator_email') or 'escalator')
    miss = ', '.join(_missing_list(row))
    link = '<%s|#%s>' % (row['ticket_url'], row['num'])
    return (
        ':rotating_light: *real_l3 escalation hygiene*\n'
        '%s · %s — missing: *%s*\n'
        'Please complete: set the team, add the Slack link, and reply to the customer.'
        % (who, link, miss)
    )


# ==================== MAIN TASK ====================

def run_real_l3_hygiene(**context):
    logger.info('=' * 60)
    logger.info('REAL_L3 HYGIENE: QUERY & PING')
    logger.info('=' * 60)

    client   = get_bigquery_client()
    notifier = SlackNotifier(SLACK_BOT_TOKEN, SLACK_CHANNEL_ID)

    logger.info('[1] Ensuring state table exists...')
    client.query(DDL).result()

    logger.info('[2] Querying real_l3 tickets with hygiene gaps (1-3h, not yet pinged)...')
    rows = list(client.query(QUERY).result())
    logger.info('      %d ticket(s) due for a ping', len(rows))
    if not rows:
        logger.info('REAL_L3 HYGIENE: nothing to ping')
        return

    pinged = []
    for r in rows:
        row = {
            'ticket_id': r.ticket_id, 'num': r.num, 'ticket_url': r.ticket_url,
            'escalator_email': r.escalator_email, 'escalator_name': r.escalator_name,
            'team_done': r.team_done, 'slack_done': r.slack_done, 'reply_done': r.reply_done,
        }
        uid = _slack_uid(row['escalator_email'], SLACK_BOT_TOKEN)
        msg = build_message(row, uid)
        try:
            notifier.send_message(msg, mrkdwn=True, unfurl_links=False, unfurl_media=False)
            pinged.append({
                'ticket_id': row['ticket_id'], 'num': row['num'],
                'escalator_email': row['escalator_email'],
                'missing': ', '.join(_missing_list(row)),
                'pinged_at': datetime.now(timezone.utc).isoformat(),
            })
            logger.info('      pinged #%s (%s)', row['num'], row['escalator_email'])
        except Exception as e:
            logger.error('      failed to post for #%s: %s', row['num'], e)

    if pinged:
        logger.info('[3] Recording %d pinged ticket(s) in state table...', len(pinged))
        table = client.get_table(STATE_TABLE)
        errors = client.insert_rows_json(table, pinged)
        if errors:
            logger.error('      state-table insert errors: %s', errors)

    logger.info('=' * 60)
    logger.info('REAL_L3 HYGIENE: COMPLETE (%d pinged)', len(pinged))
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
    description='Ping the escalator if a real_l3 ticket is missing team / slack link / reply 1h after escalation',
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
