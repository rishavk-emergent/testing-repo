"""
Ticket Stats Hourly Slack DAG
Queries BigQuery for hourly ticket creation/closure stats (IST)
and posts a formatted table to Slack.

Trigger: Triggered by atlas_support_tickets_tat DAG after each run
Data Source: analytics.support_tickets_tat (BigQuery), enriched with
             support.trinity_ticket_tat for OW-vs-Human closure split.
Output: Slack message with hourly breakdown table (L1/L2 x OW/Human)
"""

from datetime import datetime, timedelta, timezone
import logging
import os

import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator

# Set up logging
logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN
SLACK_CHANNEL_ID = os.getenv('TICKET_STATS_SLACK_CHANNEL', 'C0AHKKJME23')
BIGQUERY_PROJECT = os.getenv('BIGQUERY_PROJECT', 'emergent-default')
TABLE_ID = os.getenv('TAT_TABLE_ID', 'emergent-default.analytics.support_tickets_tat')
TRINITY_TAT_TABLE = os.getenv('TRINITY_TAT_TABLE', 'emergent-default.support.trinity_ticket_tat')
VTICKETS_TABLE = os.getenv('VTICKETS_TABLE', 'emergent-default.trinity_database.v_tickets')
CPST_TABLE = os.getenv('CPST_TABLE', 'emergent-default.analytics.closed_pending_support_tickets')
API_TIMEOUT_SECONDS = 30

# ==================== BIGQUERY QUERY ====================

HOURLY_STATS_QUERY = f"""
WITH
-- OW vs Human classification per Atlas ticket_number, sourced from Trinity.
-- Join path: trinity_ticket_tat.ticket_id -> v_tickets._id ; v_tickets.atlas_id -> cpst.id ; cpst.number -> ticket_number
trinity AS (
  SELECT ticket_id, escalated_to
  FROM `{TRINITY_TAT_TABLE}`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY sync_timestamp DESC) = 1
),
vt AS (
  SELECT _id, atlas_id
  FROM `{VTICKETS_TABLE}`
  WHERE atlas_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC) = 1
),
cpst AS (
  SELECT id, number
  FROM `{CPST_TABLE}`
  WHERE number IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY number) = 1
),
ow_class AS (
  SELECT cpst.number AS ticket_number, t.escalated_to  -- 0 = OW-handled, 1 = escalated to Human
  FROM trinity t
  JOIN vt   ON t.ticket_id = vt._id
  JOIN cpst ON vt.atlas_id = cpst.id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY cpst.number ORDER BY t.escalated_to DESC) = 1
),

-- L1/L2 tickets only (N/A and L3 excluded by design)
base AS (
  SELECT ticket_number, support_level, created_at, closed_at
  FROM `{TABLE_ID}`
  WHERE support_level IN ('L1', 'L2')
),

hourly_created AS (
  SELECT
    EXTRACT(HOUR FROM DATETIME(created_at, 'Asia/Kolkata')) AS hour_ist,
    COUNTIF(support_level = 'L1') AS cr_l1,
    COUNTIF(support_level = 'L2') AS cr_l2
  FROM base
  WHERE DATE(DATETIME(created_at, 'Asia/Kolkata')) = DATE(DATETIME(CURRENT_TIMESTAMP(), 'Asia/Kolkata'))
  GROUP BY 1
),

hourly_closed AS (
  SELECT
    EXTRACT(HOUR FROM DATETIME(b.closed_at, 'Asia/Kolkata')) AS hour_ist,
    COUNTIF(b.support_level = 'L1' AND oc.escalated_to = 0) AS ow_l1,
    COUNTIF(b.support_level = 'L2' AND oc.escalated_to = 0) AS ow_l2,
    COUNTIF(b.support_level = 'L1' AND oc.escalated_to = 1) AS hu_l1,
    COUNTIF(b.support_level = 'L2' AND oc.escalated_to = 1) AS hu_l2,
    COUNTIF(oc.escalated_to IS NULL) AS unclassified,
    COUNT(*) AS overall_closed
  FROM base b
  LEFT JOIN ow_class oc USING (ticket_number)
  WHERE b.closed_at IS NOT NULL
    AND DATE(DATETIME(b.closed_at, 'Asia/Kolkata')) = DATE(DATETIME(CURRENT_TIMESTAMP(), 'Asia/Kolkata'))
  GROUP BY 1
)

SELECT
  COALESCE(c.hour_ist, x.hour_ist) AS hour_ist,
  COALESCE(c.cr_l1, 0) AS cr_l1,
  COALESCE(c.cr_l2, 0) AS cr_l2,
  SUM(COALESCE(c.cr_l1, 0) + COALESCE(c.cr_l2, 0)) OVER w AS cr_cum,
  COALESCE(x.ow_l1, 0) AS ow_l1,
  COALESCE(x.ow_l2, 0) AS ow_l2,
  SUM(COALESCE(x.ow_l1, 0) + COALESCE(x.ow_l2, 0)) OVER w AS ow_cum,
  COALESCE(x.hu_l1, 0) AS hu_l1,
  COALESCE(x.hu_l2, 0) AS hu_l2,
  SUM(COALESCE(x.hu_l1, 0) + COALESCE(x.hu_l2, 0)) OVER w AS hu_cum,
  SUM(COALESCE(x.overall_closed, 0)) OVER w AS closed_cum,
  COALESCE(x.unclassified, 0) AS unclassified
FROM hourly_created c
FULL OUTER JOIN hourly_closed x
  ON c.hour_ist = x.hour_ist
WINDOW w AS (ORDER BY COALESCE(c.hour_ist, x.hour_ist))
ORDER BY hour_ist
"""


# ==================== SLACK FUNCTIONS ====================

def format_hour_label(hour: int) -> str:
    """Convert 24h hour integer to readable format like '9 AM', '2 PM'."""
    if hour == 0:
        return "12 AM"
    elif hour < 12:
        return f"{hour} AM"
    elif hour == 12:
        return "12 PM"
    else:
        return f"{hour - 12} PM"


def build_slack_message(rows: list, date_str: str) -> str:
    """
    Build a formatted Slack message with hourly breakdown table.
    Columns: Created (L1/L2/Cum) | Closed-OW (L1/L2/Cum) | Closed-Hu (L1/L2/Cum) | Overall Closed (Cum).
    All *-Cum columns and Overall Closed are running cumulative across hours.
    Uses Slack's monospace formatting for aligned columns.
    """
    if not rows:
        return f"📊 *Hourly Ticket Stats — {date_str} (IST)*\n\nNo ticket data available for today yet."

    # Per-hour columns are summed; cumulative columns take the last row's value.
    tot_cr_l1 = sum(r['cr_l1'] for r in rows)
    tot_cr_l2 = sum(r['cr_l2'] for r in rows)
    tot_ow_l1 = sum(r['ow_l1'] for r in rows)
    tot_ow_l2 = sum(r['ow_l2'] for r in rows)
    tot_hu_l1 = sum(r['hu_l1'] for r in rows)
    tot_hu_l2 = sum(r['hu_l2'] for r in rows)
    tot_unclassified = sum(r['unclassified'] for r in rows)

    last = rows[-1]
    total_created = tot_cr_l1 + tot_cr_l2
    total_ow = tot_ow_l1 + tot_ow_l2
    total_hu = tot_hu_l1 + tot_hu_l2
    total_closed = last['closed_cum']

    # Header
    message = f"📊 *Hourly Ticket Stats — {date_str} (IST)*\n\n"

    # Summary line
    message += (
        f"🎫 Created: *{total_created}* (L1 {tot_cr_l1} / L2 {tot_cr_l2})  |  "
        f"✅ Closed: *{total_closed}* (OW {total_ow} / Hu {total_hu})\n\n"
    )

    # Table header
    message += "```\n"
    message += (
        f"{'Hour':<7}{'C-L1':>6}{'C-L2':>6}{'C-Cum':>7}"
        f"{'OW-L1':>7}{'OW-L2':>7}{'OW-Cum':>8}"
        f"{'Hu-L1':>7}{'Hu-L2':>7}{'Hu-Cum':>8}"
        f"{'Closed':>8}\n"
    )
    width = 7 + 6 + 6 + 7 + 7 + 7 + 8 + 7 + 7 + 8 + 8
    message += "─" * width + "\n"

    # Table rows
    for row in rows:
        hour_label = format_hour_label(row['hour_ist'])
        message += (
            f"{hour_label:<7}{row['cr_l1']:>6}{row['cr_l2']:>6}{row['cr_cum']:>7}"
            f"{row['ow_l1']:>7}{row['ow_l2']:>7}{row['ow_cum']:>8}"
            f"{row['hu_l1']:>7}{row['hu_l2']:>7}{row['hu_cum']:>8}"
            f"{row['closed_cum']:>8}\n"
        )

    # Table footer with totals
    message += "─" * width + "\n"
    message += (
        f"{'Total':<7}{tot_cr_l1:>6}{tot_cr_l2:>6}{last['cr_cum']:>7}"
        f"{tot_ow_l1:>7}{tot_ow_l2:>7}{last['ow_cum']:>8}"
        f"{tot_hu_l1:>7}{tot_hu_l2:>7}{last['hu_cum']:>8}"
        f"{total_closed:>8}\n"
    )
    message += "```\n"

    # Footnote for tickets closed but not yet classified in Trinity
    if tot_unclassified > 0:
        message += (
            f"\n_⚠️ {tot_unclassified} closed ticket(s) not yet in Trinity — "
            f"counted in Closed, excluded from OW/Hu._"
        )

    # Timestamp
    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    message += f"\n_Last synced: {now_ist.strftime('%I:%M %p IST')}_"

    return message


def send_slack_message(message: str) -> bool:
    """Post message to Slack channel."""
    try:
        headers = {
            'Authorization': f'Bearer {SLACK_BOT_TOKEN}',
            'Content-Type': 'application/json'
        }
        payload = {
            "channel": SLACK_CHANNEL_ID,
            "text": message,
        }
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers=headers,
            json=payload,
            timeout=API_TIMEOUT_SECONDS
        )
        result = response.json()
        if result.get('ok'):
            logger.info("      ✓ Slack message sent successfully")
            return True
        else:
            logger.error(f"      Slack API error: {result.get('error')}")
            return False
    except Exception as e:
        logger.error(f"      Slack notification failed: {e}")
        return False


# ==================== MAIN TASK ====================

def run_ticket_stats_to_slack(**context):
    """
    Main function:
    1. Query BigQuery for today's hourly ticket stats
    2. Format as a table
    3. Post to Slack
    """
    logger.info("=" * 60)
    logger.info("TICKET STATS: QUERY & PUSH TO SLACK")
    logger.info("=" * 60)

    # 1. Query BigQuery
    logger.info("[1] Querying BigQuery for hourly ticket stats...")
    client = bigquery.Client(project=BIGQUERY_PROJECT)

    try:
        query_job = client.query(HOURLY_STATS_QUERY)
        results = query_job.result()
    except Exception as e:
        logger.error(f"      BigQuery query failed: {e}")
        send_slack_message(f"🚨 *Ticket Stats Error*\n\nBigQuery query failed: {str(e)[:300]}")
        raise

    rows = []
    for row in results:
        rows.append({
            'hour_ist': row.hour_ist,
            'cr_l1': row.cr_l1,
            'cr_l2': row.cr_l2,
            'cr_cum': row.cr_cum,
            'ow_l1': row.ow_l1,
            'ow_l2': row.ow_l2,
            'ow_cum': row.ow_cum,
            'hu_l1': row.hu_l1,
            'hu_l2': row.hu_l2,
            'hu_cum': row.hu_cum,
            'closed_cum': row.closed_cum,
            'unclassified': row.unclassified,
        })

    logger.info(f"      ✓ Got {len(rows)} hourly rows")

    # 2. Get today's date in IST for the header
    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    date_str = now_ist.strftime('%A, %d %b %Y')

    # 3. Build and send Slack message
    logger.info("[2] Building Slack message...")
    message = build_slack_message(rows, date_str)

    logger.info("[3] Sending to Slack...")
    success = send_slack_message(message)

    if not success:
        raise Exception("Failed to send Slack message")

    logger.info("=" * 60)
    logger.info("TICKET STATS: COMPLETE")
    logger.info("=" * 60)


# ==================== DAG DEFINITION ====================

default_args = {
    'owner': 'analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'ticket_stats_hourly_slack',
    default_args=default_args,
    description='Post hourly ticket creation/closure stats to Slack (triggered by TAT DAG)',
    schedule_interval=None,  # Triggered externally by TAT DAG
    catchup=False,
    tags=['slack', 'analytics', 'support_tickets', 'reporting'],
)

push_stats_task = PythonOperator(
    task_id='push_ticket_stats_to_slack',
    python_callable=run_ticket_stats_to_slack,
    dag=dag,
)
