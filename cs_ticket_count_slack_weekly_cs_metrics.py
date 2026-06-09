"""
CS Ticket Count Slack DAG — WEEKLY (Mondays 10 AM, IST)
Standalone DAG: every Monday at 10:00 AM IST it queries BigQuery for the last
5 completed Mon-Sun weeks of ticket creation/closure stats, split by support
level (L1/L2) and — for closures — by Overwatch vs Human, then posts a
formatted table (one row per week) to the #daily-cs-metrics Slack channel.

Schedule: '0 10 * * 1' interpreted in Asia/Kolkata -> every Monday 10:00 IST
Data Source: analytics.support_tickets_tat (kept fresh by the atlas sync DAGs),
             enriched with support.trinity_ticket_tat for the OW-vs-Human split.
Triggers: NONE. Fully self-scheduled; not wired to any other DAG.
Output: Slack message with a 5-week breakdown table (L1/L2 x OW/Human).

Note: Trinity OW/Human classification began ~18 May 2026, so weeks before that
show near-empty OW/Hu (counted in Closed, surfaced as 'unclassified'). This
self-corrects as older weeks roll out of the 5-week window.
"""

from datetime import datetime, timedelta, timezone
import logging
import os

import pendulum
import requests
from google.cloud import bigquery
from airflow import DAG
from airflow.operators.python import PythonOperator

# Set up logging
logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN
SLACK_CHANNEL_ID = os.getenv('CS_METRICS_SLACK_CHANNEL', 'C0B6ACKP9CH')  # #daily-cs-metrics
BIGQUERY_PROJECT = os.getenv('BIGQUERY_PROJECT', 'emergent-default')
TABLE_ID = os.getenv('TAT_TABLE_ID', 'emergent-default.analytics.support_tickets_tat')
TRINITY_TAT_TABLE = os.getenv('TRINITY_TAT_TABLE', 'emergent-default.support.trinity_ticket_tat')
VTICKETS_TABLE = os.getenv('VTICKETS_TABLE', 'emergent-default.trinity_database.v_tickets')
CPST_TABLE = os.getenv('CPST_TABLE', 'emergent-default.analytics.closed_pending_support_tickets')
API_TIMEOUT_SECONDS = 30

# ==================== BIGQUERY QUERY ====================

# The 5 completed Mon-Sun weeks before the run-day Monday. One row per week,
# running cumulative across weeks (oldest -> newest).
WEEKLY_STATS_QUERY = f"""
WITH
bounds AS (
  SELECT DATE_TRUNC(DATE(DATETIME(CURRENT_TIMESTAMP(), 'Asia/Kolkata')), WEEK(MONDAY)) AS cur_mon
),
weeks AS (
  SELECT wk
  FROM bounds, UNNEST(GENERATE_DATE_ARRAY(
    DATE_SUB(cur_mon, INTERVAL 5 WEEK), DATE_SUB(cur_mon, INTERVAL 1 WEEK), INTERVAL 1 WEEK)) AS wk
),
-- OW vs Human classification per Atlas ticket_number, sourced from Trinity.
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

wk_created AS (
  SELECT
    DATE_TRUNC(DATE(DATETIME(created_at, 'Asia/Kolkata')), WEEK(MONDAY)) AS wk,
    COUNTIF(support_level = 'L1') AS cr_l1,
    COUNTIF(support_level = 'L2') AS cr_l2
  FROM base, bounds
  WHERE DATE(DATETIME(created_at, 'Asia/Kolkata')) >= DATE_SUB(cur_mon, INTERVAL 5 WEEK)
    AND DATE(DATETIME(created_at, 'Asia/Kolkata')) <  cur_mon
  GROUP BY 1
),

wk_closed AS (
  SELECT
    DATE_TRUNC(DATE(DATETIME(b.closed_at, 'Asia/Kolkata')), WEEK(MONDAY)) AS wk,
    COUNTIF(b.support_level = 'L1' AND oc.escalated_to = 0) AS ow_l1,
    COUNTIF(b.support_level = 'L2' AND oc.escalated_to = 0) AS ow_l2,
    COUNTIF(b.support_level = 'L1' AND oc.escalated_to = 1) AS hu_l1,
    COUNTIF(b.support_level = 'L2' AND oc.escalated_to = 1) AS hu_l2,
    COUNTIF(oc.escalated_to IS NULL) AS unclassified,
    COUNT(*) AS overall_closed
  FROM base b, bounds
  LEFT JOIN ow_class oc USING (ticket_number)
  WHERE b.closed_at IS NOT NULL
    AND DATE(DATETIME(b.closed_at, 'Asia/Kolkata')) >= DATE_SUB(cur_mon, INTERVAL 5 WEEK)
    AND DATE(DATETIME(b.closed_at, 'Asia/Kolkata')) <  cur_mon
  GROUP BY 1
)

SELECT
  FORMAT_DATE('%Y-%m-%d', w.wk) AS week_start,
  COALESCE(c.cr_l1, 0) AS cr_l1,
  COALESCE(c.cr_l2, 0) AS cr_l2,
  SUM(COALESCE(c.cr_l1, 0) + COALESCE(c.cr_l2, 0)) OVER ww AS cr_cum,
  COALESCE(x.ow_l1, 0) AS ow_l1,
  COALESCE(x.ow_l2, 0) AS ow_l2,
  SUM(COALESCE(x.ow_l1, 0) + COALESCE(x.ow_l2, 0)) OVER ww AS ow_cum,
  COALESCE(x.hu_l1, 0) AS hu_l1,
  COALESCE(x.hu_l2, 0) AS hu_l2,
  SUM(COALESCE(x.hu_l1, 0) + COALESCE(x.hu_l2, 0)) OVER ww AS hu_cum,
  SUM(COALESCE(x.overall_closed, 0)) OVER ww AS closed_cum,
  COALESCE(x.unclassified, 0) AS unclassified
FROM weeks w
LEFT JOIN wk_created c ON w.wk = c.wk
LEFT JOIN wk_closed  x ON w.wk = x.wk
WINDOW ww AS (ORDER BY w.wk)
ORDER BY w.wk
"""


# ==================== SLACK FUNCTIONS ====================

def format_week_label(week_start: str) -> str:
    """'2026-05-04' (Mon) -> '04-10 May' (Mon-Sun range), cross-month aware."""
    mon = datetime.strptime(week_start, '%Y-%m-%d')
    sun = mon + timedelta(days=6)
    if mon.month == sun.month:
        return f"{mon.day:02d}-{sun.day:02d} {mon.strftime('%b')}"
    return f"{mon.day:02d} {mon.strftime('%b')}-{sun.day:02d} {sun.strftime('%b')}"


def build_weekly_slack_message(rows: list) -> str:
    """
    Weekly view: one row per week (Mon-Sun) for the last 5 completed weeks.
    Columns: Created (L1/L2/Cum) | Closed-OW (L1/L2/Cum) | Closed-Hu (L1/L2/Cum) | Overall Closed (Cum).
    All *-Cum columns and Overall Closed are running cumulative across weeks.
    """
    if not rows:
        return "📊 *CS Ticket Stats — Weekly (last 5 weeks, IST)*\n\nNo data available."

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

    # Window range from the spine, e.g. "04 May – 07 Jun 2026"
    first_mon = datetime.strptime(rows[0]['week_start'], '%Y-%m-%d')
    last_sun = datetime.strptime(last['week_start'], '%Y-%m-%d') + timedelta(days=6)
    range_str = f"{first_mon.strftime('%d %b')} – {last_sun.strftime('%d %b %Y')}"

    message = f"📊 *CS Ticket Stats — Weekly (last 5 weeks: {range_str}, IST)*\n\n"
    message += (
        f"🎫 Created: *{total_created}* (L1 {tot_cr_l1} / L2 {tot_cr_l2})  |  "
        f"✅ Closed: *{total_closed}* (OW {total_ow} / Hu {total_hu})\n\n"
    )

    message += "```\n"
    message += (
        f"{'Week':<13}{'C-L1':>8}{'C-L2':>8}{'C-Cum':>8}"
        f"{'OW-L1':>8}{'OW-L2':>8}{'OW-Cum':>8}"
        f"{'Hu-L1':>8}{'Hu-L2':>8}{'Hu-Cum':>8}"
        f"{'Closed':>8}\n"
    )
    width = 13 + 8 * 10
    message += "─" * width + "\n"

    for row in rows:
        wl = format_week_label(row['week_start'])
        message += (
            f"{wl:<13}{row['cr_l1']:>8}{row['cr_l2']:>8}{row['cr_cum']:>8}"
            f"{row['ow_l1']:>8}{row['ow_l2']:>8}{row['ow_cum']:>8}"
            f"{row['hu_l1']:>8}{row['hu_l2']:>8}{row['hu_cum']:>8}"
            f"{row['closed_cum']:>8}\n"
        )

    message += "─" * width + "\n"
    message += (
        f"{'Total':<13}{tot_cr_l1:>8}{tot_cr_l2:>8}{last['cr_cum']:>8}"
        f"{tot_ow_l1:>8}{tot_ow_l2:>8}{last['ow_cum']:>8}"
        f"{tot_hu_l1:>8}{tot_hu_l2:>8}{last['hu_cum']:>8}"
        f"{total_closed:>8}\n"
    )
    message += "```\n"

    if tot_unclassified > 0:
        message += (
            f"\n_⚠️ {tot_unclassified} closed ticket(s) lack a Trinity OW/Hu tag "
            f"(counted in Closed, excluded from OW/Hu). Mostly pre-~18 May weeks — "
            f"coverage fills in for recent weeks._"
        )

    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    message += f"\n_Generated: {now_ist.strftime('%d %b %Y %I:%M %p IST')}_"

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

def run_weekly_stats_to_slack(**context):
    """
    Main function:
    1. Query BigQuery for the last 5 completed weeks
    2. Format as a table
    3. Post to Slack
    """
    logger.info("=" * 60)
    logger.info("CS TICKET STATS (WEEKLY): QUERY & PUSH TO SLACK")
    logger.info("=" * 60)

    logger.info("[1] Querying BigQuery for weekly ticket stats...")
    client = bigquery.Client(project=BIGQUERY_PROJECT)

    try:
        query_job = client.query(WEEKLY_STATS_QUERY)
        results = query_job.result()
    except Exception as e:
        logger.error(f"      BigQuery query failed: {e}")
        send_slack_message(f"🚨 *CS Ticket Stats (Weekly) Error*\n\nBigQuery query failed: {str(e)[:300]}")
        raise

    rows = []
    for row in results:
        rows.append({
            'week_start': row.week_start,
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

    logger.info(f"      ✓ Got {len(rows)} weekly rows")

    logger.info("[2] Building Slack message...")
    message = build_weekly_slack_message(rows)

    logger.info("[3] Sending to Slack...")
    success = send_slack_message(message)

    if not success:
        raise Exception("Failed to send Slack message")

    logger.info("=" * 60)
    logger.info("CS TICKET STATS (WEEKLY): COMPLETE")
    logger.info("=" * 60)


# ==================== DAG DEFINITION ====================

default_args = {
    'owner': 'cs_team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'cs_ticket_count_slack_weekly_cs_metrics',
    default_args=default_args,
    description='Post weekly (last 5 weeks) CS ticket stats to #daily-cs-metrics, Mondays 10am IST',
    schedule_interval='0 10 * * 1',  # Every Monday 10:00 AM IST
    catchup=False,
    tags=['slack', 'analytics', 'support_tickets', 'reporting', 'cs_metrics', 'weekly'],
)

push_weekly_task = PythonOperator(
    task_id='push_cs_weekly_ticket_stats_to_slack',
    python_callable=run_weekly_stats_to_slack,
    dag=dag,
)
