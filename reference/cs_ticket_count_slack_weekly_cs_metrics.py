"""
CS Ticket Count Slack DAG — WEEKLY (Mondays 10 AM, IST)
Standalone DAG: every Monday at 10:00 AM IST it queries BigQuery for the last
5 completed Mon-Sun weeks of ticket creation/closure stats, split by support
level (L1/L2) and — for closures — by Overwatch vs Human, then posts a
formatted table (one row per week, most recent on top) to #support-daily-metrics.

Closures are EVENT-BASED (same method as the daily DAG): counted from Trinity
ticket-event close transitions — status_changed -> CLOSED by actor_kind
SYSTEM (Overwatch) / AGENT (Human). This replaces the old snapshot
(trinity_ticket_tat.escalated_to) join, which undercounted OW/Hu every week.

Trinity rollout was ~18 May 2026, and Trinity has no ticket-event history before
then — so weeks earlier than that have no usable OW/Hu split. The window is
therefore CAPPED to weeks >= the rollout: today it shows the covered subset of
the last 5 weeks, and automatically fills back to a full 5 once the pre-rollout
weeks age out of the trailing window (~late June).

Schedule: '0 10 * * 1' interpreted in Asia/Kolkata -> every Monday 10:00 IST
Data Source: closes from trinity_database.v_ticket_events; created from
             analytics.support_tickets_tat; tier (L1/L2) from CPST.support_level.
Triggers: NONE. Fully self-scheduled; not wired to any other DAG.
"""

from datetime import datetime, timedelta, timezone
import logging
import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# Set up logging
logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN
from utils.slack.slack_client import SlackNotifier
from utils.slack.bigquery_client import get_bigquery_client
SLACK_CHANNEL_ID = os.getenv('CS_METRICS_SLACK_CHANNEL', 'C0A8KA1D4U9')  # #support-daily-metrics
TABLE_ID = os.getenv('TAT_TABLE_ID', 'emergent-default.analytics.support_tickets_tat')
TICKET_EVENTS_TABLE = os.getenv('TICKET_EVENTS_TABLE', 'emergent-default.trinity_database.v_ticket_events')
VTICKETS_TABLE = os.getenv('VTICKETS_TABLE', 'emergent-default.trinity_database.v_tickets')
CPST_TABLE = os.getenv('CPST_TABLE', 'emergent-default.analytics.closed_pending_support_tickets')
# Trinity rollout — weeks before this lack ticket-event history, so OW/Hu can't be split.
TRINITY_START = os.getenv('TRINITY_START', '2026-05-18')

# ==================== BIGQUERY QUERY ====================

# Last 5 completed Mon-Sun weeks, capped to weeks >= TRINITY_START. One row per week,
# OW/Hu from event-based close transitions, running cumulative across weeks (oldest -> newest).
WEEKLY_STATS_QUERY = f"""
WITH
bounds AS (
  SELECT DATE_TRUNC(DATE(DATETIME(CURRENT_TIMESTAMP(), 'Asia/Kolkata')), WEEK(MONDAY)) AS cur_mon
),
win AS (
  SELECT
    GREATEST(DATE_SUB((SELECT cur_mon FROM bounds), INTERVAL 5 WEEK), DATE '{TRINITY_START}') AS ws,
    DATE_SUB((SELECT cur_mon FROM bounds), INTERVAL 1 DAY) AS we
),

-- tier lookup (atlas ticket id -> L1/L2)
cpst AS (
  SELECT id AS atlas_id, COALESCE(NULLIF(support_level, 'N/A'), 'untagged') AS tier
  FROM `{CPST_TABLE}`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY sync_timestamp DESC) = 1
),
vt AS (
  SELECT _id, atlas_id FROM `{VTICKETS_TABLE}`
  WHERE atlas_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC) = 1
),

-- event-based closes: SYSTEM = Overwatch, AGENT = Human
closes AS (
  SELECT e.ticket_id, e.actor_kind,
    DATE_TRUNC(DATE(DATETIME(e.created_at, 'Asia/Kolkata')), WEEK(MONDAY)) AS wk
  FROM `{TICKET_EVENTS_TABLE}` e, win
  WHERE e.action = 'status_changed' AND SAFE.STRING(e.new_value) = 'CLOSED'
    AND DATE(DATETIME(e.created_at, 'Asia/Kolkata')) BETWEEN win.ws AND win.we
),
close_tier AS (
  SELECT c.wk, c.actor_kind, cp.tier
  FROM closes c
  JOIN vt   ON c.ticket_id = vt._id
  JOIN cpst cp ON vt.atlas_id = cp.atlas_id
),
close_agg AS (
  SELECT wk,
    COUNTIF(actor_kind='SYSTEM' AND tier='L1') AS ow_l1,
    COUNTIF(actor_kind='SYSTEM' AND tier='L2') AS ow_l2,
    COUNTIF(actor_kind='AGENT'  AND tier='L1') AS hu_l1,
    COUNTIF(actor_kind='AGENT'  AND tier='L2') AS hu_l2,
    COUNTIF(tier IN ('L1','L2')) AS overall_closed
  FROM close_tier GROUP BY wk
),

created AS (
  SELECT DATE_TRUNC(DATE(DATETIME(created_at, 'Asia/Kolkata')), WEEK(MONDAY)) AS wk,
    COUNTIF(support_level='L1') AS cr_l1, COUNTIF(support_level='L2') AS cr_l2
  FROM `{TABLE_ID}`, win
  WHERE support_level IN ('L1','L2')
    AND DATE(DATETIME(created_at, 'Asia/Kolkata')) BETWEEN win.ws AND win.we
  GROUP BY 1
),

weeks AS (SELECT wk FROM created UNION DISTINCT SELECT wk FROM close_agg)

SELECT
  FORMAT_DATE('%Y-%m-%d', w.wk) AS week_start,
  COALESCE(c.cr_l1, 0) AS cr_l1,
  COALESCE(c.cr_l2, 0) AS cr_l2,
  SUM(COALESCE(c.cr_l1, 0) + COALESCE(c.cr_l2, 0)) OVER ww AS cr_cum,
  COALESCE(a.ow_l1, 0) AS ow_l1,
  COALESCE(a.ow_l2, 0) AS ow_l2,
  SUM(COALESCE(a.ow_l1, 0) + COALESCE(a.ow_l2, 0)) OVER ww AS ow_cum,
  COALESCE(a.hu_l1, 0) AS hu_l1,
  COALESCE(a.hu_l2, 0) AS hu_l2,
  SUM(COALESCE(a.hu_l1, 0) + COALESCE(a.hu_l2, 0)) OVER ww AS hu_cum,
  SUM(COALESCE(a.overall_closed, 0)) OVER ww AS closed_cum
FROM weeks w
LEFT JOIN created   c ON w.wk = c.wk
LEFT JOIN close_agg a ON w.wk = a.wk
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
    Weekly view: one row per week (Mon-Sun), most-recent on top.
    OW/Hu are event-based closes; *-Cum / Closed are running cumulative (computed oldest->newest).
    """
    if not rows:
        return "📊 *CS Ticket Stats — Weekly (IST)*\n\nNo data available."

    tot_cr_l1 = sum(r['cr_l1'] for r in rows)
    tot_cr_l2 = sum(r['cr_l2'] for r in rows)
    tot_ow_l1 = sum(r['ow_l1'] for r in rows)
    tot_ow_l2 = sum(r['ow_l2'] for r in rows)
    tot_hu_l1 = sum(r['hu_l1'] for r in rows)
    tot_hu_l2 = sum(r['hu_l2'] for r in rows)

    last = rows[-1]  # newest (cumulative computed oldest->newest)
    total_created = tot_cr_l1 + tot_cr_l2
    total_ow = tot_ow_l1 + tot_ow_l2
    total_hu = tot_hu_l1 + tot_hu_l2
    total_closed = last['closed_cum']

    first_mon = datetime.strptime(rows[0]['week_start'], '%Y-%m-%d')
    last_sun = datetime.strptime(last['week_start'], '%Y-%m-%d') + timedelta(days=6)
    range_str = f"{first_mon.strftime('%d %b')} – {last_sun.strftime('%d %b %Y')}"

    message = f"📊 *CS Ticket Stats — Weekly — last {len(rows)} completed weeks ({range_str}, IST)*\n\n"
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

    # Display most-recent week on top (cumulative columns are still computed oldest->newest)
    for row in reversed(rows):
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

    message += (
        "_C = Created · OW = closed by Overwatch · Hu = closed by Human · "
        "*-Cum / Closed = running cumulative across weeks · L1+L2 only._\n"
    )
    if len(rows) < 5:
        message += (
            f"_Showing {len(rows)} of 5 weeks — earlier weeks predate the ~18 May Trinity rollout "
            f"(no OW/Hu data); fills to 5 as they age out._\n"
        )

    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    message += f"\n_Generated: {now_ist.strftime('%d %b %Y %I:%M %p IST')}_"
    return message


# ==================== MAIN TASK ====================

def run_weekly_stats_to_slack(**context):
    """Query BigQuery for the last 5 completed weeks (capped to Trinity coverage), format, post."""
    logger.info("=" * 60)
    logger.info("CS TICKET STATS (WEEKLY): QUERY & PUSH TO SLACK")
    logger.info("=" * 60)

    logger.info("[1] Querying BigQuery for weekly ticket stats...")
    client = get_bigquery_client()
    notifier = SlackNotifier(SLACK_BOT_TOKEN, SLACK_CHANNEL_ID)

    try:
        query_job = client.query(WEEKLY_STATS_QUERY)
        results = query_job.result()
    except Exception as e:
        logger.error(f"      BigQuery query failed: {e}")
        notifier.send_message(f"🚨 *CS Ticket Stats (Weekly) Error*\n\nBigQuery query failed: {str(e)[:300]}")
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
        })

    logger.info(f"      ✓ Got {len(rows)} weekly rows")

    logger.info("[2] Building Slack message...")
    message = build_weekly_slack_message(rows)

    logger.info("[3] Sending to Slack...")
    notifier.send_message(message)

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
    description='Post weekly (last 5 wks, event-based, capped to Trinity coverage) CS ticket stats to #support-daily-metrics, Mondays 10am IST',
    schedule_interval='0 10 * * 1',  # Every Monday 10:00 AM IST
    catchup=False,
    tags=['slack', 'analytics', 'support_tickets', 'reporting', 'cs_metrics', 'weekly'],
)

push_weekly_task = PythonOperator(
    task_id='push_cs_weekly_ticket_stats_to_slack',
    python_callable=run_weekly_stats_to_slack,
    dag=dag,
)
