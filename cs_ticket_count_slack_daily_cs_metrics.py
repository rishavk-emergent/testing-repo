"""
CS Ticket Count Slack DAG — DAILY (every 4h, IST)
Standalone DAG: every 4 hours (IST) it queries BigQuery for today's ticket
creation/closure activity, split by support level (L1/L2) and — for closures —
by Overwatch vs Human, then posts a formatted table to the #support-daily-metrics
Slack channel.

Closures are EVENT-BASED (Option B): counted from Trinity ticket-event close
transitions, so a closure that happened in a given hour stays counted even if
the ticket later reopens (a reopen→re-close is a separate close event, counted
again). This makes each hour's closed count an immutable point-in-time snapshot.

Definitions (all from trinity_database.v_ticket_events, bucketed by event time):
  OW-*     = status_changed -> CLOSED by actor_kind SYSTEM  (Overwatch close)
  Hu-*     = status_changed -> CLOSED by actor_kind AGENT   (human close)
  Hu-*-new   = ticket's FIRST escalation to human (metadata_escalation_required = true)
  Hu-*-reopn = a CLOSED -> OPEN reopen on a ticket already escalated to human
  Tier (L1/L2) = the ticket's level at event time (latest level_changed); L1/L2 only.
Created (C-*) comes from analytics.support_tickets_tat (created_at + support_level).

Schedule: '0 */4 * * *' interpreted in Asia/Kolkata -> 00, 04, 08, 12, 16, 20 IST
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

# ==================== BIGQUERY QUERY ====================

HOURLY_STATS_QUERY = f"""
WITH
today AS (SELECT DATE(DATETIME(CURRENT_TIMESTAMP(), 'Asia/Kolkata')) AS d),

-- tickets with at least one event today (pull full history for point-in-time state)
tk AS (
  SELECT DISTINCT ticket_id
  FROM `{TICKET_EVENTS_TABLE}` e, today
  WHERE DATE(DATETIME(e.created_at, 'Asia/Kolkata')) = today.d
),
ev AS (
  SELECT e.ticket_id, e.created_at, e.seq, e.action, e.actor_kind,
    SAFE.STRING(e.new_value) AS nv, SAFE.STRING(e.old_value) AS ov,
    e.metadata_escalation_required AS esc_req
  FROM `{TICKET_EVENTS_TABLE}` e
  JOIN tk USING (ticket_id)
),
-- enrich each event with the ticket's level at that point and prior-escalation count
enr AS (
  SELECT ticket_id, created_at, seq, action, actor_kind, nv, ov, esc_req,
    LAST_VALUE(CASE WHEN action = 'level_changed' THEN nv END IGNORE NULLS)
      OVER (PARTITION BY ticket_id ORDER BY created_at, seq
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lvl_at,
    COUNTIF(esc_req = TRUE)
      OVER (PARTITION BY ticket_id ORDER BY created_at, seq
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS esc_prior
  FROM ev
),
te AS (
  SELECT *, EXTRACT(HOUR FROM DATETIME(created_at, 'Asia/Kolkata')) AS hour_ist
  FROM enr, today
  WHERE DATE(DATETIME(created_at, 'Asia/Kolkata')) = today.d
    AND lvl_at IN ('L1', 'L2')
),
agg AS (
  SELECT hour_ist,
    COUNTIF(action='status_changed' AND nv='CLOSED' AND actor_kind='SYSTEM' AND lvl_at='L1') AS ow_l1,
    COUNTIF(action='status_changed' AND nv='CLOSED' AND actor_kind='SYSTEM' AND lvl_at='L2') AS ow_l2,
    COUNTIF(action='status_changed' AND nv='CLOSED' AND actor_kind='AGENT'  AND lvl_at='L1') AS hu_l1,
    COUNTIF(action='status_changed' AND nv='CLOSED' AND actor_kind='AGENT'  AND lvl_at='L2') AS hu_l2,
    COUNTIF(esc_req=TRUE AND esc_prior=0 AND lvl_at='L1') AS hu_l1_new,
    COUNTIF(esc_req=TRUE AND esc_prior=0 AND lvl_at='L2') AS hu_l2_new,
    COUNTIF(action='status_changed' AND ov='CLOSED' AND nv='OPEN' AND esc_prior>0 AND lvl_at='L1') AS hu_l1_reopn,
    COUNTIF(action='status_changed' AND ov='CLOSED' AND nv='OPEN' AND esc_prior>0 AND lvl_at='L2') AS hu_l2_reopn,
    COUNTIF(action='status_changed' AND nv='CLOSED') AS overall_closed
  FROM te
  GROUP BY hour_ist
),
created AS (
  SELECT EXTRACT(HOUR FROM DATETIME(created_at, 'Asia/Kolkata')) AS hour_ist,
    COUNTIF(support_level='L1') AS cr_l1,
    COUNTIF(support_level='L2') AS cr_l2
  FROM `{TABLE_ID}`, today
  WHERE support_level IN ('L1', 'L2')
    AND DATE(DATETIME(created_at, 'Asia/Kolkata')) = today.d
  GROUP BY 1
)

SELECT
  COALESCE(c.hour_ist, a.hour_ist) AS hour_ist,
  COALESCE(c.cr_l1, 0) AS cr_l1,
  COALESCE(c.cr_l2, 0) AS cr_l2,
  SUM(COALESCE(c.cr_l1, 0) + COALESCE(c.cr_l2, 0)) OVER w AS cr_cum,
  COALESCE(a.ow_l1, 0) AS ow_l1,
  COALESCE(a.ow_l2, 0) AS ow_l2,
  SUM(COALESCE(a.ow_l1, 0) + COALESCE(a.ow_l2, 0)) OVER w AS ow_cum,
  COALESCE(a.hu_l1_new, 0) AS hu_l1_new,
  COALESCE(a.hu_l1_reopn, 0) AS hu_l1_reopn,
  COALESCE(a.hu_l1, 0) AS hu_l1,
  COALESCE(a.hu_l2_new, 0) AS hu_l2_new,
  COALESCE(a.hu_l2_reopn, 0) AS hu_l2_reopn,
  COALESCE(a.hu_l2, 0) AS hu_l2,
  SUM(COALESCE(a.hu_l1, 0) + COALESCE(a.hu_l2, 0)) OVER w AS hu_cum,
  SUM(COALESCE(a.overall_closed, 0)) OVER w AS closed_cum
FROM created c
FULL OUTER JOIN agg a ON c.hour_ist = a.hour_ist
WINDOW w AS (ORDER BY COALESCE(c.hour_ist, a.hour_ist))
ORDER BY hour_ist
"""


# ==================== SLACK FUNCTIONS ====================

# (header, width) for the table; first column is left-justified, rest right.
_COLUMNS = [
    ('Hour', 7), ('C-L1', 6), ('C-L2', 6), ('C-Cum', 7),
    ('OW-L1', 7), ('OW-L2', 7), ('OW-Cum', 8),
    ('Hu-L1-new', 10), ('Hu-L1-reopn', 12), ('Hu-L1', 7),
    ('Hu-L2-new', 10), ('Hu-L2-reopn', 12), ('Hu-L2', 7),
    ('Hu-Cum', 8), ('Closed', 8),
]
_TABLE_WIDTH = sum(w for _, w in _COLUMNS)


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


def _fmt_row(cells: list) -> str:
    """Left-justify the first cell (label), right-justify the rest, per _COLUMNS widths."""
    out = []
    for i, (cell, (_, w)) in enumerate(zip(cells, _COLUMNS)):
        out.append(f"{cell:<{w}}" if i == 0 else f"{cell:>{w}}")
    return ''.join(out) + "\n"


def build_slack_message(rows: list, date_str: str) -> str:
    """
    Build the monospace Slack table. Closures are event-based (Option B):
    OW/Hu count close events, *-new/*-reopn are escalation/reopen events.
    *-Cum and Closed are running cumulative across hours.
    """
    if not rows:
        return f"📊 *CS Ticket Stats — {date_str} (IST)*\n\nNo ticket data available for today yet."

    s = lambda k: sum(r[k] for r in rows)
    last = rows[-1]
    total_created = s('cr_l1') + s('cr_l2')
    total_ow = s('ow_l1') + s('ow_l2')
    total_hu = s('hu_l1') + s('hu_l2')
    total_new = s('hu_l1_new') + s('hu_l2_new')
    total_reopn = s('hu_l1_reopn') + s('hu_l2_reopn')
    total_closed = last['closed_cum']

    message = f"📊 *CS Ticket Stats — {date_str} (IST)*\n\n"
    message += (
        f"🎫 Created: *{total_created}* (L1 {s('cr_l1')} / L2 {s('cr_l2')})  |  "
        f"✅ Closed: *{total_closed}* (OW {total_ow} / Hu {total_hu})\n"
    )
    message += f"👤 Escalated→Human: new {total_new} · reopen {total_reopn}\n\n"

    message += "```\n"
    message += _fmt_row([h for h, _ in _COLUMNS])
    message += "─" * _TABLE_WIDTH + "\n"
    for r in rows:
        message += _fmt_row([
            format_hour_label(r['hour_ist']), r['cr_l1'], r['cr_l2'], r['cr_cum'],
            r['ow_l1'], r['ow_l2'], r['ow_cum'],
            r['hu_l1_new'], r['hu_l1_reopn'], r['hu_l1'],
            r['hu_l2_new'], r['hu_l2_reopn'], r['hu_l2'],
            r['hu_cum'], r['closed_cum'],
        ])
    message += "─" * _TABLE_WIDTH + "\n"
    message += _fmt_row([
        'Total', s('cr_l1'), s('cr_l2'), last['cr_cum'],
        s('ow_l1'), s('ow_l2'), last['ow_cum'],
        s('hu_l1_new'), s('hu_l1_reopn'), s('hu_l1'),
        s('hu_l2_new'), s('hu_l2_reopn'), s('hu_l2'),
        last['hu_cum'], total_closed,
    ])
    message += "```\n"

    # Legend / definitions
    message += (
        "_C = Created · OW = closed by Overwatch · Hu = closed by Human · "
        "Hu-*-new = first escalation to human · Hu-*-reopn = reopen of an already-escalated ticket · "
        "*-Cum / Closed = running cumulative · L1+L2 only._\n"
    )
    message += (
        "_Closes are event-based: a ticket reopened then re-closed is counted each time, "
        "so this hour's count never shrinks if a ticket later reopens._\n"
    )

    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    message += f"\n_Last synced: {now_ist.strftime('%I:%M %p IST')}_"
    return message


# ==================== MAIN TASK ====================

def run_ticket_stats_to_slack(**context):
    """Query BigQuery for today's hourly ticket stats, format, and post to Slack."""
    logger.info("=" * 60)
    logger.info("CS TICKET STATS: QUERY & PUSH TO SLACK")
    logger.info("=" * 60)

    logger.info("[1] Querying BigQuery for hourly ticket stats...")
    client = get_bigquery_client()
    notifier = SlackNotifier(SLACK_BOT_TOKEN, SLACK_CHANNEL_ID)

    try:
        query_job = client.query(HOURLY_STATS_QUERY)
        results = query_job.result()
    except Exception as e:
        logger.error(f"      BigQuery query failed: {e}")
        notifier.send_message(f"🚨 *CS Ticket Stats Error*\n\nBigQuery query failed: {str(e)[:300]}")
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
            'hu_l1_new': row.hu_l1_new,
            'hu_l1_reopn': row.hu_l1_reopn,
            'hu_l1': row.hu_l1,
            'hu_l2_new': row.hu_l2_new,
            'hu_l2_reopn': row.hu_l2_reopn,
            'hu_l2': row.hu_l2,
            'hu_cum': row.hu_cum,
            'closed_cum': row.closed_cum,
        })

    logger.info(f"      ✓ Got {len(rows)} hourly rows")

    now_ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
    date_str = now_ist.strftime('%A, %d %b %Y')

    logger.info("[2] Building Slack message...")
    message = build_slack_message(rows, date_str)

    logger.info("[3] Sending to Slack...")
    notifier.send_message(message)

    logger.info("=" * 60)
    logger.info("CS TICKET STATS: COMPLETE")
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
    'cs_ticket_count_slack_daily_cs_metrics',
    default_args=default_args,
    description='Post CS ticket stats (L1/L2 x OW/Human, event-based) to #support-daily-metrics every 4h IST',
    schedule_interval='0 */4 * * *',  # Every 4h on the hour, IST: 00,04,08,12,16,20
    catchup=False,
    tags=['slack', 'analytics', 'support_tickets', 'reporting', 'cs_metrics'],
)

push_stats_task = PythonOperator(
    task_id='push_cs_ticket_stats_to_slack',
    python_callable=run_ticket_stats_to_slack,
    dag=dag,
)
