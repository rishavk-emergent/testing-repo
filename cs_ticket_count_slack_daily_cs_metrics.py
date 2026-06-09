"""
CS Ticket Count Slack DAG — DAILY (every 4h, IST)
Standalone DAG: it queries BigQuery for today's ticket creation/closure activity,
split by support level (L1/L2) and — for closures — by Overwatch vs Human, then
posts a formatted table to the #support-daily-metrics Slack channel.

Closures are EVENT-BASED (Option B): counted from Trinity ticket-event close
transitions, so a closure that happened in a given hour stays counted even if
the ticket later reopens (a reopen→re-close is a separate close event, counted
again). Each hour's closed count is an immutable point-in-time snapshot.

Definitions:
  OW-*  = status_changed -> CLOSED by actor_kind SYSTEM (Overwatch)   [v_ticket_events]
  Hu-*  = status_changed -> CLOSED by actor_kind AGENT  (human)       [v_ticket_events]
  Hu-*-new   = escalations to human in the hour (incl. re-escalations) [Overwatch audit:
               v_ab_auto_send_audit, checks.escalation_required = 'true']
  Hu-*-reopn = a CLOSED -> OPEN reopen on a ticket already escalated to human (audit)
  Tier (L1/L2) = ticket level at event time (v_ticket_events level history) for closes/
               reopens; CPST.support_level for escalations. L1/L2 only.
Created (C-*) comes from analytics.support_tickets_tat (created_at + support_level).

Escalation source note: the Overwatch audit (v_ab_auto_send_audit) is the system of
record for escalation decisions and is a strict superset of Trinity's
metadata_escalation_required (~10%/day more) — it also ties to Polaris 35503/35123.

Schedule: '0 3,7,11,15,19,23 * * *' interpreted in Asia/Kolkata -> 03,07,11,15,19,23 IST
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
AUDIT_TABLE = os.getenv('AUDIT_TABLE', 'emergent-default.overwatch_bq.v_ab_auto_send_audit')
CPST_TABLE = os.getenv('CPST_TABLE', 'emergent-default.analytics.closed_pending_support_tickets')
VTICKETS_TABLE = os.getenv('VTICKETS_TABLE', 'emergent-default.trinity_database.v_tickets')

# ==================== BIGQUERY QUERY ====================

HOURLY_STATS_QUERY = f"""
WITH
today AS (SELECT DATE(DATETIME(CURRENT_TIMESTAMP(), 'Asia/Kolkata')) AS d),

-- tier lookup for escalations (atlas ticket id -> L1/L2)
cpst AS (
  SELECT id AS atlas_id, COALESCE(NULLIF(support_level, 'N/A'), 'untagged') AS tier
  FROM `{CPST_TABLE}`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY sync_timestamp DESC) = 1
),

-- ESCALATIONS to human (Overwatch audit), distinct ticket per hour (incl. re-escalations across hours)
audit_esc AS (
  SELECT DISTINCT a.job_id AS atlas_id,
    EXTRACT(HOUR FROM DATETIME(a.started_at, 'Asia/Kolkata')) AS hour_ist
  FROM `{AUDIT_TABLE}` a, today
  WHERE DATE(a.started_at, 'Asia/Kolkata') = today.d
    AND JSON_VALUE(a.checks, '$.escalation_required') = 'true'
),
esc_agg AS (
  SELECT ae.hour_ist, COUNTIF(c.tier='L1') AS new_l1, COUNTIF(c.tier='L2') AS new_l2
  FROM audit_esc ae LEFT JOIN cpst c USING (atlas_id)
  GROUP BY 1
),
-- earliest escalation per ticket (for the "already escalated to human" reopen test)
audit_first AS (
  SELECT job_id AS atlas_id, MIN(started_at) AS first_esc_ts
  FROM `{AUDIT_TABLE}`
  WHERE JSON_VALUE(checks, '$.escalation_required') = 'true'
  GROUP BY job_id
),

-- map Trinity _id -> atlas_id (to join reopens to the audit)
vt AS (
  SELECT _id, atlas_id FROM `{VTICKETS_TABLE}`
  WHERE atlas_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC) = 1
),

-- CLOSES + REOPENS from the Trinity event stream (with point-in-time level)
tk AS (
  SELECT DISTINCT ticket_id FROM `{TICKET_EVENTS_TABLE}` e, today
  WHERE DATE(DATETIME(e.created_at, 'Asia/Kolkata')) = today.d
),
ev AS (
  SELECT e.ticket_id, e.created_at, e.seq, e.action, e.actor_kind,
    SAFE.STRING(e.new_value) AS nv, SAFE.STRING(e.old_value) AS ov
  FROM `{TICKET_EVENTS_TABLE}` e JOIN tk USING (ticket_id)
),
enr AS (
  SELECT *, LAST_VALUE(CASE WHEN action='level_changed' THEN nv END IGNORE NULLS)
      OVER (PARTITION BY ticket_id ORDER BY created_at, seq
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lvl_at
  FROM ev
),
te AS (
  SELECT *, EXTRACT(HOUR FROM DATETIME(created_at, 'Asia/Kolkata')) AS hour_ist
  FROM enr, today
  WHERE DATE(DATETIME(created_at, 'Asia/Kolkata')) = today.d AND lvl_at IN ('L1','L2')
),
close_agg AS (
  SELECT hour_ist,
    COUNTIF(action='status_changed' AND nv='CLOSED' AND actor_kind='SYSTEM' AND lvl_at='L1') AS ow_l1,
    COUNTIF(action='status_changed' AND nv='CLOSED' AND actor_kind='SYSTEM' AND lvl_at='L2') AS ow_l2,
    COUNTIF(action='status_changed' AND nv='CLOSED' AND actor_kind='AGENT'  AND lvl_at='L1') AS hu_l1,
    COUNTIF(action='status_changed' AND nv='CLOSED' AND actor_kind='AGENT'  AND lvl_at='L2') AS hu_l2,
    COUNTIF(action='status_changed' AND nv='CLOSED') AS overall_closed
  FROM te GROUP BY hour_ist
),
reopen_agg AS (
  SELECT te.hour_ist,
    COUNTIF(te.lvl_at='L1') AS hu_l1_reopn, COUNTIF(te.lvl_at='L2') AS hu_l2_reopn
  FROM te
  JOIN vt ON te.ticket_id = vt._id
  JOIN audit_first af ON vt.atlas_id = af.atlas_id AND af.first_esc_ts < te.created_at
  WHERE te.action='status_changed' AND te.ov='CLOSED' AND te.nv='OPEN'
  GROUP BY 1
),

created AS (
  SELECT EXTRACT(HOUR FROM DATETIME(created_at, 'Asia/Kolkata')) AS hour_ist,
    COUNTIF(support_level='L1') AS cr_l1, COUNTIF(support_level='L2') AS cr_l2
  FROM `{TABLE_ID}`, today
  WHERE support_level IN ('L1','L2') AND DATE(DATETIME(created_at, 'Asia/Kolkata')) = today.d
  GROUP BY 1
),

hours AS (
  SELECT hour_ist FROM created
  UNION DISTINCT SELECT hour_ist FROM close_agg
  UNION DISTINCT SELECT hour_ist FROM esc_agg
  UNION DISTINCT SELECT hour_ist FROM reopen_agg
)

SELECT
  h.hour_ist,
  COALESCE(cr.cr_l1, 0) AS cr_l1,
  COALESCE(cr.cr_l2, 0) AS cr_l2,
  SUM(COALESCE(cr.cr_l1, 0) + COALESCE(cr.cr_l2, 0)) OVER w AS cr_cum,
  COALESCE(cl.ow_l1, 0) AS ow_l1,
  COALESCE(cl.ow_l2, 0) AS ow_l2,
  SUM(COALESCE(cl.ow_l1, 0) + COALESCE(cl.ow_l2, 0)) OVER w AS ow_cum,
  COALESCE(e.new_l1, 0) AS hu_l1_new,
  COALESCE(r.hu_l1_reopn, 0) AS hu_l1_reopn,
  COALESCE(cl.hu_l1, 0) AS hu_l1,
  COALESCE(e.new_l2, 0) AS hu_l2_new,
  COALESCE(r.hu_l2_reopn, 0) AS hu_l2_reopn,
  COALESCE(cl.hu_l2, 0) AS hu_l2,
  SUM(COALESCE(cl.hu_l1, 0) + COALESCE(cl.hu_l2, 0)) OVER w AS hu_cum,
  SUM(COALESCE(cl.overall_closed, 0)) OVER w AS closed_cum
FROM hours h
LEFT JOIN created    cr USING (hour_ist)
LEFT JOIN close_agg  cl USING (hour_ist)
LEFT JOIN esc_agg    e  USING (hour_ist)
LEFT JOIN reopen_agg r  USING (hour_ist)
WINDOW w AS (ORDER BY h.hour_ist)
ORDER BY h.hour_ist
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
    Build the monospace Slack table. Closures are event-based (Option B);
    Hu-*-new are escalations-to-human (incl. re-escalations) from the Overwatch audit;
    Hu-*-reopn are reopens of already-escalated tickets.
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

    message += (
        "_C = Created · OW = closed by Overwatch · Hu = closed by Human · "
        "Hu-*-new = escalations to human (incl. re-escalations) · "
        "Hu-*-reopn = reopen of an already-escalated ticket · "
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
    description='Post CS ticket stats (L1/L2 x OW/Human, event-based) to #support-daily-metrics every 4h IST (03–23)',
    schedule_interval='0 3,7,11,15,19,23 * * *',  # IST: 03,07,11,15,19,23 (first 3am, last 11pm)
    catchup=False,
    tags=['slack', 'analytics', 'support_tickets', 'reporting', 'cs_metrics'],
)

push_stats_task = PythonOperator(
    task_id='push_cs_ticket_stats_to_slack',
    python_callable=run_ticket_stats_to_slack,
    dag=dag,
)
