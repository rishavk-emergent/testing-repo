"""
L3 Needs Review Tickets — Slack DAG (daily, IST)

Standalone DAG: twice a day it queries Trinity for currently OPEN/PENDING tickets
at level L3 tagged `needs_review`, and posts a single flat list (oldest first)
to the #daily-report-l3-escalations channel. No team grouping / thread (source has only
date, ticket number, link and status).

Schedule: '30 11,23 * * *' interpreted in Asia/Kolkata -> 11:30 AM and 11:30 PM IST.
Data source: trinity_database.v_tickets (real-time Trinity view in BigQuery).
Filter: level='L3' AND status IN ('OPEN','PENDING') AND needs_review tag in tag_ids.
        needs_review tag _id = '6a1e3f824898b62618ffd100'.
Triggers: NONE. Fully self-scheduled; not wired to any other DAG.
Output: one Slack message — each ticket shown with date, age, Trinity link, status.
"""

from datetime import datetime, timedelta, timezone
import logging
import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN
from utils.slack.slack_client import SlackNotifier
from utils.slack.bigquery_client import get_bigquery_client

SLACK_CHANNEL_ID = os.getenv('L3_NEEDS_REVIEW_SLACK_CHANNEL', 'C0B4CHB1PRD')  # #daily-report-l3-escalations; override via env if needed

# needs_review tag (Trinity)
NEEDS_REVIEW_TAG_ID = '6a1e3f824898b62618ffd100'

# Trinity bucket the message header links to (live needs-review view)
TRINITY_BUCKET_URL = 'https://trinity-base.internal.emergent.host/tickets?bucket=6a1ee9ad5ad901b459b740b0'

IST = timezone(timedelta(hours=5, minutes=30))

# ==================== BIGQUERY QUERY ====================
# v_tickets is versioned (every change appends a row); dedupe per _id.
QUERY = f"""
WITH latest_tickets AS (
  SELECT *
  FROM `emergent-default.trinity_database.v_tickets`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC) = 1
)
SELECT
  DATE(t.created_at) AS date,
  CAST(t.num AS INT64) AS ticket_number,
  CONCAT('https://trinity-base.internal.emergent.host/tickets/', t._id) AS ticket_url,
  t.status
FROM latest_tickets t
WHERE t.level = 'L3'
  AND UPPER(t.status) IN ('OPEN','PENDING')
  AND '{NEEDS_REVIEW_TAG_ID}' IN UNNEST(t.tag_ids)
ORDER BY t.created_at DESC
"""


# ==================== MESSAGE BUILDER ====================

def _parse_date_for_sort(date_str):
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
    except Exception:
        return datetime.max.date()


def _format_date_display(date_str):
    try:
        d = datetime.strptime(date_str.strip(), "%Y-%m-%d").date()
        return d.strftime("%d/%m/%Y")
    except Exception:
        return date_str


def build_slack_message(rows: list) -> tuple:
    """
    Build a single Slack message from the BigQuery rows.

    Each row dict has: date (str 'YYYY-MM-DD'), ticket_number, ticket_url, status.
    Returns: (message, ticket_count). Flat list (no team grouping), oldest first.
    """
    today_ist = datetime.now(IST).date()

    parsed = []
    for r in rows:
        status = (r.get("status") or "").strip().upper()
        if status not in ("OPEN", "PENDING"):
            continue
        ds = (r.get("date") or "").strip()
        date_sort = _parse_date_for_sort(ds)
        age_days = (today_ist - date_sort).days if date_sort != datetime.max.date() else 0
        parsed.append({
            "date_display": _format_date_display(ds),
            "date_sort":    date_sort,
            "age_days":     age_days,
            "ticket":       str(r.get("ticket_number", "")).strip(),
            "url":          (r.get("ticket_url") or "").strip(),
            "status":       status,
        })

    # Oldest first (flat list — no team grouping since the source has no team)
    parsed.sort(key=lambda r: r["date_sort"])

    now_ist = datetime.now(IST).strftime("%d/%m/%Y %H:%M IST")
    total = len(parsed)
    header_label = "L3 Needs Review Tickets"

    if total == 0:
        return f"*{header_label}* — {now_ist}\n\n✅ Daily check: 0 needs review", 0

    lines = [f"*{header_label}* — {now_ist}  (<{TRINITY_BUCKET_URL}|{total} open/pending>)", ""]
    for r in parsed:
        age_label     = f"{r['age_days']} day" + ("" if r['age_days'] == 1 else "s")
        age_padded    = age_label.ljust(8)
        status_padded = r["status"].ljust(8)
        ticket_link   = f"<{r['url']}|Trinity #{r['ticket']}>"
        lines.append(
            f"   `{r['date_display']}`  (`{age_padded}`)  {ticket_link}  `{status_padded}`"
        )
    return "\n".join(lines), total


# ==================== MAIN TASK ====================

def run_l3_needs_review_to_slack(**context):
    """
    1. Query BigQuery for needs_review OPEN/PENDING L3 tickets.
    2. Build a single flat-list Slack message.
    3. Post it via SlackNotifier (shared bot token).
    """
    logger.info("=" * 60)
    logger.info("L3 NEEDS REVIEW: QUERY & PUSH TO SLACK")
    logger.info("=" * 60)

    bq_client = get_bigquery_client()
    notifier  = SlackNotifier(SLACK_BOT_TOKEN, SLACK_CHANNEL_ID)

    logger.info("[1] Querying BigQuery for needs_review tickets...")
    try:
        results = bq_client.query(QUERY).result()
    except Exception as e:
        logger.error(f"      BigQuery query failed: {e}")
        notifier.send_message(f"🚨 *L3 Needs Review Error*\n\nBigQuery query failed: {str(e)[:300]}")
        raise

    rows = [{
        "date":          row.date.isoformat() if row.date else "",
        "ticket_number": row.ticket_number,
        "ticket_url":    row.ticket_url,
        "status":        row.status,
    } for row in results]
    logger.info(f"      ✓ Got {len(rows)} rows")

    logger.info("[2] Building Slack message...")
    message, total = build_slack_message(rows)

    logger.info(f"[3] Posting ({total} tickets) to {SLACK_CHANNEL_ID}...")
    notifier.send_message(
        message,
        mrkdwn=True,
        unfurl_links=False,
        unfurl_media=False,
    )
    logger.info("=" * 60)
    logger.info("L3 NEEDS REVIEW: COMPLETE")
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
    'l3_needs_review_slack',
    default_args=default_args,
    description='Post L3 needs-review OPEN/PENDING tickets to Slack twice a day IST',
    schedule_interval='30 11,23 * * *',  # 11:30 AM and 11:30 PM IST
    catchup=False,
    is_paused_upon_creation=False,  # deploy active so it fires on the next schedule without a manual unpause
    tags=['slack', 'trinity', 'l3', 'needs_review', 'reporting', 'cs_team'],
)

push_l3_needs_review_task = PythonOperator(
    task_id='push_l3_needs_review_to_slack',
    python_callable=run_l3_needs_review_to_slack,
    dag=dag,
)
