"""
L3 Needs Review Tickets — Slack DAG (twice daily, IST)

Standalone DAG: twice a day it posts the currently OPEN/PENDING tickets at level L3
tagged `needs_review` as a single flat list (oldest first) to Slack.

CONFIG LIVES IN REDASH (edit there, no code push):
  - Data   #43946  [L3 Needs Review] data   -> date / ticket_number / ticket_url / status
           (the L3 + OPEN/PENDING + needs_review-tag filter that used to be inline here;
            needs_review tag _id = '6a1e3f824898b62618ffd100')
  - Config #43945  [L3 Needs Review] config -> channel_id, trigger_hours
           channel_id     = Slack channel to post to (default #daily-report-l3-escalations)
           trigger_hours  = CSV of IST hours to fire (e.g. '11,23')

Schedule: Airflow ticks HOURLY at :30 ('30 * * * *' IST). The task fires only when the
current IST hour is in trigger_hours -> so '11,23' reproduces 11:30 AM / 11:30 PM IST.
Change the fire hours or the channel in Redash #43945; change the ticket filter in #43946.
No code push needed for any of those.

Env overrides (tests): L3_NEEDS_REVIEW_SLACK_CHANNEL redirects the channel;
L3_NEEDS_REVIEW_FORCE_RUN=1 bypasses the trigger-hour gate.

Data source: trinity_database.v_tickets (real-time Trinity view in BigQuery), read via Redash.
Output: one Slack message — each ticket shown with date, age, Trinity link, status.
On a Redash failure the task posts NOTHING and raises (Airflow retries); a genuine zero-ticket
day still posts the "0 needs review" all-clear. See the [2] Data block for how the two are told
apart (the data query always returns >=1 row).
"""

from datetime import datetime, timedelta, timezone
import logging
import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
from utils.slack import RedashClient
from utils.slack.slack_config import (
    REDASH_API_KEY,
    REDASH_BASE_URL,
    SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN,
)
from utils.slack.slack_client import SlackNotifier

# Redash queries (all editable knobs live here — no code push):
DATA_QUERY_ID   = 43946   # [L3 Needs Review] data   -> ticket rows
CONFIG_QUERY_ID = 43945   # [L3 Needs Review] config -> channel_id, trigger_hours

DEFAULT_CHANNEL_ID = 'C0B4CHB1PRD'  # #daily-report-l3-escalations (fallback if config unreadable)
DEFAULT_TRIGGER_HOURS = '11,23'     # IST hours to fire (fallback)

# env overrides (tests): redirect channel / bypass the trigger-hour gate
ENV_CHANNEL = os.getenv('L3_NEEDS_REVIEW_SLACK_CHANNEL')
FORCE_RUN   = os.getenv('L3_NEEDS_REVIEW_FORCE_RUN') == '1'

# Trinity bucket the message header links to (live needs-review view)
TRINITY_BUCKET_URL = 'https://trinity-base.internal.emergent.host/tickets?bucket=6a1ee9ad5ad901b459b740b0'

IST = timezone(timedelta(hours=5, minutes=30))


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
    Build a single Slack message from the Redash data rows.

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
        # ticket_number comes back from Redash JSON as a float — render as int
        tn = r.get("ticket_number")
        try:
            ticket = str(int(float(tn))) if tn is not None and str(tn).strip() != "" else ""
        except (TypeError, ValueError):
            ticket = str(tn).strip()
        parsed.append({
            "date_display": _format_date_display(ds),
            "date_sort":    date_sort,
            "age_days":     age_days,
            "ticket":       ticket,
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
    1. Read config (channel + trigger_hours) from Redash #43945; gate on the IST hour.
    2. Fetch needs_review OPEN/PENDING L3 tickets from Redash #43946.
    3. Build a single flat-list Slack message and post it via SlackNotifier.
    """
    logger.info("=" * 60)
    logger.info("L3 NEEDS REVIEW: QUERY & PUSH TO SLACK")
    logger.info("=" * 60)

    redash = RedashClient(api_key=REDASH_API_KEY, base_url=REDASH_BASE_URL)

    # [1] Config + trigger gate. Config uses defaults on failure (not raise) so a Redash blip
    # on a NON-trigger hourly tick just skips quietly instead of flooding Airflow with retries;
    # if Redash is truly down at a trigger hour, the [2] data fetch below fails and raises anyway.
    cfg = (redash.fetch_query_results(query_id=CONFIG_QUERY_ID, max_retries=3) or [{}])[0]
    channel = ENV_CHANNEL or cfg.get('channel_id') or DEFAULT_CHANNEL_ID
    try:
        hours = {int(str(h).strip()) for h in str(cfg.get('trigger_hours') or DEFAULT_TRIGGER_HOURS).split(',') if str(h).strip() != ''}
    except (TypeError, ValueError):
        hours = {int(h) for h in DEFAULT_TRIGGER_HOURS.split(',')}

    now_ist = pendulum.now('Asia/Kolkata')
    if not FORCE_RUN and now_ist.hour not in hours:
        logger.info("L3 needs review: IST hour %d not in trigger_hours %s -> skip", now_ist.hour, sorted(hours))
        return
    logger.info("[gate] hour=%d trigger_hours=%s force=%s channel=%s", now_ist.hour, sorted(hours), FORCE_RUN, channel)

    # [2] Data. RedashClient returns None on a hard failure (it never raises). On failure we
    # post NOTHING and raise so Airflow retries -- never a false "0 needs review" all-clear.
    # #43946 always returns >=1 row (single-row anchor), so None here unambiguously means the
    # fetch failed, not that there are zero tickets (a genuine empty day is one all-NULL row,
    # which build_slack_message drops on the OPEN/PENDING status check -> "0 needs review").
    logger.info("[1] Fetching needs_review tickets from Redash #%d...", DATA_QUERY_ID)
    rows = redash.fetch_query_results(query_id=DATA_QUERY_ID, max_retries=3)
    if rows is None:
        logger.error("Redash data fetch failed (#%d) -- posting nothing; raising for Airflow retry", DATA_QUERY_ID)
        raise RuntimeError(f"Redash data fetch failed for query #{DATA_QUERY_ID}")
    logger.info("      ✓ Got %d rows", len(rows))

    # [3] Build + post
    logger.info("[2] Building Slack message...")
    message, total = build_slack_message(rows)

    logger.info("[3] Posting (%d tickets) to %s...", total, channel)
    SlackNotifier(SLACK_BOT_TOKEN, channel).send_message(
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
    description='Post L3 needs-review OPEN/PENDING tickets to Slack; channel + fire hours from Redash #43945, data from #43946',
    schedule_interval='30 * * * *',  # hourly tick at :30 IST; task self-gates on config trigger_hours
    catchup=False,
    is_paused_upon_creation=False,  # deploy active so it fires on the next scheduled tick
    tags=['slack', 'trinity', 'l3', 'needs_review', 'reporting', 'cs_team'],
)

push_l3_needs_review_task = PythonOperator(
    task_id='push_l3_needs_review_to_slack',
    python_callable=run_l3_needs_review_to_slack,
    dag=dag,
)
