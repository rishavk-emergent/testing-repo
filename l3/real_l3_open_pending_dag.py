"""
RealL3 Open/Pending Tickets — Slack DAG (twice daily, IST)

Standalone DAG: twice a day (11:30 IST) it queries Trinity for currently
OPEN/PENDING tickets tagged `real_l3` at level L3, groups them by owning team,
and posts a formatted message to the #daily-report-l3-escalations channel.

Schedule: '30 11,23 * * *' interpreted in Asia/Kolkata -> 11:30 AM and 11:30 PM IST.
          (Trinity ingest from the Atlas mirror typically completes by 06:00 / 18:00 UTC,
           so we leave a generous buffer.)

Data sources (all real-time Trinity views in BigQuery):
  * trinity_database.trinity-base-trinity_tickets — base table for team_id + slack_link
        (v_tickets drops both of these fields during JSON flattening)
  * trinity_database.v_tickets   — universe (status/level/tag_ids/atlas_id/num)
  * trinity_database.v_agents    — assignee first/last name
  * trinity_database.v_teams     — team UUID -> name

Filter: level='L3' AND status IN ('OPEN','PENDING') AND real_l3 tag in tag_ids.
        real_l3 tag _id = '6a1f2e835ad901b459b7665f' (singleton, non-archived).

Triggers: NONE. Fully self-scheduled; not wired to any other DAG.
Output:   Slack message with per-team sections, each ticket shown with date,
          age, Trinity link, assignee, status, and optional Slack thread link.
"""

from __future__ import annotations

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

SLACK_CHANNEL_ID = os.getenv('REAL_L3_SLACK_CHANNEL', 'C0B4CHB1PRD')  # #daily-report-l3-escalations; override via env if needed

# real_l3 tag (Trinity, non-archived, singleton)
REAL_L3_TAG_ID = '6a1f2e835ad901b459b7665f'

# Trinity bucket the message header links to (live RealL3 view)
TRINITY_BUCKET_URL         = 'https://trinity-base.internal.emergent.host/tickets?bucket=6a1ee9695ad901b459b74089'
TRINITY_BUCKET_PENDING_URL = 'https://trinity-base.internal.emergent.host/tickets?tab=pending&bucket=6a1ee9695ad901b459b74089'

IST = timezone(timedelta(hours=5, minutes=30))

# ==================== BIGQUERY QUERY ====================
# Notes:
#   - v_tickets is versioned (every change appends a row). Dedupe per _id.
#   - v_tickets drops team_id and slack_link during JSON flattening (bug in view layer),
#     so we read both from the base table via regex over TO_JSON_STRING(data).
#   - Base table _id is JSON-wrapped as {"$oid": "..."}, view _id is plain.
#     Extract the 24-hex oid for the join.
#   - NULLs are padded to '-' so the downstream message builder can render them
#     consistently (and any future CSV transport doesn't strip them).

QUERY = fr"""
WITH
  base_latest AS (
    SELECT
      REGEXP_EXTRACT(_id, r'[0-9a-f]{{24}}') AS ticket_id,
      REGEXP_EXTRACT(TO_JSON_STRING(data),
        r'"team_id"\s*:\s*\{{\s*"\$oid"\s*:\s*"([0-9a-f]{{24}})"') AS team_oid,
      JSON_VALUE(data, '$.slack_link') AS slack_link
    FROM `emergent-default.trinity_database.trinity-base-trinity_tickets`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY datastream_metadata.source_timestamp DESC) = 1
  ),
  latest_tickets AS (
    SELECT *
    FROM `emergent-default.trinity_database.v_tickets`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC) = 1
  ),
  latest_agents AS (
    SELECT *
    FROM `emergent-default.trinity_database.v_agents`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC) = 1
  ),
  latest_teams AS (
    SELECT *
    FROM `emergent-default.trinity_database.v_teams`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC) = 1
  ),
  last_outbound AS (
    -- Most recent outbound message per ticket (any of agent or Overwatch).
    -- v_ticket_events doesn't need dedupe — each event is a single row.
    SELECT ticket_id, MAX(created_at) AS last_outbound_ts
    FROM `emergent-default.trinity_database.v_ticket_events`
    WHERE type = 'message' AND direction = 'outbound'
    GROUP BY ticket_id
  )
SELECT
  DATE(t.created_at) AS date,
  COALESCE(tm.name, 'Team Untagged') AS team,
  CAST(t.num AS INT64) AS ticket_number,
  COALESCE(NULLIF(TRIM(CONCAT(IFNULL(a.first_name,''),' ',IFNULL(a.last_name,''))),''), '-') AS assignee,
  CONCAT('https://trinity-base.internal.emergent.host/tickets/', t._id) AS ticket_url,
  COALESCE(b.slack_link, '-') AS slack_link,
  t.status,
  lo.last_outbound_ts
FROM latest_tickets t
LEFT JOIN base_latest    b  ON b.ticket_id = t._id
LEFT JOIN latest_teams   tm ON tm._id      = b.team_oid
LEFT JOIN latest_agents  a  ON a._id       = t.assigned_agent_id
LEFT JOIN last_outbound  lo ON lo.ticket_id = t._id
WHERE t.level = 'L3'
  AND UPPER(t.status) IN ('OPEN','PENDING')
  AND '{REAL_L3_TAG_ID}' IN UNNEST(t.tag_ids)
ORDER BY t.created_at DESC
"""


# ==================== MESSAGE BUILDER ====================

TEAM_EMOJI = {
    "Expo Team":       "🟤",
    "Retention Team":  "🔵",
    "Deployment Team": "🔴",
    "Wingman Team":    "🟣",
    "Conversion Team": "🟢",
    "Team Untagged":   "⚫",
}

TEAM_ORDER = [
    "Conversion Team",
    "Retention Team",
    "Expo Team",
    "Deployment Team",
    "Wingman Team",
    "Team Untagged",
]


def _normalize_assignee(name):
    name = (name or "").strip()
    if not name or name in ("—", "-", "–"):
        return "Unassigned"
    return name


def _normalize_team(name):
    name = (name or "").strip()
    if not name or name in ("—", "-", "–"):
        return "Team Untagged"
    return name


def _clean_slack(val):
    if not val:
        return ""
    v = str(val).strip()
    if not v or v.lower() in ("none", "null", "-", "—", "–"):
        return ""
    return v


def _format_date_display(d):
    return d.strftime("%d/%m/%Y")


def build_slack_message(rows: list) -> tuple[str, str, int, int]:
    """
    Build TWO Slack messages from the BigQuery rows:
      * open_msg     — all OPEN tickets (grouped by team), with full header.
      * pending_msg  — all PENDING tickets (grouped by team), short header.

    open_msg is the parent message; pending_msg is posted as a thread reply
    under the parent so the channel only sees the active-work list at top.

    Each row dict has: date (date), team, ticket_number, assignee, ticket_url,
    slack_link, status, last_outbound_ts.

    Returns: (open_msg, pending_msg, open_count, pending_count).
    A returned message is "" when that status has zero rows.
    """
    now_ist = datetime.now(IST)
    header_ts = now_ist.strftime("%d/%m/%Y %H:%M IST")
    today_ist = now_ist.date()

    # ---- Parse + normalize ----
    parsed = []
    for r in rows:
        status = (r.get("status") or "").strip().upper()
        if status not in ("OPEN", "PENDING"):
            continue
        date_val = r.get("date")
        if isinstance(date_val, str):
            try:
                date_val = datetime.strptime(date_val, "%Y-%m-%d").date()
            except Exception:
                date_val = today_ist
        age_days = (today_ist - date_val).days if date_val else 0

        # "time since last outbound message" → "X day/s"
        last_ts = r.get("last_outbound_ts")
        if last_ts is None:
            update_label = "no reply yet"
            update_days  = float("inf")   # never replied → most urgent, sorts to top
        else:
            from datetime import datetime as _dt, timezone as _tz
            now_utc = _dt.now(_tz.utc)
            if not getattr(last_ts, "tzinfo", None):
                last_ts = last_ts.replace(tzinfo=_tz.utc)
            delta_s = int((now_utc - last_ts).total_seconds())
            if delta_s < 0:
                delta_s = 0
            d = delta_s // 86400
            update_label = f"{d} day" + ("" if d == 1 else "s")
            update_days  = d

        parsed.append({
            "date_display":  _format_date_display(date_val) if date_val else "-",
            "date_sort":     date_val or today_ist,
            "age_days":      age_days,
            "update_label":  update_label,
            "update_days":   update_days,
            "team":          _normalize_team(r.get("team")),
            "ticket":        str(r.get("ticket_number", "")).strip(),
            "assignee":      _normalize_assignee(r.get("assignee")),
            "url":           (r.get("ticket_url") or "").strip(),
            "slack":         _clean_slack(r.get("slack_link")),
            "status":        status,
        })

    open_rows    = [r for r in parsed if r["status"] == "OPEN"]
    pending_rows = [r for r in parsed if r["status"] == "PENDING"]

    # ---- Shared formatter for one section ----
    NBSP = " "  # U+00A0 (kept here only because the file already uses it elsewhere)
    PAD  = " "  # U+2007 FIGURE SPACE — fixed-width, not collapsed by Slack

    def _pad(s, width):
        s = str(s)
        return s + PAD * max(0, width - len(s))

    def _age_label(d):
        return f"{d} day" + ("" if d == 1 else "s")

    def _render_section(subset, header_line):
        """Render a single message body for the given subset of rows."""
        if not subset:
            return ""
        # Per-column widths computed over THIS subset only — so OPEN and PENDING
        # each get their own tight widths (avoids one section's wide labels
        # bleeding into the other).
        w_date     = 10
        w_update   = max(len(r["update_label"]) for r in subset) + 2
        w_assignee = max(len(r["assignee"]) for r in subset) + 2
        w_status   = max(len(r["status"]) for r in subset) + 2

        # Group + order
        grouped = {}
        for r in subset:
            grouped.setdefault(r["team"], []).append(r)
        for team in grouped:
            grouped[team].sort(key=lambda r: r["update_days"], reverse=True)
        extra_teams   = sorted(t for t in grouped if t not in TEAM_ORDER)
        ordered_teams = [t for t in TEAM_ORDER if t in grouped] + extra_teams

        lines = [header_line]
        for team in ordered_teams:
            tr = grouped[team]
            if not tr:
                continue
            bullet = TEAM_EMOJI.get(team, "⚪")
            lines.append("")
            lines.append(f"{bullet} *{team}* ({len(tr)})")
            for r in tr:
                date_padded     = _pad(r["date_display"], w_date)
                update_padded   = _pad(r["update_label"], w_update)
                assignee_padded = _pad(r["assignee"], w_assignee)
                status_padded   = _pad(r["status"], w_status)
                ticket_link     = f"<{r['url']}|Trinity #{r['ticket']}>"
                slack_part      = f"  <{r['slack']}|💬 thread>" if r["slack"] else ""
                lines.append(
                    f"   `{date_padded}`  (`{update_padded}`)  {ticket_link}  `{assignee_padded}`  `{status_padded}`{slack_part}"
                )
        return "\n".join(lines)

    total_open    = len(open_rows)
    total_pending = len(pending_rows)
    total_all     = total_open + total_pending

    # ---- Parent (OPEN) ----
    if total_all == 0:
        open_header = f"*RealL3 Open Tickets* — {header_ts}  (<{TRINITY_BUCKET_URL}|0 open>)\n\n✅ All clear"
        return open_header, "", 0, 0

    open_header = (
        f"*RealL3 Open Tickets* — {header_ts}  "
        f"(<{TRINITY_BUCKET_URL}|{total_open} open>)"
    )
    open_msg = _render_section(open_rows, open_header) if total_open else open_header + "\n\n_(no open tickets right now — pending listed in thread)_"

    # ---- Thread (PENDING) ----
    pending_header = f"*Pending tickets* (waiting on customer)  (<{TRINITY_BUCKET_PENDING_URL}|{total_pending} pending>)"
    pending_msg    = _render_section(pending_rows, pending_header) if total_pending else ""

    return open_msg, pending_msg, total_open, total_pending


# ==================== MAIN TASK ====================

def run_real_l3_to_slack(**context):
    """
    1. Query BigQuery for real_l3 OPEN/PENDING L3 tickets.
    2. Build TWO Slack messages: parent (OPEN list) + thread reply (PENDING list).
    3. Post parent via chat.postMessage; if PENDING exists, post the second as a
       threaded reply under the parent.
    """
    logger.info("=" * 60)
    logger.info("REAL_L3 OPEN/PENDING: QUERY & PUSH TO SLACK")
    logger.info("=" * 60)

    bq_client = get_bigquery_client()
    notifier  = SlackNotifier(SLACK_BOT_TOKEN, SLACK_CHANNEL_ID)

    logger.info("[1] Querying BigQuery for real_l3 tickets...")
    try:
        query_job = bq_client.query(QUERY)
        results   = query_job.result()
    except Exception as e:
        logger.error(f"      BigQuery query failed: {e}")
        notifier.send_message(f"🚨 *RealL3 Report Error*\n\nBigQuery query failed: {str(e)[:300]}")
        raise

    rows = [{
        "date":             row.date,
        "team":             row.team,
        "ticket_number":    row.ticket_number,
        "assignee":         row.assignee,
        "ticket_url":       row.ticket_url,
        "slack_link":       row.slack_link,
        "status":           row.status,
        "last_outbound_ts": row.last_outbound_ts,
    } for row in results]
    logger.info(f"      ✓ Got {len(rows)} rows")

    logger.info("[2] Building parent + thread messages...")
    open_msg, pending_msg, n_open, n_pending = build_slack_message(rows)

    logger.info(f"[3] Posting parent (OPEN: {n_open}) to {SLACK_CHANNEL_ID}...")
    parent = notifier.send_message(
        open_msg,
        mrkdwn=True,
        unfurl_links=False,
        unfurl_media=False,
    )
    parent_ts = parent.get("ts")
    logger.info(f"      ✓ Parent posted ts={parent_ts}")

    if pending_msg:
        logger.info(f"[4] Posting thread reply (PENDING: {n_pending})...")
        notifier.send_message(
            pending_msg,
            thread_ts=parent_ts,
            mrkdwn=True,
            unfurl_links=False,
            unfurl_media=False,
        )
        logger.info("      ✓ Pending thread reply posted")
    else:
        logger.info("[4] No pending tickets — skipping thread reply")

    logger.info("=" * 60)
    logger.info("REAL_L3 OPEN/PENDING: COMPLETE")
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
    'real_l3_open_pending_slack',
    default_args=default_args,
    description='Post RealL3 OPEN/PENDING L3 tickets (grouped by team) to Slack twice a day IST',
    schedule_interval='30 11,23 * * *',  # 11:30 AM and 11:30 PM IST (after Trinity BQ sync)
    catchup=False,
    is_paused_upon_creation=False,  # deploy active so it fires on the next schedule without a manual unpause
    tags=['slack', 'trinity', 'real_l3', 'reporting', 'cs_team'],
)

push_real_l3_task = PythonOperator(
    task_id='push_real_l3_to_slack',
    python_callable=run_real_l3_to_slack,
    dag=dag,
)
