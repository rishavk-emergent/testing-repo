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

SLACK_CHANNEL_ID = os.getenv('REAL_L3_SLACK_CHANNEL', 'C0B4CHB1PRD')  # #daily-report-l3-escalations; override via env if needed
# Team Untagged tickets post to a separate channel; defaults to the main channel until a dedicated one is set
UNTAGGED_SLACK_CHANNEL_ID = os.getenv('REAL_L3_UNTAGGED_CHANNEL', 'C0B7TBXP60M')  # fallback if config unavailable
CONFIG_QUERY_ID = 43625   # [RealL3] config: main_channel_id / untagged_channel_id (edit in Redash, no code push)
DATA_QUERY_ID   = 45385   # [RealL3] Open/Pending data (filter/dedup/columns editable in Redash, no code push)

# real_l3 tag (Trinity, non-archived, singleton)
REAL_L3_TAG_ID = '6a1f2e835ad901b459b7665f'

# Trinity bucket the message header links to (live RealL3 view)
TRINITY_BUCKET_URL         = 'https://trinity-base.internal.emergent.host/tickets?bucket=6a1ee9695ad901b459b74089'
TRINITY_BUCKET_PENDING_URL = 'https://trinity-base.internal.emergent.host/tickets?tab=pending&bucket=6a1ee9695ad901b459b74089'

IST = timezone(timedelta(hours=5, minutes=30))

# ==================== DATA (Redash #45385) ====================
# SQL lives in Redash query 45385 now (was inline). Edit filter/dedup/columns there.


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
    # Lazy imports (EME-595): keep slack_config Variable.get() out of DAG-parse so a
    # transient module/token issue can't make this a Broken DAG.
    from utils.slack import RedashClient
    from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN, REDASH_API_KEY, REDASH_BASE_URL
    from utils.slack.slack_client import SlackNotifier

    logger.info("=" * 60)
    logger.info("REAL_L3 OPEN/PENDING: QUERY & PUSH TO SLACK")
    logger.info("=" * 60)

    # channels from Redash config (precedence: env override > config query > built-in default)
    cfg = {}
    try:
        cfg = (RedashClient(api_key=REDASH_API_KEY, base_url=REDASH_BASE_URL).fetch_query_results(query_id=CONFIG_QUERY_ID, max_retries=3) or [{}])[0]
    except Exception as e:
        logger.warning("config query %s fetch failed, using defaults: %s", CONFIG_QUERY_ID, e)
    main_channel     = os.getenv('REAL_L3_SLACK_CHANNEL') or cfg.get('main_channel_id') or SLACK_CHANNEL_ID
    untagged_channel = os.getenv('REAL_L3_UNTAGGED_CHANNEL') or cfg.get('untagged_channel_id') or UNTAGGED_SLACK_CHANNEL_ID
    notifier  = SlackNotifier(SLACK_BOT_TOKEN, main_channel)

    logger.info("[1] Fetching real_l3 tickets from Redash #%d...", DATA_QUERY_ID)
    redash = RedashClient(api_key=REDASH_API_KEY, base_url=REDASH_BASE_URL)
    rows = redash.fetch_query_results(query_id=DATA_QUERY_ID, max_retries=3)
    if rows is None:
        logger.error("Redash data fetch failed (#%d) -- posting nothing; raising for retry", DATA_QUERY_ID)
        raise RuntimeError("Redash data fetch failed for query #%d" % DATA_QUERY_ID)
    rows = [r for r in rows if not r.get("is_anchor")]   # drop the always-present sentinel row (empty queue -> [])
    # Redash returns strings/floats; normalize the two typed fields the builder needs
    # (date stays a 'YYYY-MM-DD' string -> builder already parses it).
    for r in rows:
        tn = r.get("ticket_number")
        if tn is not None:
            try: r["ticket_number"] = int(float(tn))
            except (TypeError, ValueError): pass
        lo = r.get("last_outbound_ts")
        if isinstance(lo, str):
            ss = lo.strip()
            if not ss or ss in ("-", "None", "null"):
                r["last_outbound_ts"] = None
            else:
                try:
                    r["last_outbound_ts"] = datetime.fromisoformat(ss.replace("Z", "+00:00"))
                except Exception:
                    try: r["last_outbound_ts"] = datetime.strptime(ss[:19], "%Y-%m-%dT%H:%M:%S")
                    except Exception: r["last_outbound_ts"] = None
    logger.info("      ✓ Got %d rows", len(rows))

    logger.info("[2] Partitioning by team (Team Untagged -> separate channel)...")
    def _is_untagged(r):
        t = (r.get("team") or "").strip()
        return t in ("", "-", "\u2014", "\u2013", "Team Untagged")
    untagged_rows = [r for r in rows if _is_untagged(r)]
    main_rows     = [r for r in rows if not _is_untagged(r)]
    logger.info("      main=%d untagged=%d", len(main_rows), len(untagged_rows))

    def _post(channel, rowset, label):
        o_msg, p_msg, n_o, n_p = build_slack_message(rowset)
        if not o_msg:
            logger.info("      %s: nothing to post", label); return
        nt = SlackNotifier(SLACK_BOT_TOKEN, channel)
        parent = nt.send_message(o_msg, mrkdwn=True, unfurl_links=False, unfurl_media=False)
        pts = parent.get("ts")
        logger.info("      %s -> %s: parent posted (OPEN %d) ts=%s", label, channel, n_o, pts)
        if p_msg:
            nt.send_message(p_msg, thread_ts=pts, mrkdwn=True, unfurl_links=False, unfurl_media=False)
            logger.info("      %s -> %s: pending thread posted (PENDING %d)", label, channel, n_p)

    logger.info("[3] Posting main (excl. Team Untagged) -> %s", main_channel)
    _post(main_channel, main_rows, "main")
    logger.info("[4] Posting Team Untagged -> %s", untagged_channel)
    if untagged_rows:
        _post(untagged_channel, untagged_rows, "untagged")
    else:
        logger.info("      no Team Untagged tickets")

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
