"""
Polaris AI-QC — Reopen classification pipeline (Airflow port)

WHY THIS DAG EXISTS
  The reopen QC classifier used to run in-process on the Polaris pod
  (data-dashboard-mtd, APScheduler every 5 min) and wrote its output to
  BigQuery `support.reopen_classifications` by POSTing DELETE+INSERT SQL to
  Redash's /api/query_results adhoc endpoint under a shared Redash API key.
  On 2026-09-24 the Redash BigQuery service account had its write (dataEditor)
  IAM removed, so every adhoc write now returns 403 and all five classifier
  tables went stale. Reads still work; only the write path died.

  This DAG moves the pipeline off Redash entirely. It runs in Composer, whose
  service account (analytics-composer@emergent-default) holds project-level
  roles/bigquery.dataEditor — so it can read the input AND write the output
  directly with the BigQuery client. No Redash in the loop.

WHAT IT DOES (one tick, every 5 min — mirrors the pod exactly)
  1. READ input: run the SQL of Redash query #36960 directly against BigQuery
     (last 8d of CLOSED->non-CLOSED reopen events + the customer trigger msg,
     the attributed human agent, the OW RCA note, and the ticket's first msg).
  2. DIFF: read event_ids already in `support.reopen_classifications` for the
     last 10 days; keep only unclassified events (idempotent, self-backfilling
     within the 8-day input window).
  3. CLASSIFY: one gpt-4o-mini call per new event -> 5-bucket taxonomy
     (incorrect / incomplete / new_issue / clarification / noise).
  4. JUDGE: for incorrect/incomplete reopens that had a human agent reply, a
     second gpt-4o-mini call produces the agent-attribution judgement + issue.
  5. WRITE: DELETE the batch's event_ids, then load the rows into BigQuery
     (WRITE_APPEND) via the Composer SA. DELETE+load = idempotent re-runs.

DOWNSTREAM (unchanged): Redash #36967 / #37182 / #36969 read this table.

DEPENDENCIES (Composer PyPI packages): `openai`, `google-cloud-bigquery`.
SECRETS (Composer Secret Manager or Airflow Variable):
  REOPEN_OPENAI_API_KEY  — OpenAI key for the classifier account
                           (falls back to OPENAI_API_KEY).
  Optionally REOPEN_OPENAI_MODEL / REOPEN_JUDGE_MODEL (default gpt-4o-mini).

STATUS: staged in testing-repo for review. Ported verbatim from
  data-dashboard-mtd:/app/backend/services/reopen_pipeline/{pipeline,judge,taxonomy}.py
  — same prompts, same model, same table schema. The only change is the
  read/write transport (BigQuery client instead of Redash adhoc).
"""

from __future__ import annotations

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BQ_TABLE = "emergent-default.support.reopen_classifications"
OPENAI_MODEL = os.environ.get("REOPEN_OPENAI_MODEL", "gpt-4o-mini")
JUDGE_MODEL = os.environ.get("REOPEN_JUDGE_MODEL", "gpt-4o-mini")
CLASSIFIER_VERSION = "v1-2026-06-16"   # keep in lockstep with the pod's version tag
BQ_LOAD_BATCH = 500
_MSG_MAX_CHARS = 2000
_AGENT_MAX_CHARS = 800
_LOOKBACK_DAYS = 10                     # dedup window for existing event_ids

# The classifier's OpenAI account. Prefer Secret Manager, then Airflow Variable,
# then process env. Mirrors reopen_openai_client() on the pod.
try:
    from utils.secrets import get_secret          # Composer Secret Manager helper
except Exception:                                  # pragma: no cover - local/test fallback
    def get_secret(_k):
        return None


def _secret(name: str) -> str | None:
    v = get_secret(name)
    if v:
        return v
    try:
        v = Variable.get(name, default_var=None)
    except Exception:
        v = None
    return v or os.environ.get(name)


# ---------------------------------------------------------------------------
# BigQuery client (Composer ADC -> analytics-composer SA, has dataEditor)
# ---------------------------------------------------------------------------
def _bq():
    try:
        from utils.slack.bigquery_client import get_bigquery_client   # house helper
        return get_bigquery_client()
    except Exception:                                                  # pragma: no cover
        from google.cloud import bigquery
        return bigquery.Client(project="emergent-default")


# ---------------------------------------------------------------------------
# Input: SQL of Redash query #36960, run directly against BigQuery.
# (Verbatim copy — the only source of the reopen event feed.)
# ---------------------------------------------------------------------------
INPUT_SQL = r"""
DECLARE start_day DATE DEFAULT DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL 7 DAY);

WITH
trinity_tickets AS (
  SELECT _id, atlas_id, level
  FROM `emergent-default.trinity_database.v_tickets`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC) = 1
),
agents AS (
  SELECT _id, email, CONCAT(first_name,' ',last_name) AS agent_name
  FROM `emergent-default.trinity_database.v_agents`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC) = 1
),
reopens AS (
  SELECT
    ev.ticket_id,
    ev.created_at AS reopen_ts,
    ROW_NUMBER() OVER (PARTITION BY ev.ticket_id ORDER BY ev.created_at) AS reopen_index,
    ev.metadata_reason AS reopen_metadata_reason,
    ev.actor_kind AS reopen_actor_kind
  FROM `emergent-default.trinity_database.v_ticket_events` ev
  WHERE ev.action = 'status_changed'
    AND JSON_VALUE(ev.old_value) = 'CLOSED'
    AND JSON_VALUE(ev.new_value) <> 'CLOSED'
    AND DATE(ev.created_at, 'Asia/Kolkata') >= start_day
),
trigger_msg AS (
  SELECT r.ticket_id, r.reopen_ts, ev.body AS customer_msg
  FROM reopens r
  JOIN `emergent-default.trinity_database.v_ticket_events` ev
    ON ev.ticket_id = r.ticket_id
   AND ev.type = 'message'
   AND ev.actor_kind = 'CUSTOMER'
   AND ev.created_at BETWEEN TIMESTAMP_SUB(r.reopen_ts, INTERVAL 1 HOUR)
                         AND TIMESTAMP_ADD(r.reopen_ts, INTERVAL 5 MINUTE)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY r.ticket_id, r.reopen_ts ORDER BY ev.created_at DESC) = 1
),
first_cust AS (
  SELECT ev.ticket_id, ev.body AS original_customer_msg
  FROM `emergent-default.trinity_database.v_ticket_events` ev
  WHERE ev.type = 'message' AND ev.actor_kind = 'CUSTOMER'
    AND ev.ticket_id IN (SELECT DISTINCT ticket_id FROM reopens)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ev.ticket_id ORDER BY ev.created_at ASC) = 1
),
prev_agent_reply AS (
  SELECT r.ticket_id, r.reopen_ts, ev.body AS agent_msg
  FROM reopens r
  JOIN `emergent-default.trinity_database.v_ticket_events` ev
    ON ev.ticket_id = r.ticket_id
   AND ev.type = 'message'
   AND ev.actor_kind IN ('AGENT','SYSTEM')
   AND ev.created_at < r.reopen_ts
  QUALIFY ROW_NUMBER() OVER (PARTITION BY r.ticket_id, r.reopen_ts ORDER BY ev.created_at DESC) = 1
),
attributed_human AS (
  SELECT r.ticket_id, r.reopen_ts,
         ev.body AS agent_reply,
         ev.actor_agent_id,
         ev.created_at AS agent_reply_ts
  FROM reopens r
  JOIN `emergent-default.trinity_database.v_ticket_events` ev
    ON ev.ticket_id = r.ticket_id
   AND ev.type = 'message' AND ev.actor_kind = 'AGENT' AND ev.visibility = 'public'
   AND ev.created_at < r.reopen_ts
  QUALIFY ROW_NUMBER() OVER (PARTITION BY r.ticket_id, r.reopen_ts ORDER BY ev.created_at DESC) = 1
),
ow_rca AS (
  SELECT ah.ticket_id, ah.reopen_ts, ev.metadata_rca_note AS ow_rca_note
  FROM attributed_human ah
  JOIN `emergent-default.trinity_database.v_ticket_events` ev
    ON ev.ticket_id = ah.ticket_id
   AND ev.type = 'ai_draft'
   AND ev.metadata_rca_note IS NOT NULL AND ev.metadata_rca_note != ''
   AND ev.created_at < ah.agent_reply_ts
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ah.ticket_id, ah.reopen_ts ORDER BY ev.created_at DESC) = 1
)
SELECT
  CONCAT(CAST(r.ticket_id AS STRING), '|',
         FORMAT_TIMESTAMP('%Y%m%dT%H%M%SZ', r.reopen_ts)) AS event_id,
  r.ticket_id      AS ticket_id_mongo,
  vt.atlas_id,
  COALESCE(NULLIF(vt.level, ''), 'untagged') AS tier,
  r.reopen_ts,
  DATE(r.reopen_ts, 'Asia/Kolkata') AS reopen_day_ist,
  r.reopen_index,
  r.reopen_metadata_reason,
  r.reopen_actor_kind,
  m.customer_msg,
  fc.original_customer_msg,
  pa.agent_msg                AS prev_agent_msg,
  ah.actor_agent_id           AS attributed_agent_id,
  ag.email                    AS attributed_agent_email,
  ag.agent_name               AS attributed_agent_name,
  ah.agent_reply,
  ow.ow_rca_note
FROM reopens r
LEFT JOIN trinity_tickets vt    ON vt._id = r.ticket_id
LEFT JOIN trigger_msg m         ON m.ticket_id = r.ticket_id AND m.reopen_ts = r.reopen_ts
LEFT JOIN first_cust fc         ON fc.ticket_id = r.ticket_id
LEFT JOIN prev_agent_reply pa   ON pa.ticket_id = r.ticket_id AND pa.reopen_ts = r.reopen_ts
LEFT JOIN attributed_human ah   ON ah.ticket_id = r.ticket_id AND ah.reopen_ts = r.reopen_ts
LEFT JOIN ow_rca ow             ON ow.ticket_id = r.ticket_id AND ow.reopen_ts = r.reopen_ts
LEFT JOIN agents ag             ON ag._id = ah.actor_agent_id
ORDER BY r.reopen_ts DESC;
"""

# ---------------------------------------------------------------------------
# Taxonomy + prompts (verbatim from reopen_pipeline/taxonomy.py)
# ---------------------------------------------------------------------------
BUCKETS = ["incorrect", "incomplete", "new_issue", "clarification", "noise"]
_BUCKET_SET = set(BUCKETS)

SYSTEM_PROMPT = """You classify support-ticket REOPEN events into EXACTLY ONE of 5 buckets.

Context: a customer reopened a support ticket by replying after it was closed. Your job is to read the customer's reply (and optionally the agent's last message before close) and pick the single best bucket describing WHY the ticket reopened.

The 5 buckets (pick exactly one):

1. incorrect       — the customer says the fix did not work, the issue is still there, the agent gave the wrong answer, or the problem persists unchanged.
2. incomplete      — the fix worked partially, OR something else started breaking as a result, OR the customer needs more done to fully close the issue.
3. new_issue       — the customer is asking about a DIFFERENT problem in the same ticket (unrelated to the original issue).
4. clarification   — the fix may be correct but the customer needs help applying it / asks where to click / asks for status update / asks a follow-up question about the resolution.
5. noise           — the reopen carries no quality signal: thank-you, acknowledgment, providing info the agent asked for, auto-reply / out-of-office / bounce, agent-initiated reopen for internal review, billing/refund follow-up unrelated to the original fix.

Return ONLY a JSON object with exactly these keys:
  {"bucket":"<one of: incorrect, incomplete, new_issue, clarification, noise>",
   "sub":"<short snake_case tag, max 30 chars, e.g. didnt_work, thank_you, status_check>",
   "confidence":<float 0..1>,
   "evidence":"<<= 80 char direct quote from the customer that drove your decision>"}

Rules:
- bucket MUST be one of the 5 lowercase strings above.
- If the customer message is empty, missing, or pure pleasantry ("thanks", "\U0001f44d", "got it"), use "noise".
- If you cannot tell, default to "clarification" with confidence ≤ 0.4.
- If multiple buckets fit, pick the one the customer's main intent points to.
- Reply ONLY with the JSON — no prose, no markdown.

Examples:
- "This is not working still. The same error keeps appearing." -> {"bucket":"incorrect","sub":"still_broken","confidence":0.95,"evidence":"same error keeps appearing"}
- "Thanks so much! All good now." -> {"bucket":"noise","sub":"thank_you","confidence":0.99,"evidence":"Thanks so much! All good now."}
- "The credits issue is fixed but now I cant deploy." -> {"bucket":"incomplete","sub":"side_effect","confidence":0.9,"evidence":"credits issue is fixed but now I cant deploy"}
- "By the way, can I also change my plan?" -> {"bucket":"new_issue","sub":"different_problem","confidence":0.9,"evidence":"can I also change my plan"}
- "How exactly do I apply the env variable you mentioned?" -> {"bucket":"clarification","sub":"how_to_apply","confidence":0.95,"evidence":"How exactly do I apply the env variable"}
- "Out of office until Monday" -> {"bucket":"noise","sub":"auto_reply","confidence":0.99,"evidence":"Out of office"}
"""

JUDGE_SYSTEM_PROMPT = """You review a support ticket that was REOPENED — the customer came back unsatisfied. The failure is ALREADY categorized, so you do NOT decide whether it failed:
- bucket "incorrect"  -> the customer says the problem is still not fixed.
- bucket "incomplete" -> the fix was partial, or the customer says something was missed / still pending.

You produce TWO things: (A) a plain-English account of WHAT THE NAMED HUMAN AGENT DID and why it led to the reopen, and (B) a comprehensive understanding of the ACTUAL ISSUE the customer had on this ticket.

You are given (any may be empty):
- The ticket's ORIGINAL (first) customer message — the real problem the customer raised.
- The Overwatch (AI) RCA note available to the agent BEFORE they replied.
- The agent's actual reply to the customer.
- The customer's reopening message.

Return ONLY a JSON object with keys: judgement, issue, severity, confidence, evidence.
- judgement: plain-English. 1-2 sentences (up to ~200 words only if genuinely complex). MUST: (1) say how the agent's reply related to the Overwatch RCA — pasted ~verbatim, lightly reworded, built on it, wrote something independent, or gave no real diagnosis (only say "copied/pasted" if the wording is clearly the same); (2) state what went wrong, grounded in what the customer said on reopening; (3) stay observable — frame failure as outcome ("the customer reported it still failing"), not your own technical verdict; (4) if there is no Overwatch RCA note, just describe the agent's response.
- issue: a comprehensive, self-contained understanding of the ACTUAL underlying issue on this ticket — what the customer was really trying to do, what was broken, and the core problem — grounded in the original customer message and the conversation. 1-4 sentences. Describe the PROBLEM, not the agent's handling.
- severity: one of "high", "med", "low" (high = clear avoidable mishandling; low = minor / mostly outside the agent's control).
- confidence: a float 0..1 for how sure you are about the judgement.
- evidence: a short string with key supporting snippets (e.g. a phrase from the OW note + the agent reply + the customer rebuttal). <= 240 chars.

Reply ONLY with the JSON — no prose, no markdown."""

_JUDGE_BUCKETS = {"incorrect", "incomplete"}
_SEV = {"high", "med", "low"}
_FENCE_RE = re.compile(r"```(?:json)?\s*|\s*```", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def strip_html(s: str | None) -> str:
    if not s:
        return ""
    s = _TAG_RE.sub(" ", s)
    return _WS_RE.sub(" ", s).strip()


def _openai_client():
    from openai import OpenAI
    key = _secret("REOPEN_OPENAI_API_KEY") or _secret("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("No OpenAI key: set REOPEN_OPENAI_API_KEY (or OPENAI_API_KEY)")
    return OpenAI(api_key=key)


def _extract_json(raw: str) -> dict:
    return json.loads(_FENCE_RE.sub("", raw or "").strip())


# ---------------------------------------------------------------------------
# 5-bucket classifier (verbatim logic from pipeline.classify_one)
# ---------------------------------------------------------------------------
def classify_one(client, customer_msg, agent_msg=None) -> dict:
    customer_msg = strip_html(customer_msg)[:_MSG_MAX_CHARS]
    agent_msg = strip_html(agent_msg or "")[:_AGENT_MAX_CHARS]
    if not customer_msg:
        return {"bucket": "noise", "sub": "empty_message", "confidence": 0.5,
                "evidence": "", "_method": "empty"}
    user_payload = f"Customer reply (this triggered the reopen):\n{customer_msg}"
    if agent_msg:
        user_payload += f"\n\nAgent's previous message (for context):\n{agent_msg}"
    try:
        r = client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_payload},
            ],
            max_tokens=160,
            temperature=0,
        )
        out = _extract_json(r.choices[0].message.content)
    except Exception as e:
        logger.warning(f"[reopen qc] classify failed: {type(e).__name__}: {e}")
        return {"bucket": "clarification", "sub": "classify_error",
                "confidence": 0.0, "evidence": "", "_method": f"error:{type(e).__name__}"}
    bucket = (out.get("bucket") or "").strip().lower()
    if bucket not in _BUCKET_SET:
        bucket = "clarification"
    sub = str(out.get("sub") or "")[:30]
    try:
        conf = max(0.0, min(1.0, float(out.get("confidence") or 0)))
    except (TypeError, ValueError):
        conf = 0.5
    ev = (out.get("evidence") or "")[:240]
    return {"bucket": bucket, "sub": sub, "confidence": conf, "evidence": ev, "_method": "llm"}


def _classify_batch(rows, max_workers=6) -> list[dict]:
    client = _openai_client()
    results: list[dict | None] = [None] * len(rows)
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(classify_one, client, r.get("customer_msg"), r.get("prev_agent_msg")): i
                for i, r in enumerate(rows)}
        for fut in as_completed(futs):
            i = futs[fut]
            try:
                results[i] = fut.result()
            except Exception as e:
                results[i] = {"bucket": "clarification", "sub": "future_error",
                              "confidence": 0.0, "evidence": "",
                              "_method": f"future_error:{type(e).__name__}"}
    return [r for r in results if r is not None]


# ---------------------------------------------------------------------------
# Agent-attribution judge (verbatim logic from judge.py)
# ---------------------------------------------------------------------------
def _judge_one(client, agent_name, bucket, ow_rca, agent_reply, customer_msg, original_msg=None) -> dict:
    payload = (
        f"Agent: {agent_name or 'Unknown'}\n"
        f"Bucket: {bucket}\n\n"
        f"Original customer message (ticket first):\n{strip_html(original_msg)[:1200] or '(none)'}\n\n"
        f"Overwatch RCA note (pre-reply):\n{strip_html(ow_rca)[:1500] or '(none)'}\n\n"
        f"Agent reply to customer:\n{strip_html(agent_reply)[:1500]}\n\n"
        f"Customer reopening message:\n{strip_html(customer_msg)[:1000] or '(none)'}"
    )
    try:
        r = client.chat.completions.create(
            model=JUDGE_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": JUDGE_SYSTEM_PROMPT},
                {"role": "user", "content": payload},
            ],
            max_tokens=400,
            temperature=0,
        )
        out = json.loads(_FENCE_RE.sub("", r.choices[0].message.content or "").strip())
    except Exception as e:
        logger.warning(f"[reopen judge] failed: {type(e).__name__}: {e}")
        return {"judgement": "", "issue": "", "severity": None, "confidence": None, "evidence": ""}
    sev = str(out.get("severity") or "").strip().lower()
    if sev not in _SEV:
        sev = None
    try:
        conf = out.get("confidence")
        conf = max(0.0, min(1.0, float(conf))) if conf is not None else None
    except (TypeError, ValueError):
        conf = None
    return {
        "judgement": (out.get("judgement") or "").strip()[:1000],
        "issue": (out.get("issue") or "").strip()[:2000],
        "severity": sev,
        "confidence": conf,
        "evidence": (out.get("evidence") or "").strip()[:240],
    }


def judge_batch(rows, classifications, max_workers=6) -> list[dict | None]:
    out: list[dict | None] = [None] * len(rows)
    tasks: dict[tuple, list[int]] = {}
    for i, (r, c) in enumerate(zip(rows, classifications)):
        if not c or c.get("bucket") not in _JUDGE_BUCKETS:
            continue
        if not r.get("attributed_agent_email"):
            continue
        if not (r.get("agent_reply") or "").strip():
            continue
        key = (r.get("ticket_id_mongo"), (r.get("agent_reply") or "")[:500])
        tasks.setdefault(key, []).append(i)
    if not tasks:
        return out
    client = _openai_client()
    cache: dict[tuple, dict] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {}
        for key, idxs in tasks.items():
            r = rows[idxs[0]]
            c = classifications[idxs[0]]
            futs[ex.submit(_judge_one, client, r.get("attributed_agent_name"),
                           c.get("bucket"), r.get("ow_rca_note"),
                           r.get("agent_reply"), r.get("customer_msg"),
                           r.get("original_customer_msg"))] = key
        for fut in as_completed(futs):
            key = futs[fut]
            try:
                cache[key] = fut.result()
            except Exception as e:
                logger.warning(f"[reopen judge] future error: {type(e).__name__}: {e}")
                cache[key] = None
    for key, idxs in tasks.items():
        for i in idxs:
            out[i] = cache.get(key)
    return out


# ---------------------------------------------------------------------------
# BigQuery read / write (Composer SA — replaces the Redash adhoc transport)
# ---------------------------------------------------------------------------
def _fetch_input_rows(client) -> list[dict]:
    rows = [dict(r) for r in client.query(INPUT_SQL).result()]
    logger.info(f"[reopen qc] fetched {len(rows)} reopen events from Trinity (BQ)")
    return rows


def _existing_event_ids(client, days_back=_LOOKBACK_DAYS) -> set[str]:
    sql = (
        f"SELECT event_id FROM `{BQ_TABLE}` "
        f"WHERE reopen_day_ist >= DATE_SUB(CURRENT_DATE('Asia/Kolkata'), INTERVAL {days_back} DAY)"
    )
    return {r["event_id"] for r in client.query(sql).result() if r["event_id"]}


def _iso(v):
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    return str(v)


def _date_str(v):
    if not v:
        return None
    return str(v)[:10]


def _fnum(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _inum(v):
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _s(v):
    if v is None:
        return None
    v = str(v)
    return v if v != "" else None


def _out_row(row: dict, cls: dict, now_iso: str) -> dict:
    return {
        "event_id":           _s(row.get("event_id")),
        "ticket_id_mongo":    _s(row.get("ticket_id_mongo")),
        "atlas_id":           _s(row.get("atlas_id")),
        "tier":               _s(row.get("tier") or "untagged"),
        "reopen_ts":          _iso(row.get("reopen_ts")),
        "reopen_day_ist":     _date_str(row.get("reopen_day_ist")),
        "reopen_index":       _inum(row.get("reopen_index")),
        "bucket":             _s(cls.get("bucket")),
        "sub":                _s(cls.get("sub")),
        "confidence":         _fnum(cls.get("confidence")),
        "evidence":           _s(cls.get("evidence")),
        "classifier_method":  _s(cls.get("_method")),
        "classifier_version": CLASSIFIER_VERSION,
        "classifier_model":   OPENAI_MODEL,
        "classified_at":      now_iso,
        "attributed_agent_email": _s(row.get("attributed_agent_email")),
        "attributed_agent_name":  _s(row.get("attributed_agent_name")),
        "ai_judgement":       _s(row.get("ai_judgement")),
        "judge_severity":     _s(row.get("judge_severity")),
        "judge_confidence":   _fnum(row.get("judge_confidence")),
        "judge_evidence":     _s(row.get("judge_evidence")),
        "ai_issue":           _s(row.get("ai_issue")),
    }


def _write_to_bq(client, rows: list[dict], classifications: list[dict]) -> int:
    from google.cloud import bigquery
    if not rows:
        return 0
    now_iso = datetime.now(timezone.utc).isoformat()
    out_rows = [_out_row(r, c, now_iso) for r, c in zip(rows, classifications)]
    written = 0
    for i in range(0, len(out_rows), BQ_LOAD_BATCH):
        chunk = out_rows[i:i + BQ_LOAD_BATCH]
        ids = [r["event_id"] for r in chunk]
        # 1) idempotent delete of this batch's ids (handles re-runs / force-reclass)
        client.query(
            f"DELETE FROM `{BQ_TABLE}` WHERE event_id IN UNNEST(@ids)",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", ids)]
            ),
        ).result()
        # 2) append the freshly classified rows
        load = client.load_table_from_json(
            chunk, BQ_TABLE,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        )
        load.result()
        written += len(chunk)
        logger.info(f"[reopen qc] BQ wrote {written}/{len(out_rows)}")
    return written


# ---------------------------------------------------------------------------
# Pipeline (mirrors pipeline.run_pipeline)
# ---------------------------------------------------------------------------
def run_pipeline(**_):
    import time
    t0 = time.time()
    client = _bq()

    rows = _fetch_input_rows(client)
    if not rows:
        logger.info("[reopen qc] no reopen events in window; nothing to do")
        return

    existing = _existing_event_ids(client)
    logger.info(f"[reopen qc] BQ already has {len(existing)} events in {_LOOKBACK_DAYS}d window")
    new_rows = [r for r in rows if r["event_id"] not in existing]
    logger.info(f"[reopen qc] {len(new_rows)} events to classify")
    if not new_rows:
        return

    classifications = _classify_batch(new_rows)

    judgements = judge_batch(new_rows, classifications)
    for _r, _j in zip(new_rows, judgements):
        if _j:
            _r["ai_judgement"]     = _j.get("judgement")
            _r["ai_issue"]         = _j.get("issue")
            _r["judge_severity"]   = _j.get("severity")
            _r["judge_confidence"] = _j.get("confidence")
            _r["judge_evidence"]   = _j.get("evidence")

    written = _write_to_bq(client, new_rows, classifications)
    stats = {
        "fetched": len(rows), "new": len(new_rows),
        "classified": len(classifications), "written": written,
        "elapsed_s": round(time.time() - t0, 1),
    }
    logger.info(f"[reopen qc] done: {stats}")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "rishav.k@emergent.sh",
    "retries": 0,                      # fail fast; the next 5-min tick self-heals
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="polaris_reopen_qc",
    description="Polaris reopen QC classifier — LLM buckets + agent judgement, writes support.reopen_classifications (BQ, Composer SA)",
    default_args=default_args,
    schedule_interval="*/5 * * * *",   # every 5 min, matches the pod cadence
    start_date=pendulum.datetime(2026, 9, 27, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,                 # never overlap — concurrent writes would race
    dagrun_timeout=timedelta(minutes=30),
    tags=["polaris", "ai_qc", "reopen", "support"],
) as dag:
    PythonOperator(
        task_id="run_reopen_qc",
        python_callable=run_pipeline,
    )
