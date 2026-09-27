"""
Polaris AI-QC — Reopen classification WRITER (Airflow port)

SPLIT OF RESPONSIBILITIES
  Classification LOGIC stays in Polaris (data-dashboard-mtd), so it can be edited
  freely without touching this DAG:
    services/reopen_pipeline/{pipeline,judge,taxonomy}.py  — fetch + classify + judge
    routes/reopen.py  ->  GET /api/reopen/pending           — classify NEW events, RETURN rows (no write)

  This DAG is only the WRITER. It exists because on 2026-09-24 the Redash
  BigQuery service account lost write (dataEditor) IAM, so Polaris can no longer
  push its rows to BigQuery through Redash. Composer's service account
  (analytics-composer@emergent-default) still holds project-level dataEditor, so
  this DAG pulls the already-classified rows from Polaris and loads them to BQ.

WHAT IT DOES (every 5 min)
  Loop:
    1. GET {POLARIS_BASE_URL}/api/reopen/pending?limit=PULL_LIMIT
       -> Polaris classifies the next batch of NEW reopen events and returns
          them as `support.reopen_classifications`-shaped rows (it dedups against
          BQ itself, so each call returns only rows not yet written).
    2. DELETE those event_ids + load the rows into BQ (WRITE_APPEND) as the
       Composer SA. DELETE+append = idempotent re-runs.
    3. Repeat until a call returns fewer than PULL_LIMIT rows (backlog drained).
  Because Polaris's feed is the last 8 days, this self-backfills the Sep-24 gap
  on the first few ticks, then steady-states at a few rows per 5 min.

DOWNSTREAM (unchanged): Redash #36967 / #37182 / #36969 read this table.

CONFIG (Airflow Variable or Composer Secret; env fallback):
  POLARIS_BASE_URL  — base URL of the Polaris backend that serves /api/reopen/pending.
                      (dev pod: https://data-dashboard-mtd.internal.preview.emergentagent.com ;
                       set to the prod Polaris host for the prod DAG.)
  POLARIS_API_KEY   — optional; sent as `Authorization: Bearer <key>` if set
                      (the endpoint is currently open, so this is future-proofing).

DEPENDENCIES (Composer PyPI): `requests`, `google-cloud-bigquery` (both already present).

NOTE: once this DAG owns the write, disable the pod's in-process writer with
  REOPEN_SCHEDULER_DISABLED=1 so it stops the (now-failing) Redash writes + wasted LLM calls.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BQ_TABLE = "emergent-default.support.reopen_classifications"
PULL_LIMIT = 500          # rows classified + returned per /pending call
MAX_BATCHES = 25          # safety cap (25 * 500 = 12.5k rows / tick, well over the 8-day feed)
HTTP_TIMEOUT = 285        # seconds — /pending runs the LLM classifier synchronously
WAKE_RETRIES = 3          # the pod may be cold; retry the first hit

_DEFAULT_BASE = "https://data-dashboard-mtd.internal.preview.emergentagent.com"

try:
    from utils.secrets import get_secret          # Composer Secret Manager helper
except Exception:                                  # pragma: no cover - local/test fallback
    def get_secret(_k):
        return None


def _cfg(name: str, default: str | None = None) -> str | None:
    v = get_secret(name)
    if v:
        return v
    try:
        v = Variable.get(name, default_var=None)
    except Exception:
        v = None
    if v:
        return v
    import os
    return os.environ.get(name, default)


def _bq():
    try:
        from utils.slack.bigquery_client import get_bigquery_client   # house helper (Composer ADC)
        return get_bigquery_client()
    except Exception:                                                  # pragma: no cover
        from google.cloud import bigquery
        return bigquery.Client(project="emergent-default")


# ---------------------------------------------------------------------------
# Pull one batch of classified rows from Polaris
# ---------------------------------------------------------------------------
def _pull_batch(base_url: str, headers: dict, limit: int) -> dict:
    url = f"{base_url.rstrip('/')}/api/reopen/pending"
    last_err = None
    for attempt in range(1, WAKE_RETRIES + 1):
        try:
            r = requests.get(url, params={"limit": limit}, headers=headers, timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:            # cold pod / transient — retry a few times
            last_err = e
            logger.warning(f"[reopen writer] /pending attempt {attempt} failed: {type(e).__name__}: {e}")
    raise RuntimeError(f"/pending unreachable after {WAKE_RETRIES} attempts: {last_err}")


# ---------------------------------------------------------------------------
# Idempotent write: DELETE the batch's event_ids, then append the rows
# ---------------------------------------------------------------------------
def _write_batch(client, rows: list[dict]) -> int:
    from google.cloud import bigquery
    if not rows:
        return 0
    ids = [r["event_id"] for r in rows if r.get("event_id")]
    if ids:
        client.query(
            f"DELETE FROM `{BQ_TABLE}` WHERE event_id IN UNNEST(@ids)",
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("ids", "STRING", ids)]
            ),
        ).result()
    load = client.load_table_from_json(
        rows, BQ_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    load.result()
    return len(rows)


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------
def run_writer(**_):
    base_url = _cfg("POLARIS_BASE_URL", _DEFAULT_BASE)
    api_key = _cfg("POLARIS_API_KEY")
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}

    client = _bq()
    total_written = 0
    for batch_no in range(1, MAX_BATCHES + 1):
        res = _pull_batch(base_url, headers, PULL_LIMIT)
        rows = res.get("rows") or []
        logger.info(f"[reopen writer] batch {batch_no}: fetched={res.get('fetched')} "
                    f"new={res.get('new')} rows={len(rows)}")
        if not rows:
            break
        written = _write_batch(client, rows)
        total_written += written
        logger.info(f"[reopen writer] batch {batch_no}: wrote {written} (total {total_written})")
        if len(rows) < PULL_LIMIT:        # backlog drained
            break
    logger.info(f"[reopen writer] done: total_written={total_written}")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "rishav.k@emergent.sh",
    "retries": 0,                          # fail fast; the next 5-min tick self-heals
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="polaris_reopen_qc",
    description="Polaris reopen QC WRITER — pulls classified rows from Polaris /api/reopen/pending, loads support.reopen_classifications (Composer SA)",
    default_args=default_args,
    schedule_interval="*/5 * * * *",       # every 5 min, matches the old pod cadence
    start_date=pendulum.datetime(2026, 9, 27, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,                     # never overlap — concurrent writes would race
    dagrun_timeout=timedelta(minutes=30),
    tags=["polaris", "ai_qc", "reopen", "support"],
) as dag:
    PythonOperator(
        task_id="pull_and_load_reopen_qc",
        python_callable=run_writer,
    )
