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

CONFIG — lives in Redash (edit there, no code push), house convention:
  Query [polaris-reopen-qc] config (#49612) returns ONE row with columns:
    polaris_base_url — base URL of the Polaris backend serving /api/reopen/pending
                       (dev pod default; set to the prod Polaris host for the prod DAG)
    polaris_api_key  — optional bearer for that endpoint (NULL = endpoint is open)
    pull_limit       — rows classified + returned per /pending call
    max_batches      — safety cap on batches drained per 5-min tick
    http_timeout     — seconds to wait on /pending (LLM classifier runs synchronously)
    wake_retries     — retries for a cold pod on the first hit
  Only the config-query id is (optionally) overridable via env/Variable REOPEN_QC_CONFIG_QUERY_ID;
  everything else is read from the row, with hardcoded fallbacks if Redash is unreachable.
  (Reading a Redash query is a SELECT — unaffected by the BigQuery write-permission change.)

DEPENDENCIES (Composer PyPI): `requests`, `google-cloud-bigquery` (both already present).

NOTE: once this DAG owns the write, disable the pod's in-process writer with
  REOPEN_SCHEDULER_DISABLED=1 so it stops the (now-failing) Redash writes + wasted LLM calls.
"""

from __future__ import annotations

import logging
import time
from datetime import timedelta

import os

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.redash import RedashClient   # plugins/utils/redash — RedashClient() self-resolves key+base
from utils.bq import get_bq_client      # plugins/utils/bq — Composer BQ client (ADC)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config — the connection/tuning lives in Redash (edit there, no code push).
# Only the config-query id is overridable via env/Variable; the rest is read
# from that query's single row, with hardcoded fallbacks if Redash is down.
# ---------------------------------------------------------------------------
BQ_TABLE = "emergent-default.support.reopen_classifications"
CONFIG_QUERY_ID = int(os.getenv("REOPEN_QC_CONFIG_QUERY_ID", "49612"))  # [polaris-reopen-qc] config

# PROD-SAFE fallbacks: used only if the Redash config query is unreachable on a tick.
# polaris_base_url MUST be the prod host (never the dev/preview pod) so a config-fetch
# blip can never cause the prod writer to ingest preview classifications into BQ.
# max_batches is capped so worst-case (max_batches * http_timeout) stays under dagrun_timeout.
_FALLBACK = {
    "polaris_base_url": "https://polaris-analytics.internal.emergent.host",
    "polaris_api_key": None,
    "pull_limit": 500,
    "max_batches": 8,          # 8 * 285s = 38m < 45m dagrun_timeout
    "http_timeout": 285,
    "wake_retries": 3,
}


def _load_config() -> dict:
    """Read the one-row Redash config query; fall back to _FALLBACK if unreachable."""
    cfg = dict(_FALLBACK)
    try:
        redash = RedashClient()   # self-resolves API key (Secret Manager) + base URL
        rows = redash.fetch_query_results(query_id=CONFIG_QUERY_ID, max_retries=3)
        if rows:
            row = rows[0]
            for k in _FALLBACK:
                if row.get(k) is not None:
                    cfg[k] = row[k]
    except Exception as e:                          # never let config fetch break the run
        logger.warning(f"[reopen writer] config fetch failed, using fallbacks: {type(e).__name__}: {e}")
    cfg["pull_limit"] = int(cfg["pull_limit"])
    cfg["max_batches"] = int(cfg["max_batches"])
    cfg["http_timeout"] = int(cfg["http_timeout"])
    cfg["wake_retries"] = int(cfg["wake_retries"])
    return cfg


def _bq():
    return get_bq_client(project="emergent-default")   # Composer ADC (analytics-composer SA, has dataEditor)


# ---------------------------------------------------------------------------
# Pull one batch of classified rows from Polaris
# ---------------------------------------------------------------------------
def _pull_batch(base_url: str, headers: dict, limit: int, timeout: int, wake_retries: int) -> dict:
    url = f"{base_url.rstrip('/')}/api/reopen/pending"
    last_err = None
    for attempt in range(1, wake_retries + 1):
        try:
            r = requests.get(url, params={"limit": limit}, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:            # cold pod / transient — back off so the pod can warm
            last_err = e
            logger.warning(f"[reopen writer] /pending attempt {attempt} failed: {type(e).__name__}: {e}")
            if attempt < wake_retries:
                time.sleep(min(60, 10 * (2 ** (attempt - 1))))   # 10s, 20s, 40s… (bounded)
    raise RuntimeError(f"/pending unreachable after {wake_retries} attempts: {last_err}")


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
    cfg = _load_config()
    base_url = cfg["polaris_base_url"]
    api_key = cfg["polaris_api_key"]
    pull_limit = cfg["pull_limit"]
    max_batches = cfg["max_batches"]
    timeout = cfg["http_timeout"]
    wake_retries = cfg["wake_retries"]
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    logger.info(f"[reopen writer] config: base_url={base_url} pull_limit={pull_limit} max_batches={max_batches}")

    client = _bq()
    total_written = 0
    for batch_no in range(1, max_batches + 1):
        res = _pull_batch(base_url, headers, pull_limit, timeout, wake_retries)
        rows = res.get("rows") or []
        logger.info(f"[reopen writer] batch {batch_no}: fetched={res.get('fetched')} "
                    f"new={res.get('new')} rows={len(rows)}")
        if not rows:
            break
        written = _write_batch(client, rows)
        total_written += written
        logger.info(f"[reopen writer] batch {batch_no}: wrote {written} (total {total_written})")
        if len(rows) < pull_limit:        # backlog drained
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
    dagrun_timeout=timedelta(minutes=45),  # covers worst-case max_batches * http_timeout (8*285s=38m)
    tags=["polaris", "ai_qc", "reopen", "support"],
) as dag:
    PythonOperator(
        task_id="pull_and_load_reopen_qc",
        python_callable=run_writer,
    )
