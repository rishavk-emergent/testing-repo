"""
GST Invoice - New Vendor Submission Slack Alert (every 5 min, IST)

Watches the "GST Invoice" Google Form response sheet and posts a Slack alert for every NEW
vendor submission (a row whose Timestamp is filled and that we haven't alerted before). Each
submission is alerted exactly once (dedup state table support.gst_invoice_pinged).

HOW IT READS THE SHEET (no key needed):
  Composer workers run AS a Google service account (Application Default Credentials). We read the
  sheet through the Sheets REST API using google.auth.default() -> a short-lived access token.
  >>> PREREQUISITE: share the sheet (Viewer) with the Composer runtime SA email. <<<
  This sidesteps the org policy that blocks downloading service-account keys.

Sources:
  Sheet -> Google Sheets REST API (values.get), first/target tab, header-mapped rows.
  Dedup -> support.gst_invoice_pinged (row_key = sha1(timestamp|email|vendor)).

Schedule: '*/5 * * * *' Asia/Kolkata.
Channel:  <set GST_INVOICE_SLACK_CHANNEL>; override the env for testing.
"""

from datetime import datetime, timedelta, timezone
import logging, os, hashlib, json

import pendulum
import requests
import google.auth
import google.auth.transport.requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack.slack_config import SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN
from utils.slack.bigquery_client import get_bigquery_client

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
SLACK_CHANNEL_ID = os.getenv('GST_INVOICE_SLACK_CHANNEL', 'C0B4J9RBWDC')   # TODO: set live channel; env overrides for testing
SHEET_ID   = '1It-QLilNPKev_gYQe9RDFUkE4muSYWxdcqsFZwS58go'   # "GST Invoice" responses
SHEET_GID  = 1327167093                                        # target tab (from the sheet URL)
STATE_TABLE = 'emergent-default.support.gst_invoice_pinged'
SHEET_URL   = 'https://docs.google.com/spreadsheets/d/%s/edit#gid=%s' % (SHEET_ID, SHEET_GID)

# Which columns to surface in the alert (exact header text -> label). Anything else is ignored.
FIELDS = [
    ('Name of Vendor', 'Vendor'),
    ('Nature of Vendor', 'Nature'),
    ('Email (Your registered email on Emergent)', 'Emergent email'),
    ('Email Address', 'Form email'),
    ('Telephone/Mobile No.', 'Phone'),
    ('Whether Registered as Micro/Small/Medium Enterprise (MSME)', 'MSME'),
    ('PAN No.', 'PAN'),
    ('GST Number.', 'GST'),
    ('HSN/SAC Code', 'HSN/SAC'),
    ('Bank Name', 'Bank'),
    ('Account No.', 'A/C No.'),
    ('IFSC Code', 'IFSC'),
    ('Nature of Invoice generation', 'Invoice cadence'),
]
# Document/attachment columns -> rendered as links
DOC_FIELDS = [
    ('Provide the copy of Registration Certificate', 'MSME cert'),
    ('Attach Pan card copy', 'PAN copy'),
    ('Attach GST Certificate copy', 'GST cert'),
    ('Copy of Cancelled Cheque', 'Cancelled cheque'),
    ('Signature Image Upload', 'Signature'),
]

# ==================== STATE TABLE (dedup) ====================
DDL = f"""
CREATE TABLE IF NOT EXISTS `{STATE_TABLE}` (
  row_key STRING,
  ts_raw STRING,
  vendor STRING,
  pinged_at TIMESTAMP
)
"""


# ==================== SHEET READ (ADC, no key) ====================

def _sheet_token():
    creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    creds.refresh(google.auth.transport.requests.Request())
    return creds.token


def read_sheet():
    """Return (header:list, rows:list[list]) for the target tab via the Sheets REST API."""
    token = _sheet_token()
    hdrs = {'Authorization': 'Bearer %s' % token}
    # resolve the tab title from the gid
    meta = requests.get(
        'https://sheets.googleapis.com/v4/spreadsheets/%s?fields=sheets.properties' % SHEET_ID,
        headers=hdrs, timeout=30).json()
    title = None
    for s in meta.get('sheets', []):
        p = s.get('properties', {})
        if p.get('sheetId') == SHEET_GID:
            title = p.get('title')
            break
    if title is None and meta.get('sheets'):
        title = meta['sheets'][0]['properties']['title']  # fallback: first tab
    rng = requests.utils.quote("'%s'" % title, safe='')
    vals = requests.get(
        'https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s' % (SHEET_ID, rng),
        headers=hdrs, timeout=30).json().get('values', [])
    if not vals:
        return [], []
    return vals[0], vals[1:]


# ==================== HELPERS ====================

def _row_key(rowmap):
    raw = '%s|%s|%s' % (rowmap.get('Timestamp', ''),
                        rowmap.get('Email Address', ''),
                        rowmap.get('Name of Vendor', ''))
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()


def build_message(rowmap):
    lines = [':page_facing_up: *New GST / Vendor onboarding submission*']
    ts = rowmap.get('Timestamp', '')
    if ts:
        lines.append('_submitted %s_' % ts)
    detail = []
    for col, label in FIELDS:
        v = (rowmap.get(col) or '').strip()
        if v:
            detail.append('*%s:* %s' % (label, v))
    lines.append('\n'.join(detail))
    docs = []
    for col, label in DOC_FIELDS:
        v = (rowmap.get(col) or '').strip()
        if v:
            docs.append('<%s|%s>' % (v, label))
    if docs:
        lines.append(':paperclip: ' + '  ·  '.join(docs))
    lines.append('<%s|Open sheet>' % SHEET_URL)
    return '\n'.join(lines)


def slack_post(channel, text):
    resp = requests.post(
        'https://slack.com/api/chat.postMessage',
        headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN,
                 'Content-Type': 'application/json; charset=utf-8'},
        json={'channel': channel, 'text': text, 'unfurl_links': False, 'unfurl_media': False},
        timeout=30)
    data = resp.json()
    if not data.get('ok'):
        raise Exception('chat.postMessage failed: %s' % data.get('error'))
    return data['ts']


# ==================== MAIN TASK ====================

def run_gst_invoice(**context):
    logger.info('=' * 60)
    logger.info('GST INVOICE ALERT: READ SHEET & POST')
    logger.info('=' * 60)

    client = get_bigquery_client()
    logger.info('[1] Ensuring state table exists...')
    client.query(DDL).result()

    logger.info('[2] Reading sheet %s ...', SHEET_ID)
    header, rows = read_sheet()
    if not header:
        logger.info('GST INVOICE ALERT: sheet empty / unreadable')
        return
    header = [h.strip() for h in header]   # sheet headers can carry trailing spaces
    # keep only real submissions (Timestamp filled)
    ts_idx = header.index('Timestamp') if 'Timestamp' in header else 0
    submissions = []
    for r in rows:
        r = r + [''] * (len(header) - len(r))   # pad short rows
        if (r[ts_idx] or '').strip():
            submissions.append(dict(zip(header, r)))
    logger.info('      %d submission row(s) in sheet', len(submissions))
    if not submissions:
        return

    # [3] dedup
    already = {row.row_key for row in client.query(
        f"SELECT row_key FROM `{STATE_TABLE}`").result()}
    new = [(s, _row_key(s)) for s in submissions]
    new = [(s, k) for s, k in new if k not in already]
    logger.info('      %d new submission(s) after dedup', len(new))
    if not new:
        return

    pinged, now_iso = [], datetime.now(timezone.utc).isoformat()
    for s, k in new:
        try:
            slack_post(SLACK_CHANNEL_ID, build_message(s))
            pinged.append({'row_key': k, 'ts_raw': s.get('Timestamp', ''),
                           'vendor': s.get('Name of Vendor', ''), 'pinged_at': now_iso})
            logger.info('      alerted vendor=%s ts=%s', s.get('Name of Vendor'), s.get('Timestamp'))
        except Exception as e:
            logger.error('      failed to post for %s: %s', s.get('Name of Vendor'), e)

    if pinged:
        errs = client.insert_rows_json(client.get_table(STATE_TABLE), pinged)
        if errs:
            logger.error('      state-table insert errors: %s', errs)

    logger.info('GST INVOICE ALERT: COMPLETE (%d alerted)', len(pinged))


# ==================== DAG DEFINITION ====================

default_args = {
    'owner': 'cs_team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'gst_invoice_slack',
    default_args=default_args,
    description='Alert Slack for every new GST/vendor onboarding form submission',
    schedule_interval='*/5 * * * *',  # every 5 min, Asia/Kolkata
    catchup=False,
    is_paused_upon_creation=False,
    tags=['slack', 'gst', 'vendor', 'forms', 'cs_team'],
)

PythonOperator(task_id='run_gst_invoice', python_callable=run_gst_invoice, dag=dag)
