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

# ---- MASTER message: only the headline fields (col header -> label) ----
MASTER_FIELDS = [
    ('Name of Vendor', 'Vendor'),
    ('Nature of Vendor', 'Nature of Vendor'),
    ('GST Number.', 'GST No.'),
    ('Nature of Invoice generation', 'Invoice cadence'),
]
# ---- THREAD reply: full detail, grouped into sections (audit-complete). Blank -> shown as "—". ----
DETAIL_GROUPS = [
    ('Vendor & Contact', [
        ('Name of Vendor', 'Vendor'),
        ('Nature of Vendor', 'Nature of Vendor'),
        ('Registered Office Address', 'Registered Office'),
        ('Telephone/Mobile No.', 'Phone'),
        ('Email (Your registered email on Emergent)', 'Emergent email'),
        ('Email Address', 'Form email'),
    ]),
    ('Registration & Tax', [
        ('Whether Registered as Micro/Small/Medium Enterprise (MSME)', 'MSME registered'),
        ('Micro/Small/Medium Enterprise (MSME) Registration no.', 'MSME Reg. no.'),
        ('PAN No.', 'PAN No.'),
        ('GST Number.', 'GST No.'),
        ('HSN/SAC Code', 'HSN/SAC'),
    ]),
    ('Banking', [
        ('Bank Name', 'Bank Name'),
        ('Bank Branch', 'Branch'),
        ('Account No.', 'Account No.'),
        ('IFSC Code', 'IFSC'),
    ]),
    ('Invoicing & Signatory', [
        ('Nature of Invoice generation', 'Invoice cadence'),
        ('Contact Person Name', 'Contact Person'),
        ('Name of the authorized signatory', 'Authorized Signatory'),
        ('Place', 'Place'),
        ('Status', 'Status'),
    ]),
]
# Document/attachment columns -> rendered as link buttons in the thread
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


def _val(rowmap, col):
    return (rowmap.get(col) or '').strip()


def _field(label, value):
    """Header (bold) over its value shown in an inline-code 'box' for clear separation."""
    v = (value or '').strip()
    return {'type': 'mrkdwn', 'text': '*%s*\n%s' % (label, ('`%s`' % v) if v else '`—`')}


def _doc_button_blocks(rowmap):
    """Attached documents as boxed URL buttons (max 5 per actions block). Needs app Interactivity
    enabled to clear Slack's warning triangle; buttons are the only 'boxed' link style Slack offers."""
    btns = [{'type': 'button', 'text': {'type': 'plain_text', 'text': ':page_facing_up: %s' % label, 'emoji': True},
             'url': _val(rowmap, col)}
            for col, label in DOC_FIELDS if _val(rowmap, col).startswith('http')]
    return [{'type': 'actions', 'elements': btns[i:i + 5]} for i in range(0, len(btns), 5)]


def build_master_blocks(rowmap):
    """Headline card (Block Kit) for the master message — key fields + all doc buttons."""
    vendor = _val(rowmap, 'Name of Vendor') or '(no name)'
    fields = [_field(label, _val(rowmap, col)) for col, label in MASTER_FIELDS]
    blocks = [
        {'type': 'header', 'text': {'type': 'plain_text',
                                    'text': ':page_facing_up: New GST / Vendor Onboarding', 'emoji': True}},
        {'type': 'section', 'fields': fields},
        {'type': 'context', 'elements': [{'type': 'mrkdwn',
            'text': ':inbox_tray: submitted *%s*   ·   %s   ·   :thread: full details in thread'
                    % (_val(rowmap, 'Timestamp') or '—', _val(rowmap, 'Email Address') or '—')}]},
        {'type': 'divider'},
    ]
    doc_blocks = _doc_button_blocks(rowmap)
    if doc_blocks:
        blocks.append({'type': 'section', 'text': {'type': 'mrkdwn', 'text': ':paperclip: *Documents*'}})
        blocks += doc_blocks
    blocks.append({'type': 'actions', 'elements': [
        {'type': 'button', 'text': {'type': 'plain_text', 'text': ':page_with_curl: Open sheet', 'emoji': True},
         'url': SHEET_URL, 'style': 'primary'}]})
    text = 'New GST / Vendor Onboarding — %s' % vendor   # notification fallback
    return text, blocks


def build_detail_blocks(rowmap):
    """Full, audit-complete breakdown (Block Kit) for the threaded reply."""
    vendor = _val(rowmap, 'Name of Vendor') or '(no name)'
    blocks = [{'type': 'header', 'text': {'type': 'plain_text',
              'text': ':clipboard: Full submission — %s' % vendor[:140], 'emoji': True}}]
    for title, cols in DETAIL_GROUPS:
        blocks.append({'type': 'section', 'text': {'type': 'mrkdwn', 'text': '*%s*' % title}})
        fields = [_field(label, _val(rowmap, col)) for col, label in cols]
        for i in range(0, len(fields), 10):   # max 10 fields per section
            blocks.append({'type': 'section', 'fields': fields[i:i + 10]})
        blocks.append({'type': 'divider'})
    doc_blocks = _doc_button_blocks(rowmap)
    if doc_blocks:
        blocks.append({'type': 'section', 'text': {'type': 'mrkdwn', 'text': ':paperclip: *Documents*'}})
        blocks += doc_blocks
    text = 'Full submission — %s' % vendor   # notification fallback
    return text, blocks


def slack_post(channel, text, blocks=None, thread_ts=None):
    payload = {'channel': channel, 'text': text, 'unfurl_links': False, 'unfurl_media': False}
    if blocks:
        payload['blocks'] = blocks
    if thread_ts:
        payload['thread_ts'] = thread_ts
    resp = requests.post(
        'https://slack.com/api/chat.postMessage',
        headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN,
                 'Content-Type': 'application/json; charset=utf-8'},
        json=payload, timeout=30)
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
            m_text, m_blocks = build_master_blocks(s)
            ts = slack_post(SLACK_CHANNEL_ID, m_text, blocks=m_blocks)          # master (headline)
            d_text, d_blocks = build_detail_blocks(s)
            slack_post(SLACK_CHANNEL_ID, d_text, blocks=d_blocks, thread_ts=ts)  # full detail in thread
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
