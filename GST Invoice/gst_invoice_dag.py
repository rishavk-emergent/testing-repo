"""
GST Invoice - Slack DAGs (two DAGs in one file)

1) gst_invoice_slack  (hourly, IST)
   Watches the "GST Invoice" Google Form response sheet and posts a Slack alert for every NEW
   vendor submission (Timestamp filled, not seen before): a master card + full audit detail in
   the thread. Dedup: support.gst_invoice_pinged.

2) gst_monthly_payments_slack  (daily tick, fires on config trigger-day(s), IST)
   For every sheet vendor that is Status=Accepted AND Nature of Invoice generation contains
   "Recurring", on the configured day(s) of the month posts a master (Vendor/Period/Emergent
   email/GST No.) + the previous calendar month's real money-in payments with proof in the thread.
   Dedup: support.gst_monthly_pinged (email+period).

SHEET READ (no key): Composer workers run AS a Google service account (ADC); we read the sheet
via the Sheets REST API + google.auth.default(). PREREQUISITE: share the sheet (Viewer) with the
Composer runtime SA. Sidesteps the org policy that blocks service-account key downloads.

CONFIG (all editable in Redash, no code push):
  * config #40445  -> ONE row per trigger day carrying EVERY monthly-DAG knob:
      day, channel_id/name, sheet_id/gid, col_status/col_cadence/col_email/col_vendor/col_gst,
      accepted_status, recurring_match, default_since, payments_query_id,
      excel_headers/excel_fields/amount_field, master_text, thread_comment.
    The DAG constants (COL_*, ACCEPTED_STATUS, DEFAULT_* ...) are FALLBACKS only.
  * payments query #46587 -> SOURCE OF TRUTH: payment_transactions (Postgres, status='SUCCEEDED');
    params email, since_date, as_of_date -> date/payment_id/order_id/amount/currency. Swap the
    source by pointing payments_query_id at a different query — no code push.
Channel: BOTH DAGs post to config #40445 channel_id (tf-cs-finance-collab, C0B9Y89RSL9);
GST_INVOICE_SLACK_CHANNEL env overrides both for testing. Both ship PAUSED (sensitive channel).
"""

from datetime import datetime, timedelta, timezone
import logging, os, hashlib, time, json, io, zipfile
from xml.sax.saxutils import escape

import pendulum
import requests
import google.auth
import google.auth.transport.requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack.slack_config import (
    REDASH_API_KEY, REDASH_BASE_URL,
    SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN,
)
from utils.slack.bigquery_client import get_bigquery_client

logger = logging.getLogger(__name__)

# ==================== SHARED CONFIG ====================
# Fallbacks only — the live sheet location is read from Redash config #CONFIG_QUERY_ID (sheet_id/gid),
# so moving the sheet is a query edit, no code change.
SHEET_ID  = '1It-QLilNPKev_gYQe9RDFUkE4muSYWxdcqsFZwS58go'   # "GST Invoice" responses (fallback)
SHEET_GID = 1327167093                                        # target tab (fallback)


def _sheet_url(sid, gid):
    return 'https://docs.google.com/spreadsheets/d/%s/edit#gid=%s' % (sid, gid)


def _sheet_loc(cfg_rows):
    """(sheet_id, sheet_gid, sheet_url) from config rows, falling back to the constants above."""
    sid = next((r.get('sheet_id') for r in (cfg_rows or []) if r.get('sheet_id')), None) or SHEET_ID
    gid = next((r.get('sheet_gid') for r in (cfg_rows or []) if r.get('sheet_gid') is not None), None)
    gid = int(gid) if gid is not None else SHEET_GID
    return sid, gid, _sheet_url(sid, gid)

# ---- onboarding-alert config ----
# Channel comes from config #40445 (same as monthly -> tf-cs-finance-collab); env overrides for testing.
INVOICE_STATE_TABLE  = 'emergent-default.support.gst_invoice_pinged'
INVOICE_DDL = f"""
CREATE TABLE IF NOT EXISTS `{INVOICE_STATE_TABLE}` (
  row_key STRING, ts_raw STRING, vendor STRING, pinged_at TIMESTAMP
)
"""
MASTER_FIELDS = [
    ('Name of Vendor', 'Vendor'),
    ('Nature of Vendor', 'Nature of Vendor'),
    ('GST Number.', 'GST No.'),
    ('Nature of Invoice generation', 'Invoice cadence'),
]
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
DOC_FIELDS = [
    ('Provide the copy of Registration Certificate', 'MSME cert'),
    ('Attach Pan card copy', 'PAN copy'),
    ('Attach GST Certificate copy', 'GST cert'),
    ('Copy of Cancelled Cheque', 'Cancelled cheque'),
    ('Signature Image Upload', 'Signature'),
]

# ---- monthly-payments config ----
ENV_CHANNEL_OVERRIDE = os.getenv('GST_INVOICE_SLACK_CHANNEL')   # test channel for dry runs; unset in prod
FALLBACK_CHANNEL     = 'C0B9Y89RSL9'   # tf-cs-finance-collab (used only if config row blank)
PAYMENTS_QUERY_ID    = 40082           # Redash: "[GST] Vendor monthly payments feed"
PAYMENTS_RANGE_QUERY_ID = 46587        # Redash: "[GST] Vendor payments (range, ids)" (since,as_of] -> payment_id/order_id/date/amount
MONTHLY_STATE_VAR    = 'GST_MONTHLY_STATE'   # Airflow Variable: {email: {last_trigger_at, nonrec_fired}}
DEFAULT_SINCE        = '2020-01-01'          # first-ever trigger window lower bound (all history)
CONFIG_QUERY_ID      = 40445           # Redash: "[GST] Monthly config" -> day, channel_id, channel_name
MONTHLY_STATE_TABLE  = 'emergent-default.support.gst_monthly_pinged'
MONTHLY_DDL = f"""
CREATE TABLE IF NOT EXISTS `{MONTHLY_STATE_TABLE}` (
  email STRING, period STRING, vendor STRING, n_payments INT64, pinged_at TIMESTAMP
)
"""
# --- ALL of the below are FALLBACKS ONLY ---
# At runtime run_gst_monthly reads these from config #CONFIG_QUERY_ID (accepted_status, recurring_match,
# col_status/col_cadence/col_email/col_vendor/col_gst, default_since, payments_query_id, excel_headers,
# excel_fields, amount_field, master_text, thread_comment). They are used only if the config row is
# blank/broken, so editing the Redash config needs no code push.
ACCEPTED_STATUS = 'accepted'
RECURRING_MATCH = 'recurring'
COL_STATUS  = 'Status'
COL_CADENCE = 'Nature of Invoice generation'
COL_EMAIL   = 'Email (Your registered email on Emergent)'
COL_VENDOR  = 'Name of Vendor'
COL_GST     = 'GST Number.'
DEFAULT_MASTER_TEXT    = (":receipt: *GST Monthly Payments — {as_of}*  ·  *{n}* vendor(s)\n"
                          "Please find each vendor's list of payments (Excel) in the thread below.")
DEFAULT_THREAD_COMMENT = ':receipt: *{email}*  ·  {n} payment(s)  ·  {since} → {as_of}'
DEFAULT_EXCEL_HEADERS  = ['Date', 'Payment ID', 'Order ID', 'Amount', 'Currency']
DEFAULT_EXCEL_FIELDS   = ['date', 'payment_id', 'order_id', 'amount', 'currency']
DEFAULT_AMOUNT_FIELD   = 'amount'


# ==================== SHARED HELPERS ====================

def sheet_rows(sheet_id=SHEET_ID, sheet_gid=SHEET_GID):
    """Return the target tab as a list of header-mapped dicts (headers stripped), via Sheets ADC."""
    creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    creds.refresh(google.auth.transport.requests.Request())
    hdrs = {'Authorization': 'Bearer %s' % creds.token}
    meta = requests.get('https://sheets.googleapis.com/v4/spreadsheets/%s?fields=sheets.properties' % sheet_id,
                        headers=hdrs, timeout=30).json()
    title = next((s['properties']['title'] for s in meta.get('sheets', [])
                  if s['properties'].get('sheetId') == sheet_gid),
                 meta['sheets'][0]['properties']['title'] if meta.get('sheets') else None)
    rng = requests.utils.quote("'%s'" % title, safe='')
    vals = requests.get('https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s' % (sheet_id, rng),
                        headers=hdrs, timeout=30).json().get('values', [])
    if not vals:
        return []
    header = [h.strip() for h in vals[0]]
    return [dict(zip(header, r + [''] * (len(header) - len(r)))) for r in vals[1:]]


def slack_post(channel, text, blocks=None, thread_ts=None):
    p = {'channel': channel, 'text': text, 'unfurl_links': False, 'unfurl_media': False}
    if blocks:
        p['blocks'] = blocks
    if thread_ts:
        p['thread_ts'] = thread_ts
    d = requests.post('https://slack.com/api/chat.postMessage',
                      headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN,
                               'Content-Type': 'application/json; charset=utf-8'},
                      json=p, timeout=30).json()
    if not d.get('ok'):
        raise Exception('chat.postMessage failed: %s' % d.get('error'))
    return d['ts']


def redash_run(query_id, parameters, max_wait=90):
    """Run a Redash query with parameters, return result rows (list of dict)."""
    h = {'Authorization': 'Key %s' % REDASH_API_KEY, 'Content-Type': 'application/json'}
    job = requests.post('%s/api/queries/%s/results' % (REDASH_BASE_URL, query_id),
                        json={'parameters': parameters or {}, 'max_age': 0}, headers=h, timeout=60).json()
    if 'query_result' in job:
        return job['query_result']['data']['rows']
    jid = job['job']['id']
    for _ in range(max_wait):
        jr = requests.get('%s/api/jobs/%s' % (REDASH_BASE_URL, jid), headers=h, timeout=30).json()['job']
        if jr['status'] in (3, 4):
            if jr['status'] == 4:
                raise Exception('Redash query %s failed: %s' % (query_id, jr.get('error')))
            rid = jr['query_result_id']
            return requests.get('%s/api/query_results/%s.json' % (REDASH_BASE_URL, rid),
                                headers=h, timeout=30).json()['query_result']['data']['rows']
        time.sleep(2)
    raise Exception('Redash query %s timed out' % query_id)


def _val(rowmap, col):
    return (rowmap.get(col) or '').strip()


def _cfg_val(cfg, key, default=None):
    """First non-empty value of `key` across config rows (config #CONFIG_QUERY_ID), else default."""
    return next((r.get(key) for r in (cfg or []) if r.get(key) not in (None, '')), default)


def _cfg_list(cfg, key, default):
    """Config value split on commas into a trimmed list, else the default list."""
    v = _cfg_val(cfg, key, None)
    return [x.strip() for x in str(v).split(',') if x.strip()] if v else list(default)


# ==================== DAG 1: ONBOARDING ALERT ====================

def _row_key(rowmap):
    raw = '%s|%s|%s' % (rowmap.get('Timestamp', ''), rowmap.get('Email Address', ''),
                        rowmap.get('Name of Vendor', ''))
    return hashlib.sha1(raw.encode('utf-8')).hexdigest()


def _field(label, value):
    v = (value or '').strip()
    return {'type': 'mrkdwn', 'text': '*%s*\n%s' % (label, ('`%s`' % v) if v else '`—`')}


def _doc_button_blocks(rowmap):
    btns = [{'type': 'button', 'text': {'type': 'plain_text', 'text': ':page_facing_up: %s' % label, 'emoji': True},
             'url': _val(rowmap, col)}
            for col, label in DOC_FIELDS if _val(rowmap, col).startswith('http')]
    return [{'type': 'actions', 'elements': btns[i:i + 5]} for i in range(0, len(btns), 5)]


def build_master_blocks(rowmap, sheet_url):
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
         'url': sheet_url, 'style': 'primary'}]})
    return 'New GST / Vendor Onboarding — %s' % vendor, blocks


def build_detail_blocks(rowmap):
    vendor = _val(rowmap, 'Name of Vendor') or '(no name)'
    blocks = [{'type': 'header', 'text': {'type': 'plain_text',
              'text': ':clipboard: Full submission — %s' % vendor[:140], 'emoji': True}}]
    for title, cols in DETAIL_GROUPS:
        blocks.append({'type': 'section', 'text': {'type': 'mrkdwn', 'text': '*%s*' % title}})
        fields = [_field(label, _val(rowmap, col)) for col, label in cols]
        for i in range(0, len(fields), 10):
            blocks.append({'type': 'section', 'fields': fields[i:i + 10]})
        blocks.append({'type': 'divider'})
    doc_blocks = _doc_button_blocks(rowmap)
    if doc_blocks:
        blocks.append({'type': 'section', 'text': {'type': 'mrkdwn', 'text': ':paperclip: *Documents*'}})
        blocks += doc_blocks
    return 'Full submission — %s' % vendor, blocks


def run_gst_invoice(**context):
    logger.info('GST INVOICE ALERT: READ SHEET & POST')
    client = get_bigquery_client()
    client.query(INVOICE_DDL).result()

    cfg = redash_run(CONFIG_QUERY_ID, {})
    sid, gid, sheet_url = _sheet_loc(cfg)
    channel = ENV_CHANNEL_OVERRIDE or next((r.get('channel_id') for r in (cfg or []) if r.get('channel_id')), None) or FALLBACK_CHANNEL
    rows = sheet_rows(sid, gid)
    submissions = [r for r in rows if (r.get('Timestamp') or '').strip()]
    logger.info('      %d submission row(s) in sheet', len(submissions))
    if not submissions:
        return

    already = {row.row_key for row in client.query(f"SELECT row_key FROM `{INVOICE_STATE_TABLE}`").result()}
    new = [(s, _row_key(s)) for s in submissions]
    new = [(s, k) for s, k in new if k not in already]
    logger.info('      %d new submission(s) after dedup', len(new))
    if not new:
        return

    pinged, now_iso = [], datetime.now(timezone.utc).isoformat()
    for s, k in new:
        try:
            m_text, m_blocks = build_master_blocks(s, sheet_url)
            ts = slack_post(channel, m_text, blocks=m_blocks)
            d_text, d_blocks = build_detail_blocks(s)
            slack_post(channel, d_text, blocks=d_blocks, thread_ts=ts)
            pinged.append({'row_key': k, 'ts_raw': s.get('Timestamp', ''),
                           'vendor': s.get('Name of Vendor', ''), 'pinged_at': now_iso})
            logger.info('      alerted vendor=%s', s.get('Name of Vendor'))
        except Exception as e:
            logger.error('      failed to post for %s: %s', s.get('Name of Vendor'), e)

    if pinged:
        errs = client.insert_rows_json(client.get_table(INVOICE_STATE_TABLE), pinged)
        if errs:
            logger.error('      state-table insert errors: %s', errs)
    logger.info('GST INVOICE ALERT: COMPLETE (%d alerted)', len(pinged))


# ==================== DAG 2: MONTHLY GST PAYMENT EXCELS ====================
# For each ACCEPTED vendor on a trigger day, upload ONE .xlsx named <email>.xlsx listing that vendor's
# payments since their last trigger (Payment ID / Order ID / Date / Amount + total).
#   - recurring   : fires every trigger; resets the vendor's non-recurring "once" flag.
#   - non-recurring: fires ONCE per non-recurring stint (flag set on fire; cleared whenever recurring).
# State (per vendor) lives in an Airflow Variable so a mode flip resumes/one-shots correctly.

def _fmt_amt(a, c):
    a = int(round(a or 0))
    if c == 'INR':
        return u'₹%s' % format(a, ',')
    if c == 'USD':
        return '$%s' % format(a, ',')
    return '%s %s' % (format(a, ','), c or '')


def _colref(c0):
    s, c = '', c0 + 1
    while c:
        c, r = divmod(c - 1, 26)
        s = chr(65 + r) + s
    return s


def build_payments_xlsx(rows_2d, path, amount_col=3):
    """Single-sheet .xlsx (stdlib, no deps): bold header band, auto-fit column widths, #,##0 amounts."""
    n_cols = max((len(r) for r in rows_2d), default=1)
    widths = []
    for c in range(n_cols):
        w = max((len(str(r[c])) for r in rows_2d if c < len(r) and r[c] not in ('', None)), default=8)
        widths.append(min(max(w + 3, 11), 60))
    cols_xml = '<cols>%s</cols>' % ''.join(
        '<col min="%d" max="%d" width="%.2f" customWidth="1"/>' % (c + 1, c + 1, widths[c]) for c in range(n_cols))

    def cell(r, c, v, header):
        ref = _colref(c) + str(r)
        if header:
            return '<c r="%s" s="1" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>' % (ref, escape(str(v)))
        if isinstance(v, bool):
            v = str(v)
        if isinstance(v, (int, float)):
            style = ' s="2"' if c == amount_col else ''
            return '<c r="%s"%s><v>%s</v></c>' % (ref, style, v)
        return '<c r="%s" t="inlineStr"><is><t xml:space="preserve">%s</t></is></c>' % (ref, escape(str(v)))

    xml_rows = ''.join('<row r="%d">%s</row>' % (i, ''.join(cell(i, ci, v, i == 1) for ci, v in enumerate(row)))
                       for i, row in enumerate(rows_2d, 1))
    sheet = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
             '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
             '%s<sheetData>%s</sheetData></worksheet>' % (cols_xml, xml_rows))
    styles = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
              '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
              '<numFmts count="1"><numFmt numFmtId="164" formatCode="#,##0"/></numFmts>'
              '<fonts count="2">'
              '<font><sz val="11"/><name val="Calibri"/></font>'
              '<font><b/><sz val="11"/><color rgb="FFFFFFFF"/><name val="Calibri"/></font></fonts>'
              '<fills count="3">'
              '<fill><patternFill patternType="none"/></fill>'
              '<fill><patternFill patternType="gray125"/></fill>'
              '<fill><patternFill patternType="solid"><fgColor rgb="FF2E5A88"/></patternFill></fill></fills>'
              '<borders count="1"><border/></borders>'
              '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
              '<cellXfs count="3">'
              '<xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
              '<xf numFmtId="0" fontId="1" fillId="2" borderId="0" xfId="0" applyFont="1" applyFill="1"><alignment vertical="center"/></xf>'
              '<xf numFmtId="164" fontId="0" fillId="0" borderId="0" xfId="0" applyNumberFormat="1"/>'
              '</cellXfs><cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles></styleSheet>')
    ctypes = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
              '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
              '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
              '<Default Extension="xml" ContentType="application/xml"/>'
              '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
              '<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
              '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>')
    rrels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
             '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
             '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>')
    wb = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
          '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
          'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
          '<sheets><sheet name="Payments" sheetId="1" r:id="rId1"/></sheets></workbook>')
    wbrels = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
              '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
              '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>'
              '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/></Relationships>')
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('[Content_Types].xml', ctypes)
        z.writestr('_rels/.rels', rrels)
        z.writestr('xl/workbook.xml', wb)
        z.writestr('xl/_rels/workbook.xml.rels', wbrels)
        z.writestr('xl/styles.xml', styles)
        z.writestr('xl/worksheets/sheet1.xml', sheet)
    return path


def slack_upload_file(channel, path, title, comment, thread_ts=None):
    ln = os.path.getsize(path)
    g = requests.get('https://slack.com/api/files.getUploadURLExternal',
                     params={'filename': title, 'length': ln},
                     headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN}, timeout=30).json()
    if not g.get('ok'):
        raise Exception('getUploadURL: %s' % g.get('error'))
    requests.post(g['upload_url'], data=open(path, 'rb').read(),
                  headers={'Content-Type': 'application/octet-stream'}, timeout=60)
    data = {'channel_id': channel, 'initial_comment': comment,
            'files': json.dumps([{'id': g['file_id'], 'title': title}])}
    if thread_ts:
        data['thread_ts'] = thread_ts
    c = requests.post('https://slack.com/api/files.completeUploadExternal',
                      headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN},
                      data=data, timeout=30).json()
    if not c.get('ok'):
        raise Exception('completeUpload: %s' % c.get('error'))


def run_gst_monthly(**context):
    from airflow.models import Variable
    logger.info('GST MONTHLY PAYMENT EXCELS')

    now = pendulum.now('Asia/Kolkata')
    dom, last_dom = now.day, now.end_of('month').day
    cfg = redash_run(CONFIG_QUERY_ID, {}) or []
    days = set()
    for r in cfg:
        try:
            days.add(int(r['day']))
        except Exception:
            pass
    channel = ENV_CHANNEL_OVERRIDE or next((r.get('channel_id') for r in cfg if r.get('channel_id')), None) or FALLBACK_CHANNEL
    fire = (dom in days) or (99 in days and dom == last_dom)
    logger.info('[0] IST day=%d (last=%d), trigger days=%s, channel=%s -> fire=%s', dom, last_dom, sorted(days), channel, fire)
    if not fire:
        logger.info('GST MONTHLY: not a trigger day, exiting')
        return

    as_of = now.format('YYYY-MM-DD')
    sid, gid, sheet_url = _sheet_loc(cfg)

    # ---- every editable knob comes from config #CONFIG_QUERY_ID (fallback constants if blank) ----
    col_status   = _cfg_val(cfg, 'col_status',  COL_STATUS)
    col_email    = _cfg_val(cfg, 'col_email',   COL_EMAIL)
    col_cadence  = _cfg_val(cfg, 'col_cadence', COL_CADENCE)
    accepted     = str(_cfg_val(cfg, 'accepted_status', ACCEPTED_STATUS)).strip().lower()
    recurring_kw = str(_cfg_val(cfg, 'recurring_match', RECURRING_MATCH)).strip().lower()
    default_since = _cfg_val(cfg, 'default_since', DEFAULT_SINCE)
    payments_qid = int(_cfg_val(cfg, 'payments_query_id', PAYMENTS_RANGE_QUERY_ID))
    headers      = _cfg_list(cfg, 'excel_headers', DEFAULT_EXCEL_HEADERS)
    fields       = _cfg_list(cfg, 'excel_fields',  DEFAULT_EXCEL_FIELDS)
    amount_field = _cfg_val(cfg, 'amount_field',  DEFAULT_AMOUNT_FIELD)
    master_tmpl  = _cfg_val(cfg, 'master_text',    DEFAULT_MASTER_TEXT)
    comment_tmpl = _cfg_val(cfg, 'thread_comment', DEFAULT_THREAD_COMMENT)
    if len(fields) != len(headers):   # mismatched config lists -> fall back to defaults
        logger.warning('      excel_headers/excel_fields length mismatch; using defaults')
        headers, fields = list(DEFAULT_EXCEL_HEADERS), list(DEFAULT_EXCEL_FIELDS)
    amt_idx = fields.index(amount_field) if amount_field in fields else (len(fields) - 2 if len(fields) > 1 else 0)

    # accepted vendors with an email; dedup by email (first accepted row wins) so a vendor with
    # duplicate sheet rows still gets exactly ONE Excel.
    vendors, seen = [], set()
    for r in sheet_rows(sid, gid):
        if (r.get(col_status, '') or '').strip().lower() != accepted:
            continue
        em = (r.get(col_email, '') or '').strip().lower()
        if not em or em in seen:
            continue
        seen.add(em)
        vendors.append(r)
    logger.info('      %d accepted vendor(s) after email dedup', len(vendors))

    try:
        state = json.loads(Variable.get(MONTHLY_STATE_VAR))
        if not isinstance(state, dict):
            state = {}
    except Exception:
        state = {}

    # ---- pass 1: build a workbook for every eligible vendor ----
    jobs = []
    for v in vendors:
        email = v.get(col_email, '').strip()
        key = email.lower()
        recurring = recurring_kw in (v.get(col_cadence, '') or '').lower()
        vs = state.get(key, {})
        fire_v = True if recurring else (not bool(vs.get('nonrec_fired')))
        if not fire_v:
            logger.info('      skip %s (non-recurring, already fired once)', email)
            continue
        since = vs.get('last_trigger_at') or default_since
        try:
            pays = redash_run(payments_qid, {'email': email, 'since_date': since, 'as_of_date': as_of}) or []
        except Exception as e:
            logger.error('      payments fetch failed for %s: %s', email, e)
            continue
        if not pays:
            # nothing in this window -> don't post an empty file; leave state untouched so the
            # vendor stays eligible (recurring re-fires next trigger; non-recurring hasn't fired yet).
            logger.info('      skip %s (0 payments in window %s -> %s)', email, since, as_of)
            continue
        # Excel columns are config-driven: header headers[i] shows payment field fields[i].
        rows2d = [list(headers)]
        for p in pays:
            rows2d.append([(p.get(f) or 0) if f == amount_field
                           else (str(p.get(f)) if p.get(f) not in (None, '') else '-')
                           for f in fields])
        totals = {}
        for p in pays:
            cur = p.get('currency') or ''
            totals[cur] = totals.get(cur, 0) + (p.get(amount_field) or 0)
        rows2d.append([''] * len(headers))
        for cur, tot in totals.items():
            tr = [''] * len(headers)
            tr[amt_idx] = tot
            tr[amt_idx - 1 if amt_idx > 0 else 0] = 'TOTAL'
            if 'currency' in fields:
                tr[fields.index('currency')] = cur
            rows2d.append(tr)
        path = '/tmp/gst_%s.xlsx' % hashlib.md5(key.encode()).hexdigest()
        build_payments_xlsx(rows2d, path, amount_col=amt_idx)
        try:
            comment = comment_tmpl.format(email=email, n=len(pays), since=since, as_of=as_of)
        except Exception:
            comment = DEFAULT_THREAD_COMMENT.format(email=email, n=len(pays), since=since, as_of=as_of)
        jobs.append({'key': key, 'recurring': recurring, 'path': path, 'title': '%s.xlsx' % email,
                     'comment': comment, 'n': len(pays)})

    if not jobs:
        Variable.set(MONTHLY_STATE_VAR, json.dumps(state))
        logger.info('GST MONTHLY: no eligible vendors this trigger')
        return

    # ---- master message (config template), then one Excel per vendor threaded beneath it ----
    try:
        master = master_tmpl.format(as_of=as_of, n=len(jobs))
    except Exception:
        master = DEFAULT_MASTER_TEXT.format(as_of=as_of, n=len(jobs))
    master_ts = slack_post(channel, master)

    posted = 0
    for j in jobs:
        try:
            slack_upload_file(channel, j['path'], j['title'], j['comment'], thread_ts=master_ts)
        except Exception as e:
            logger.error('      upload failed for %s: %s', j['title'], e)
            continue  # leave state untouched -> retried next trigger
        state[j['key']] = {'last_trigger_at': as_of, 'nonrec_fired': (False if j['recurring'] else True)}
        posted += 1
        logger.info('      threaded %s (%d payments)', j['title'], j['n'])

    Variable.set(MONTHLY_STATE_VAR, json.dumps(state))
    logger.info('GST MONTHLY: COMPLETE (master + %d excel(s) in thread)', posted)


# ==================== DAG DEFINITIONS ====================

default_args = {
    'owner': 'cs_team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag_invoice = DAG(
    'gst_invoice_slack',
    default_args=default_args,
    description='Alert Slack for every new GST/vendor onboarding form submission',
    schedule_interval='0 * * * *',   # hourly, Asia/Kolkata
    catchup=False,
    is_paused_upon_creation=True,    # posts to sensitive tf-cs-finance-collab; unpause after validation
    tags=['slack', 'gst', 'vendor', 'forms', 'cs_team'],
)
PythonOperator(task_id='run_gst_invoice', python_callable=run_gst_invoice, dag=dag_invoice)

dag_monthly = DAG(
    'gst_monthly_payments_slack',
    default_args=default_args,
    description='Monthly GST payments + proofs per accepted+recurring vendor (config-driven trigger day)',
    schedule_interval='0 9 * * *',
    catchup=False,
    is_paused_upon_creation=True,   # posts to sensitive tf-cs-finance-collab; unpause after validation
    tags=['slack', 'gst', 'vendor', 'payments', 'cs_team'],
)
PythonOperator(task_id='run_gst_monthly', python_callable=run_gst_monthly, dag=dag_monthly)
