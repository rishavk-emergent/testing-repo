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

CONFIG (editable in Redash, no code push):
  * payments feed  -> #40082  (params: email, as_of_date)  email->user_id->prev-month money-in
  * trigger days + channel -> #40445  (day, channel_id, channel_name)
Channels: onboarding -> GST_INVOICE_SLACK_CHANNEL env (test) else INVOICE default; monthly ->
config #40445 channel (tf-cs-finance-collab), env overrides for testing.
"""

from datetime import datetime, timedelta, timezone
import logging, os, hashlib, time

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
INVOICE_CHANNEL      = os.getenv('GST_INVOICE_SLACK_CHANNEL', 'C0B4J9RBWDC')   # TODO live channel; env overrides
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
CONFIG_QUERY_ID      = 40445           # Redash: "[GST] Monthly config" -> day, channel_id, channel_name
MONTHLY_STATE_TABLE  = 'emergent-default.support.gst_monthly_pinged'
MONTHLY_DDL = f"""
CREATE TABLE IF NOT EXISTS `{MONTHLY_STATE_TABLE}` (
  email STRING, period STRING, vendor STRING, n_payments INT64, pinged_at TIMESTAMP
)
"""
ACCEPTED_STATUS = 'accepted'
RECURRING_MATCH = 'recurring'
COL_STATUS  = 'Status'
COL_CADENCE = 'Nature of Invoice generation'
COL_EMAIL   = 'Email (Your registered email on Emergent)'
COL_VENDOR  = 'Name of Vendor'
COL_GST     = 'GST Number.'


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

    sid, gid, sheet_url = _sheet_loc(redash_run(CONFIG_QUERY_ID, {}))
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
            ts = slack_post(INVOICE_CHANNEL, m_text, blocks=m_blocks)
            d_text, d_blocks = build_detail_blocks(s)
            slack_post(INVOICE_CHANNEL, d_text, blocks=d_blocks, thread_ts=ts)
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


# ==================== DAG 2: MONTHLY PAYMENTS ====================

def _fmt_amt(a, c):
    a = int(round(a or 0))
    if c == 'INR':
        return u'₹%s' % format(a, ',')
    if c == 'USD':
        return '$%s' % format(a, ',')
    return '%s %s' % (format(a, ','), c)


def build_payments_master_blocks(vendor, gst, email, period, payments, sheet_url):
    totals = {}
    for p in payments:
        totals[p['currency']] = totals.get(p['currency'], 0) + (p['amount'] or 0)
    tot = '  ·  '.join(_fmt_amt(v, c) for c, v in totals.items()) or '—'
    return [
        {'type': 'header', 'text': {'type': 'plain_text',
         'text': ':receipt: GST Monthly Payments — %s' % (vendor or '(no name)')[:120], 'emoji': True}},
        {'type': 'section', 'fields': [
            {'type': 'mrkdwn', 'text': '*Vendor*\n`%s`' % (vendor or '—')},
            {'type': 'mrkdwn', 'text': '*Period*\n`%s`' % period},
            {'type': 'mrkdwn', 'text': '*Emergent email*\n`%s`' % (email or '—')},
            {'type': 'mrkdwn', 'text': '*GST No.*\n`%s`' % (gst or '—')},
        ]},
        {'type': 'context', 'elements': [{'type': 'mrkdwn',
         'text': ':moneybag: *%d* payment(s)  ·  total *%s*  ·  :thread: proofs in thread'
                 % (len(payments), tot)}]},
        {'type': 'section', 'text': {'type': 'mrkdwn',
         'text': ':page_with_curl: <%s|Open source sheet (Excel)>' % sheet_url}},
    ]


def build_payments_thread_blocks(period, payments):
    lines = ['*%d.* `%s`  ·  *%s*  ·  %s  ·  %s\n     proof: `%s`'
             % (i, str(p.get('created_at'))[:10], p.get('reference_type'),
                _fmt_amt(p.get('amount'), p.get('currency')), p.get('provider'), p.get('reference_id'))
             for i, p in enumerate(payments, 1)]
    return [
        {'type': 'header', 'text': {'type': 'plain_text', 'text': ':page_facing_up: Payment proofs — %s' % period, 'emoji': True}},
        {'type': 'section', 'text': {'type': 'mrkdwn', 'text': '\n'.join(lines) if lines else '_no payments this period_'}},
        {'type': 'context', 'elements': [{'type': 'mrkdwn',
         'text': 'Verify each proof id against the processor (Stripe/Razorpay) to confirm legitimacy.'}]},
    ]


def run_gst_monthly(**context):
    logger.info('GST MONTHLY PAYMENTS')

    now = pendulum.now('Asia/Kolkata')
    dom, last_dom = now.day, now.end_of('month').day
    cfg = redash_run(CONFIG_QUERY_ID, {}) or []
    days = set()
    for r in cfg:
        try:
            days.add(int(r['day']))
        except Exception:
            pass
    cfg_channel = next((r.get('channel_id') for r in cfg if r.get('channel_id')), None)
    channel = ENV_CHANNEL_OVERRIDE or cfg_channel or FALLBACK_CHANNEL
    fire = (dom in days) or (99 in days and dom == last_dom)
    logger.info('[0] IST day=%d (last=%d), trigger days=%s, channel=%s -> fire=%s',
                dom, last_dom, sorted(days), channel, fire)
    if not fire:
        logger.info('GST MONTHLY: not a trigger day, exiting')
        return

    as_of = now.format('YYYY-MM-DD')
    period = now.subtract(months=1).format('MMM YYYY')
    sid, gid, sheet_url = _sheet_loc(cfg)   # reuse the config rows already fetched above

    client = get_bigquery_client()
    client.query(MONTHLY_DDL).result()

    vendors = [r for r in sheet_rows(sid, gid)
               if (r.get(COL_STATUS, '') or '').strip().lower() == ACCEPTED_STATUS
               and RECURRING_MATCH in (r.get(COL_CADENCE, '') or '').lower()
               and (r.get(COL_EMAIL, '') or '').strip()]
    logger.info('      %d accepted + recurring vendor(s)', len(vendors))
    if not vendors:
        return

    done = {(r.email, r.period) for r in client.query(
        f"SELECT email, period FROM `{MONTHLY_STATE_TABLE}`").result()}

    posted, now_iso = [], datetime.now(timezone.utc).isoformat()
    for v in vendors:
        email = v.get(COL_EMAIL, '').strip()
        if (email.lower(), period) in done:
            logger.info('      skip %s (already posted for %s)', email, period)
            continue
        try:
            payments = redash_run(PAYMENTS_QUERY_ID, {'email': email, 'as_of_date': as_of}) or []
            ts = slack_post(channel, 'GST Monthly Payments — %s' % v.get(COL_VENDOR, email),
                            blocks=build_payments_master_blocks(v.get(COL_VENDOR, ''), v.get(COL_GST, ''),
                                                                email, period, payments, sheet_url))
            slack_post(channel, 'Payment proofs — %s' % period,
                       blocks=build_payments_thread_blocks(period, payments), thread_ts=ts)
            posted.append({'email': email.lower(), 'period': period, 'vendor': v.get(COL_VENDOR, ''),
                           'n_payments': len(payments), 'pinged_at': now_iso})
            logger.info('      posted %s (%d payments)', email, len(payments))
        except Exception as e:
            logger.error('      failed for %s: %s', email, e)

    if posted:
        errs = client.insert_rows_json(client.get_table(MONTHLY_STATE_TABLE), posted)
        if errs:
            logger.error('      state-table insert errors: %s', errs)
    logger.info('GST MONTHLY: COMPLETE (%d vendor(s) posted)', len(posted))


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
    is_paused_upon_creation=False,
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
