"""
GST Monthly Payments - Slack DAG (runs daily, fires on configured day(s), IST)

For every vendor in the GST Invoice sheet that is Status = Accepted AND
Nature of Invoice generation = Recurring / Monthly, on the configured trigger day(s) of the
month this posts ONE master message per vendor (Vendor / Period / Emergent email / GST No.)
and, in its thread, every real money-in payment that account made in the PREVIOUS calendar
month, with proof (payment reference id).

TRIGGER DAY(S) ARE CONFIG-DRIVEN (no code push to change):
  * The DAG runs DAILY and reads the allowed calendar days from Redash #TRIGGER_DAYS_QUERY_ID
    (e.g. [1], or [1,15]; 99 = last day of month). It only proceeds when today (IST) matches.

DATA (all editable in Redash, nothing inline):
  * qualifying payments -> Redash #PAYMENTS_QUERY_ID  (params: email, as_of_date)
      email -> user_id via analytics.signups_raw_dataset -> credit_ledger money-in (prev month)
  * vendors           -> the GST Invoice Google Sheet (Sheets API via Composer ADC; share Viewer)
  * dedup             -> support.gst_monthly_pinged (email + period)

Schedule: '0 9 * * *' Asia/Kolkata (daily; gated on the trigger-day config).
Channel:  <set GST_INVOICE_SLACK_CHANNEL>; env overrides for testing.
"""

from datetime import datetime, timedelta, timezone
import logging, os

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

# ==================== CONFIG ====================
# Destination channel + trigger days come from Redash #CONFIG_QUERY_ID (edit there, no code push).
# LIVE channel = tf-cs-finance-collab (C0B9Y89RSL9) — sensitive; DAG ships PAUSED and testing uses the
# GST_INVOICE_SLACK_CHANNEL env override (test channel) until validated.
ENV_CHANNEL_OVERRIDE  = os.getenv('GST_INVOICE_SLACK_CHANNEL')   # set to a test channel for dry runs; unset in prod
FALLBACK_CHANNEL      = 'C0B9Y89RSL9'   # tf-cs-finance-collab (used only if config row is blank)
SHEET_ID              = '1It-QLilNPKev_gYQe9RDFUkE4muSYWxdcqsFZwS58go'   # "GST Invoice" responses
SHEET_GID             = 1327167093
SHEET_URL             = 'https://docs.google.com/spreadsheets/d/%s/edit#gid=%s' % (SHEET_ID, SHEET_GID)
PAYMENTS_QUERY_ID     = 40082    # Redash: "[GST] Vendor monthly payments feed" (params: email, as_of_date)
CONFIG_QUERY_ID       = 40445    # Redash: "[GST] Monthly config" -> day, channel_id, channel_name
STATE_TABLE           = 'emergent-default.support.gst_monthly_pinged'

# vendor qualifies when (case-insensitive):
ACCEPTED_STATUS   = 'accepted'
RECURRING_MATCH   = 'recurring'   # substring of "Recurring / Monthly"
COL_STATUS        = 'Status'
COL_CADENCE       = 'Nature of Invoice generation'
COL_EMAIL         = 'Email (Your registered email on Emergent)'
COL_VENDOR        = 'Name of Vendor'
COL_GST           = 'GST Number.'

DDL = f"""
CREATE TABLE IF NOT EXISTS `{STATE_TABLE}` (
  email STRING, period STRING, vendor STRING, n_payments INT64, pinged_at TIMESTAMP
)
"""


# ==================== SHEET READ (ADC, no key) ====================

def _sheet_rows():
    creds, _ = google.auth.default(scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    creds.refresh(google.auth.transport.requests.Request())
    hdrs = {'Authorization': 'Bearer %s' % creds.token}
    meta = requests.get('https://sheets.googleapis.com/v4/spreadsheets/%s?fields=sheets.properties' % SHEET_ID,
                        headers=hdrs, timeout=30).json()
    title = next((s['properties']['title'] for s in meta.get('sheets', [])
                  if s['properties'].get('sheetId') == SHEET_GID),
                 meta['sheets'][0]['properties']['title'] if meta.get('sheets') else None)
    rng = requests.utils.quote("'%s'" % title, safe='')
    vals = requests.get('https://sheets.googleapis.com/v4/spreadsheets/%s/values/%s' % (SHEET_ID, rng),
                        headers=hdrs, timeout=30).json().get('values', [])
    if not vals:
        return []
    header = [h.strip() for h in vals[0]]
    out = []
    for r in vals[1:]:
        r = r + [''] * (len(header) - len(r))
        out.append(dict(zip(header, r)))
    return out


# ==================== REDASH (parameterized) ====================

def redash_run(query_id, parameters, max_wait=90):
    """Run a Redash query with parameters and return result rows (list of dict)."""
    h = {'Authorization': 'Key %s' % REDASH_API_KEY, 'Content-Type': 'application/json'}
    job = requests.post('%s/api/queries/%s/results' % (REDASH_BASE_URL, query_id),
                        json={'parameters': parameters or {}, 'max_age': 0}, headers=h, timeout=60).json()
    if 'query_result' in job:  # cached
        return job['query_result']['data']['rows']
    jid = job['job']['id']
    import time
    for _ in range(max_wait):
        jr = requests.get('%s/api/jobs/%s' % (REDASH_BASE_URL, jid), headers=h, timeout=30).json()['job']
        if jr['status'] in (3, 4):
            if jr['status'] == 4:
                raise Exception('Redash query %s failed: %s' % (query_id, jr.get('error')))
            rid = jr['query_result_id']
            res = requests.get('%s/api/query_results/%s.json' % (REDASH_BASE_URL, rid), headers=h, timeout=30).json()
            return res['query_result']['data']['rows']
        time.sleep(2)
    raise Exception('Redash query %s timed out' % query_id)


# ==================== SLACK ====================

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


def _fmt_amt(a, c):
    a = int(round(a or 0))
    if c == 'INR':
        return u'₹%s' % format(a, ',')
    if c == 'USD':
        return '$%s' % format(a, ',')
    return '%s %s' % (format(a, ','), c)


def build_master_blocks(vendor, gst, email, period, payments):
    totals = {}
    for p in payments:
        totals[p['currency']] = totals.get(p['currency'], 0) + (p['amount'] or 0)
    tot = '  ·  '.join(_fmt_amt(v, c) for c, v in totals.items()) or '—'
    blocks = [
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
         'text': ':page_with_curl: <%s|Open source sheet (Excel)>' % SHEET_URL}},
    ]
    return blocks


def build_thread_blocks(period, payments):
    lines = []
    for i, p in enumerate(payments, 1):
        lines.append('*%d.* `%s`  ·  *%s*  ·  %s  ·  %s\n     proof: `%s`'
                     % (i, str(p.get('created_at'))[:10], p.get('reference_type'),
                        _fmt_amt(p.get('amount'), p.get('currency')), p.get('provider'), p.get('reference_id')))
    return [
        {'type': 'header', 'text': {'type': 'plain_text', 'text': ':page_facing_up: Payment proofs — %s' % period, 'emoji': True}},
        {'type': 'section', 'text': {'type': 'mrkdwn', 'text': '\n'.join(lines) if lines else '_no payments this period_'}},
        {'type': 'context', 'elements': [{'type': 'mrkdwn',
         'text': 'Verify each proof id against the processor (Stripe/Razorpay) to confirm legitimacy.'}]},
    ]


# ==================== MAIN TASK ====================

def run_gst_monthly(**context):
    logger.info('=' * 60)
    logger.info('GST MONTHLY PAYMENTS')
    logger.info('=' * 60)

    # [0] read config (trigger days + destination channel) and gate on the day
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
    logger.info('[0] today IST day=%d (last=%d), trigger days=%s, channel=%s -> fire=%s',
                dom, last_dom, sorted(days), channel, fire)
    if not fire:
        logger.info('GST MONTHLY: not a trigger day, exiting')
        return

    as_of = now.format('YYYY-MM-DD')
    period = now.subtract(months=1).format('MMM YYYY')

    client = get_bigquery_client()
    logger.info('[1] Ensuring state table...')
    client.query(DDL).result()

    logger.info('[2] Reading vendor sheet...')
    rows = _sheet_rows()
    vendors = [r for r in rows
               if (r.get(COL_STATUS, '') or '').strip().lower() == ACCEPTED_STATUS
               and RECURRING_MATCH in (r.get(COL_CADENCE, '') or '').lower()
               and (r.get(COL_EMAIL, '') or '').strip()]
    logger.info('      %d accepted + recurring vendor(s)', len(vendors))
    if not vendors:
        return

    # [3] dedup: already posted this vendor+period?
    done = {(r.email, r.period) for r in client.query(
        f"SELECT email, period FROM `{STATE_TABLE}`").result()}

    posted, now_iso = [], datetime.now(timezone.utc).isoformat()
    for v in vendors:
        email = v.get(COL_EMAIL, '').strip()
        if (email.lower(), period) in done:
            logger.info('      skip %s (already posted for %s)', email, period)
            continue
        try:
            payments = redash_run(PAYMENTS_QUERY_ID, {'email': email, 'as_of_date': as_of}) or []
            ts = slack_post(channel, 'GST Monthly Payments — %s' % v.get(COL_VENDOR, email),
                            blocks=build_master_blocks(v.get(COL_VENDOR, ''), v.get(COL_GST, ''), email, period, payments))
            slack_post(channel, 'Payment proofs — %s' % period,
                       blocks=build_thread_blocks(period, payments), thread_ts=ts)
            posted.append({'email': email.lower(), 'period': period, 'vendor': v.get(COL_VENDOR, ''),
                           'n_payments': len(payments), 'pinged_at': now_iso})
            logger.info('      posted %s (%d payments)', email, len(payments))
        except Exception as e:
            logger.error('      failed for %s: %s', email, e)

    if posted:
        errs = client.insert_rows_json(client.get_table(STATE_TABLE), posted)
        if errs:
            logger.error('      state-table insert errors: %s', errs)
    logger.info('GST MONTHLY: COMPLETE (%d vendor(s) posted)', len(posted))


# ==================== DAG DEFINITION ====================

default_args = {
    'owner': 'cs_team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'gst_monthly_payments_slack',
    default_args=default_args,
    description='Monthly GST payments + proofs per accepted+recurring vendor (config-driven trigger day)',
    schedule_interval='0 9 * * *',  # daily 09:00 IST; gated on Redash trigger-day config
    catchup=False,
    is_paused_upon_creation=True,   # PAUSED on merge — posts to sensitive tf-cs-finance-collab; unpause only after validation
    tags=['slack', 'gst', 'vendor', 'payments', 'cs_team'],
)

PythonOperator(task_id='run_gst_monthly', python_callable=run_gst_monthly, dag=dag)
