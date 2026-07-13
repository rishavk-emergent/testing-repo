"""
CS SOD Count - Slack DAG (daily, IST)

Posts the "SOD Count" backlog snapshot to cs-associates: a header line plus one line per
bucket (All / L1 / L2 / Expo / L3 needs review / L3 real), e.g.

    SOD Count:
    All - 182
    > L1 - 9
    > L2 - 74
    > Expo - 81
    > L3 needs review - 1
    > L3 real - 26

WHY NOT BIGQUERY: the Trinity BQ mirror (v_tickets) lags on status transitions, so its OPEN
count runs ~2x Trinity's live count — it cannot reproduce the real bucket numbers. So counts are
read LIVE from Trinity's MCP endpoint (ticket_counts per admin bucket); value = open + pending.

CONFIG-DRIVEN (Redash #CONFIG_QUERY_ID) — edit there, no code push:
  Each row = one message line: line_order, label, type ('bucket'|'sum'), bucket_id, indent(0/1).
  Globals on every row: channel_id, channel_name, trinity_api_key.
  * type='bucket' -> DAG calls Trinity ticket_counts(bucket_id); value = open + pending.
  * type='sum'    -> DAG sums the values of the 'bucket' rows (that's how "All" is computed).
  Add / remove / reorder buckets = edit rows in the query.

Schedule: '0 10 * * *' Asia/Kolkata (daily). Channel: from config (cs-associates); override
CS_SOD_SLACK_CHANNEL env for testing.
"""

from datetime import timedelta
import logging, os, json, time

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack.slack_config import (
    REDASH_API_KEY, REDASH_BASE_URL,
    SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN,
)

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
ENV_CHANNEL_OVERRIDE = os.getenv('CS_SOD_SLACK_CHANNEL')   # set to a test channel for dry runs; unset in prod
FALLBACK_CHANNEL     = 'C0B075CBPS7'                        # cs-associates
CONFIG_QUERY_ID      = 40628                                # Redash: "[CS] SOD Count config"
TRINITY_MCP_URL      = 'https://trinity-base.internal.emergent.host/api/mcp/'   # base URL lives in code
COUNT_STATUSES       = ('open', 'pending')                  # value per bucket = sum of these


# ==================== REDASH (config) ====================

def redash_run(query_id, max_wait=90):
    h = {'Authorization': 'Key %s' % REDASH_API_KEY, 'Content-Type': 'application/json'}
    job = requests.post('%s/api/queries/%s/results' % (REDASH_BASE_URL, query_id),
                        json={'parameters': {}, 'max_age': 0}, headers=h, timeout=60).json()
    if 'query_result' in job:
        return job['query_result']['data']['rows']
    jid = job['job']['id']
    for _ in range(max_wait):
        jr = requests.get('%s/api/jobs/%s' % (REDASH_BASE_URL, jid), headers=h, timeout=30).json()['job']
        if jr['status'] in (3, 4):
            if jr['status'] == 4:
                raise Exception('Redash config query %s failed: %s' % (query_id, jr.get('error')))
            rid = jr['query_result_id']
            return requests.get('%s/api/query_results/%s.json' % (REDASH_BASE_URL, rid),
                                headers=h, timeout=30).json()['query_result']['data']['rows']
        time.sleep(2)
    raise Exception('Redash config query %s timed out' % query_id)


# ==================== TRINITY MCP (live counts) ====================

def _mcp_parse(text):
    if text.strip().startswith('{'):
        return json.loads(text)
    for line in text.splitlines():          # SSE
        if line.startswith('data:'):
            try:
                return json.loads(line[5:].strip())
            except Exception:
                pass
    raise Exception('unparseable MCP response: %s' % text[:200])


class TrinityMCP:
    """Minimal MCP-over-HTTP client: initialize once, then call ticket_counts per bucket."""

    def __init__(self, url, api_key):
        self.url = url
        self.h = {'Authorization': 'Bearer %s' % api_key,
                  'Content-Type': 'application/json',
                  'Accept': 'application/json, text/event-stream'}
        init = requests.post(self.url, headers=self.h, timeout=30, json={
            'jsonrpc': '2.0', 'id': 1, 'method': 'initialize',
            'params': {'protocolVersion': '2024-11-05', 'capabilities': {},
                       'clientInfo': {'name': 'cs-sod-counts', 'version': '1'}}})
        sid = init.headers.get('Mcp-Session-Id')
        if sid:
            self.h['Mcp-Session-Id'] = sid
        requests.post(self.url, headers=self.h, timeout=30,
                      json={'jsonrpc': '2.0', 'method': 'notifications/initialized', 'params': {}})

    def ticket_counts(self, bucket_id):
        r = requests.post(self.url, headers=self.h, timeout=30, json={
            'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call',
            'params': {'name': 'ticket_counts', 'arguments': {'bucket_id': bucket_id}}})
        data = _mcp_parse(r.text)
        if data.get('error'):
            raise Exception('Trinity ticket_counts error: %s' % data['error'])
        return json.loads(data['result']['content'][0]['text'])   # {open, pending, ...}


# ==================== SLACK ====================

def slack_post(channel, text):
    d = requests.post('https://slack.com/api/chat.postMessage',
                      headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN,
                               'Content-Type': 'application/json; charset=utf-8'},
                      json={'channel': channel, 'text': text, 'unfurl_links': False,
                            'unfurl_media': False}, timeout=30).json()
    if not d.get('ok'):
        raise Exception('chat.postMessage failed: %s' % d.get('error'))
    return d['ts']


# ==================== MAIN TASK ====================

def run_cs_sod_counts(**context):
    logger.info('=' * 60)
    logger.info('CS SOD COUNT')
    logger.info('=' * 60)

    rows = redash_run(CONFIG_QUERY_ID)
    if not rows:
        logger.info('SOD: config query returned no rows')
        return
    rows.sort(key=lambda r: r.get('line_order', 0))
    channel = ENV_CHANNEL_OVERRIDE or rows[0].get('channel_id') or FALLBACK_CHANNEL
    api_key = rows[0].get('trinity_api_key')

    # [1] live counts per bucket from Trinity
    mcp = TrinityMCP(TRINITY_MCP_URL, api_key)
    values, bucket_total = {}, 0
    for r in rows:
        if r.get('type') == 'bucket' and r.get('bucket_id'):
            c = mcp.ticket_counts(r['bucket_id'])
            v = sum(int(c.get(s, 0) or 0) for s in COUNT_STATUSES)
            values[r['line_order']] = v
            bucket_total += v
            logger.info('      %s = %d (%s)', r.get('label'), v, c)

    # [2] resolve 'sum' rows (e.g. All)
    for r in rows:
        if r.get('type') == 'sum':
            values[r['line_order']] = bucket_total

    # [3] render message (indent 1 -> Slack quote line)
    today = pendulum.now('Asia/Kolkata').format('D MMM YYYY')
    lines = ['*SOD Count:*  _(%s)_' % today]
    for r in rows:
        v = values.get(r['line_order'], 0)
        prefix = '> ' if int(r.get('indent', 0) or 0) >= 1 else ''
        link = (r.get('link') or '').strip()
        value = '<%s|%d>' % (link, v) if link else str(v)
        lines.append('%s%s - %s' % (prefix, r.get('label'), value))
    msg = '\n'.join(lines)

    logger.info('[4] Posting to %s', channel)
    slack_post(channel, msg)
    logger.info('CS SOD COUNT: COMPLETE')


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
    'cs_sod_counts_slack',
    default_args=default_args,
    description='Post SOD Count (live Trinity bucket counts) to cs-associates; buckets configured in Redash',
    schedule_interval='0 10 * * *',   # daily 10:00 IST
    catchup=False,
    is_paused_upon_creation=True,     # posts to cs-associates; unpause after validation
    tags=['slack', 'trinity', 'sod', 'cs_reports', 'cs_team'],
)

PythonOperator(task_id='run_cs_sod_counts', python_callable=run_cs_sod_counts, dag=dag)
