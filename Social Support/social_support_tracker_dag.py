"""
Social Support Tracker — Trinity-style snapshot DB for the #social-support Slack channel.

Brand24 posts one Slack message per brand mention into #social-support; the team reacts with emojis
to run the mention like a support ticket:
  👀 eyes           -> picked up / assigned
  ✅ white_check_mark-> issue resolved
  👍 +1             -> negative post taken down (user removed it)
  ❌ x              -> mention dismissed / rejected
A ticket is OPEN (no emoji), ASSIGNED (👀 only), or CLOSED (any of ✅ / 👍 / ❌ present — the emoji
buckets are independent, so several can be set at once). Removing every closing emoji REOPENS it.

Every 30 min this DAG re-reads the last `lookback_hours` of the channel and upserts one snapshot row
per mention into `support.social_mentions`. Each lifecycle stage keeps BOTH the LATEST value
(scalar `<stage>_at` / `<stage>_by`, always current) AND the full history (`<stage>_*_hist` arrays,
append-only) so "latest state of a ticket" is a direct single-row read while nothing is lost.

NOTE on timing: Slack's history API carries NO per-reaction timestamp, so lifecycle times are the
DAG's DETECTION time (accurate to the 30-min cadence); only `created_at` (the post) is exact.

CONFIG (all editable in Redash #CONFIG_QUERY_ID, no code push): channel_id, lookback_hours,
target_table, slack_subdomain, and the emoji->stage mapping (emoji_assigned/resolved/taken_down/rejected).
The bot (SLACK_BOT_TOKEN_ALERTS) must be a member of the channel with channels:history + reactions:read.
"""

from datetime import datetime, timedelta, timezone
import logging, time, json, html, re

import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.slack.slack_config import (
    REDASH_API_KEY, REDASH_BASE_URL,
    SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN,
)
from utils.slack.bigquery_client import get_bigquery_client

logger = logging.getLogger(__name__)

CONFIG_QUERY_ID = 46788   # "[Social] Social-support tracker config"

# ---- fallbacks only (config #46788 overrides all of these at runtime) ----
CHANNEL_ID      = 'C0AHXA20DHS'
CHANNEL_NAME    = 'social-support'
SLACK_SUBDOMAIN = 'emergentlabsinc'
LOOKBACK_HOURS  = 168
TARGET_TABLE    = 'emergent-default.support.social_mentions'
EMOJI_ASSIGNED   = 'eyes'
EMOJI_RESOLVED   = 'white_check_mark,heavy_check_mark,ballot_box_with_check'
EMOJI_TAKEN_DOWN = '+1,thumbsup'
EMOJI_REJECTED   = 'x,heavy_multiplication_x,negative_squared_cross_mark'

# Lifecycle stages: (key, config-field, fallback). Order matters for status/priority.
STAGES = [
    ('assigned',   'emoji_assigned',   EMOJI_ASSIGNED),
    ('resolved',   'emoji_resolved',   EMOJI_RESOLVED),
    ('taken_down', 'emoji_taken_down', EMOJI_TAKEN_DOWN),
    ('rejected',   'emoji_rejected',   EMOJI_REJECTED),
]
CLOSE_STAGES = ('resolved', 'taken_down', 'rejected')

DDL = """
CREATE TABLE IF NOT EXISTS `{table}` (
  ticket_id STRING, source STRING, author STRING, author_url STRING, text STRING,
  slack_link STRING, filter_name STRING, brand24_project STRING,
  created_at TIMESTAMP,
  status STRING,
  is_assigned BOOL, is_resolved BOOL, is_taken_down BOOL, is_rejected BOOL,
  reaction_count INT64, reopen_count INT64,
  picked_up_at TIMESTAMP,  picked_up_by STRING,
  resolved_at TIMESTAMP,   resolved_by STRING,
  taken_down_at TIMESTAMP, taken_down_by STRING,
  rejected_at TIMESTAMP,   rejected_by STRING,
  reopened_at TIMESTAMP,
  picked_up_at_hist ARRAY<TIMESTAMP>,  picked_up_by_hist ARRAY<STRING>,
  resolved_at_hist ARRAY<TIMESTAMP>,   resolved_by_hist ARRAY<STRING>,
  taken_down_at_hist ARRAY<TIMESTAMP>, taken_down_by_hist ARRAY<STRING>,
  rejected_at_hist ARRAY<TIMESTAMP>,   rejected_by_hist ARRAY<STRING>,
  reopened_at_hist ARRAY<TIMESTAMP>,
  first_seen_at TIMESTAMP, last_synced_at TIMESTAMP
)
"""


# ==================== HELPERS ====================

def redash_run(query_id, parameters=None, max_wait=90):
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


def _cfg_val(cfg, key, default=None):
    return next((r.get(key) for r in (cfg or []) if r.get(key) not in (None, '')), default)


def _emoji_set(cfg, field, fallback):
    v = _cfg_val(cfg, field, fallback)
    return {x.strip() for x in str(v).split(',') if x.strip()}


def slack_get(method, params):
    r = requests.get('https://slack.com/api/%s' % method,
                     headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN},
                     params=params, timeout=30).json()
    if not r.get('ok'):
        raise Exception('slack %s: %s' % (method, r.get('error')))
    return r


def channel_history(channel, oldest_ts):
    """All messages in (oldest_ts, now], following pagination."""
    out, cursor = [], None
    while True:
        p = {'channel': channel, 'oldest': '%.6f' % oldest_ts, 'limit': 200, 'inclusive': 'true'}
        if cursor:
            p['cursor'] = cursor
        r = slack_get('conversations.history', p)
        out.extend(r.get('messages', []))
        cursor = (r.get('response_metadata') or {}).get('next_cursor')
        if not cursor:
            return out


_NAME_CACHE = {}


def user_name(uid):
    if not uid:
        return None
    if uid in _NAME_CACHE:
        return _NAME_CACHE[uid]
    try:
        u = slack_get('users.info', {'user': uid}).get('user', {})
        name = (u.get('profile', {}) or {}).get('real_name') or u.get('real_name') or u.get('name') or uid
    except Exception:
        name = uid
    _NAME_CACHE[uid] = name
    return name


def _clean(s):
    # Brand24 double-encodes HTML entities (e.g. "didn&amp;#039;t"); unescape twice.
    return html.unescape(html.unescape(s or '')).strip()


def _source_from_filter(filter_name):
    tok = (filter_name.split(' on ')[-1] if ' on ' in filter_name else filter_name).strip().lower()
    if tok in ('x', 'twitter'):
        return 'x'
    for known in ('linkedin', 'trustpilot', 'reddit', 'facebook', 'instagram', 'youtube'):
        if known in tok:
            return known
    return tok or 'other'


def parse_mention(msg, subdomain, channel):
    """Return a parsed mention dict, or None if this message is not a Brand24 mention post."""
    text = msg.get('text', '') or ''
    atts = msg.get('attachments') or []
    if 'New mentions' not in text or not atts:
        return None
    a = atts[0]
    if 'brand24' not in (a.get('title_link', '') or ''):
        return None
    ts = msg['ts']
    m_filter = re.search(r'Filter:\s*([^\n]+)', text)
    filter_name = _clean(m_filter.group(1)) if m_filter else ''
    m_proj = re.search(r'in project <[^|>]+\|([^>]+)>', text)
    return {
        'ticket_id': ts,
        'created_at': datetime.fromtimestamp(float(ts), tz=timezone.utc),
        'source': _source_from_filter(filter_name),
        'filter_name': filter_name,
        'brand24_project': _clean(m_proj.group(1)) if m_proj else None,
        'author': _clean(a.get('title')),
        'author_url': a.get('title_link'),
        'text': _clean(a.get('text')),
        'slack_link': 'https://%s.slack.com/archives/%s/p%s' % (subdomain, channel, ts.replace('.', '')),
        'reactions': msg.get('reactions', []) or [],
    }


def _iso(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat()
    return str(v)


def _present(reactions, nameset):
    """(present_bool, [reactor user ids]) for the emojis in nameset."""
    users = []
    for r in reactions:
        if r.get('name') in nameset:
            users.extend(r.get('users', []) or [])
    return (len(users) > 0, users)


def merge_ticket(parsed, prev, emoji_sets, sync_dt):
    """Fold the current reactions of one mention into its snapshot row (latest scalars + history)."""
    prev = prev or {}
    row = {
        'ticket_id': parsed['ticket_id'], 'source': parsed['source'], 'author': parsed['author'],
        'author_url': parsed['author_url'], 'text': parsed['text'], 'slack_link': parsed['slack_link'],
        'filter_name': parsed['filter_name'], 'brand24_project': parsed['brand24_project'],
        'created_at': parsed['created_at'],
        'first_seen_at': prev.get('first_seen_at') or sync_dt,
        'last_synced_at': sync_dt,
        'reaction_count': int(sum(r.get('count', 0) for r in parsed['reactions'])),
        'reopen_count': int(prev.get('reopen_count') or 0),
    }
    flags = {}
    for stage, _field, _fb in STAGES:
        now_present, uids = _present(parsed['reactions'], emoji_sets[stage])
        flags[stage] = now_present
        was_present = bool(prev.get('is_%s' % stage))
        at_hist = list(prev.get('%s_at_hist' % stage) or [])
        by_hist = list(prev.get('%s_by_hist' % stage) or [])
        if now_present and not was_present:            # new occurrence -> append to history
            at_hist.append(sync_dt)
            by_hist.append(', '.join(user_name(u) for u in uids) or None)
        row['%s_at_hist' % stage] = at_hist
        row['%s_by_hist' % stage] = by_hist
        row['%s_at' % stage] = at_hist[-1] if at_hist else None    # latest scalar
        row['%s_by' % stage] = by_hist[-1] if by_hist else None
        row['is_%s' % stage] = now_present
    # picked_up_* mirrors the 'assigned' stage (👀)
    row['picked_up_at'] = row.pop('assigned_at'); row['picked_up_by'] = row.pop('assigned_by')
    row['picked_up_at_hist'] = row.pop('assigned_at_hist'); row['picked_up_by_hist'] = row.pop('assigned_by_hist')
    # reopen: ticket went from CLOSED -> not closed
    was_closed = any(bool(prev.get('is_%s' % s)) for s in CLOSE_STAGES)
    now_closed = any(flags[s] for s in CLOSE_STAGES)
    reop_hist = list(prev.get('reopened_at_hist') or [])
    if was_closed and not now_closed:
        reop_hist.append(sync_dt)
        row['reopen_count'] += 1
    row['reopened_at_hist'] = reop_hist
    row['reopened_at'] = reop_hist[-1] if reop_hist else None
    row['status'] = 'closed' if now_closed else ('assigned' if flags['assigned'] else 'open')
    return row


BQ_SCHEMA = None  # built lazily inside the task (needs google.cloud.bigquery)


def run_social_tracker(**context):
    from google.cloud import bigquery
    logger.info('SOCIAL SUPPORT TRACKER')

    cfg = redash_run(CONFIG_QUERY_ID) or []
    channel   = _cfg_val(cfg, 'channel_id', CHANNEL_ID)
    subdomain = _cfg_val(cfg, 'slack_subdomain', SLACK_SUBDOMAIN)
    table     = _cfg_val(cfg, 'target_table', TARGET_TABLE)
    lookback  = int(_cfg_val(cfg, 'lookback_hours', LOOKBACK_HOURS))
    emoji_sets = {stage: _emoji_set(cfg, field, fb) for stage, field, fb in STAGES}
    logger.info('[cfg] channel=%s lookback=%dh table=%s', channel, lookback, table)

    client = get_bigquery_client()
    client.query(DDL.format(table=table)).result()

    # existing snapshot rows (so we can diff reaction presence + keep history)
    existing = {}
    for r in client.query('SELECT * FROM `%s`' % table).result():
        existing[r['ticket_id']] = dict(r)
    logger.info('      %d existing ticket(s) in table', len(existing))

    now = pendulum.now('UTC')
    sync_dt = datetime.fromtimestamp(now.timestamp(), tz=timezone.utc)
    oldest = now.timestamp() - lookback * 3600

    msgs = channel_history(channel, oldest)
    parsed = [p for p in (parse_mention(m, subdomain, channel) for m in msgs) if p]
    logger.info('      %d message(s) in window, %d mention ticket(s)', len(msgs), len(parsed))

    merged = dict(existing)  # start from all known tickets; overwrite the ones we re-scanned
    changed = 0
    for p in parsed:
        merged[p['ticket_id']] = merge_ticket(p, existing.get(p['ticket_id']), emoji_sets, sync_dt)
        changed += 1

    # normalize every row to JSON-loadable (timestamps -> ISO strings)
    ts_scalars = ['created_at', 'first_seen_at', 'last_synced_at', 'reopened_at',
                  'picked_up_at', 'resolved_at', 'taken_down_at', 'rejected_at']
    ts_arrays  = ['reopened_at_hist', 'picked_up_at_hist', 'resolved_at_hist', 'taken_down_at_hist', 'rejected_at_hist']
    out = []
    for row in merged.values():
        row = dict(row)
        for k in ts_scalars:
            row[k] = _iso(row.get(k))
        for k in ts_arrays:
            row[k] = [_iso(x) for x in (row.get(k) or [])]
        out.append(row)

    schema = [
        bigquery.SchemaField('ticket_id', 'STRING'), bigquery.SchemaField('source', 'STRING'),
        bigquery.SchemaField('author', 'STRING'), bigquery.SchemaField('author_url', 'STRING'),
        bigquery.SchemaField('text', 'STRING'), bigquery.SchemaField('slack_link', 'STRING'),
        bigquery.SchemaField('filter_name', 'STRING'), bigquery.SchemaField('brand24_project', 'STRING'),
        bigquery.SchemaField('created_at', 'TIMESTAMP'), bigquery.SchemaField('status', 'STRING'),
        bigquery.SchemaField('is_assigned', 'BOOL'), bigquery.SchemaField('is_resolved', 'BOOL'),
        bigquery.SchemaField('is_taken_down', 'BOOL'), bigquery.SchemaField('is_rejected', 'BOOL'),
        bigquery.SchemaField('reaction_count', 'INT64'), bigquery.SchemaField('reopen_count', 'INT64'),
        bigquery.SchemaField('picked_up_at', 'TIMESTAMP'), bigquery.SchemaField('picked_up_by', 'STRING'),
        bigquery.SchemaField('resolved_at', 'TIMESTAMP'), bigquery.SchemaField('resolved_by', 'STRING'),
        bigquery.SchemaField('taken_down_at', 'TIMESTAMP'), bigquery.SchemaField('taken_down_by', 'STRING'),
        bigquery.SchemaField('rejected_at', 'TIMESTAMP'), bigquery.SchemaField('rejected_by', 'STRING'),
        bigquery.SchemaField('reopened_at', 'TIMESTAMP'),
        bigquery.SchemaField('picked_up_at_hist', 'TIMESTAMP', mode='REPEATED'),
        bigquery.SchemaField('picked_up_by_hist', 'STRING', mode='REPEATED'),
        bigquery.SchemaField('resolved_at_hist', 'TIMESTAMP', mode='REPEATED'),
        bigquery.SchemaField('resolved_by_hist', 'STRING', mode='REPEATED'),
        bigquery.SchemaField('taken_down_at_hist', 'TIMESTAMP', mode='REPEATED'),
        bigquery.SchemaField('taken_down_by_hist', 'STRING', mode='REPEATED'),
        bigquery.SchemaField('rejected_at_hist', 'TIMESTAMP', mode='REPEATED'),
        bigquery.SchemaField('rejected_by_hist', 'STRING', mode='REPEATED'),
        bigquery.SchemaField('reopened_at_hist', 'TIMESTAMP', mode='REPEATED'),
        bigquery.SchemaField('first_seen_at', 'TIMESTAMP'), bigquery.SchemaField('last_synced_at', 'TIMESTAMP'),
    ]
    job_cfg = bigquery.LoadJobConfig(schema=schema, write_disposition='WRITE_TRUNCATE')
    client.load_table_from_json(out, table, job_config=job_cfg).result()
    logger.info('SOCIAL SUPPORT TRACKER: wrote %d ticket(s) (%d refreshed this run)', len(out), changed)


# ==================== DAG ====================

default_args = {
    'owner': 'rishav.k@emergent.sh',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 9, 1, tz='Asia/Kolkata'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'social_support_tracker',
    default_args=default_args,
    description='Snapshot #social-support Brand24 mentions as tickets (emoji lifecycle) into support.social_mentions',
    schedule_interval='*/30 * * * *',   # every 30 min
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,        # unpause after first validated run
    tags=['slack', 'social', 'brand24', 'support', 'cs_team'],
)
PythonOperator(task_id='run_social_tracker', python_callable=run_social_tracker, dag=dag)
