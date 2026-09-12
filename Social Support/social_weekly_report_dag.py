"""
Social weekly report — DB-free. Reads #social-support live at report time and posts a per-platform
reaction table (incoming / open / close / takedown / ignored), Mon 10:00 IST. No DB, tracker, or
backfill: the channel is the source of truth, read fresh each run.

Split of concerns (both edit without touching the other):
  * config query #47889 — tunables: scan/report channels, trigger day/time, emoji->bucket map.
  * message query #47894 — RENDER: builds the whole Slack message (title + table) from the counts,
    so layout/labels/widths/row-filtering are query-editable with no code push.
Only the scan + bucketing (how each post is classified) is reviewed code here (ruff/pytest/PR apply).

Flow: read config #47889 -> gate (Mon 10:00 IST, once/day) -> scan the channel for the PREVIOUS week
(Mon–Sun IST) via conversations.history -> bucket each Brand24 mention by its CURRENT reactions
(coded meaning: 👀 open · ✅ close · 👍 takedown · ❌ ignored; one bucket per post, precedence
takedown>close>ignored>open) -> pass the per-platform counts to message query #47894 -> post the
`message` it returns via the alerts bot. Ships paused. Env SOCIAL_WEEKLY_SLACK_CHANNEL overrides the
destination for testing.
"""

from datetime import timedelta
import logging, os, json, re, html

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

CONFIG_QUERY_ID  = 47889   # tunables: channels, trigger, emoji->bucket map
MESSAGE_QUERY_ID = 47894   # RENDER: builds the Slack table from counts passed as params (edit layout here)
STATE_VAR       = 'SOCIAL_WEEKLY_STATE'
ENV_CHANNEL     = os.getenv('SOCIAL_WEEKLY_SLACK_CHANNEL')   # test override; unset in prod
TRIG_HOUR, TRIG_MIN, TRIG_DOW = 10, 0, 1                     # Mon 10:00 IST

# Fallback emoji->bucket map (config #47889 overrides each). Coded meaning:
FB_EMOJI = {
    'assigned':   'eyes',
    'resolved':   'white_check_mark,heavy_check_mark,ballot_box_with_check',
    'taken_down': '+1,thumbsup',
    'rejected':   'x,heavy_multiplication_x,negative_squared_cross_mark',
}
PLATS = ['linkedin', 'x', 'trustpilot', 'other']


def _load_state(V, state_var):
    """Gate state dict. Absent Variable = fresh ({}); any read/JSON error RAISES so a transient blip
    can't reopen the gate and re-post an already-delivered report (same contract as the L3 posters)."""
    raw = V.get(state_var, default_var=None)
    if raw is None:
        return {}
    state = json.loads(raw)
    if not isinstance(state, dict):
        raise ValueError('state var %s is not a dict: %r' % (state_var, state))
    return state


def _emoji_set(cfg, key, fallback):
    return {x.strip() for x in str(cfg.get(key) or fallback).split(',') if x.strip()}


def _clean(s):
    # Brand24 double-encodes HTML entities; unescape twice.
    return html.unescape(html.unescape(s or '')).strip()


def _source_from_filter(filter_name):
    tok = (filter_name.split(' on ')[-1] if ' on ' in filter_name else filter_name).strip().lower()
    if tok in ('x', 'twitter'):
        return 'x'
    for known in ('linkedin', 'trustpilot', 'reddit', 'facebook', 'instagram', 'youtube'):
        if known in tok:
            return known
    return tok or 'other'


def _platform(src):
    return src if src in ('linkedin', 'x', 'trustpilot') else 'other'


def _bucket(reactions, sets):
    """One bucket per post; precedence takedown > close(resolved) > ignored(rejected) > open."""
    names = {r.get('name') for r in reactions}
    if names & sets['taken_down']:
        return 'takedown'
    if names & sets['resolved']:
        return 'close'
    if names & sets['rejected']:
        return 'ignored'
    return 'open'   # 👀-only or untouched


def _rows_csv(rows):
    """One line per segment: 'seg,incoming,open,close,takedown,ignored', ';'-joined. Safe for the
    Redash text param (digits + segment keys only). The render/message query decides layout + which
    rows to show."""
    order = ['overall'] + PLATS
    return ';'.join(
        '%s,%d,%d,%d,%d,%d' % (seg, r['incoming'], r['open'], r['close'], r['takedown'], r['ignored'])
        for seg, r in ((s, rows[s]) for s in order)
    )


def run_report(**context):
    from airflow.models import Variable
    from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL, SLACK_BOT_TOKEN_ALERTS
    from utils.slack.redash_client import RedashClient
    from utils.slack.slack_client import SlackNotifier
    import urllib.request
    import urllib.parse

    redash = RedashClient(REDASH_API_KEY, REDASH_BASE_URL)
    rows_cfg = redash.fetch_query_results(CONFIG_QUERY_ID) or []
    if not rows_cfg:
        raise Exception('config query %s returned no rows' % CONFIG_QUERY_ID)
    cfg = rows_cfg[0]

    def _int(key, default):
        try:
            return int(cfg.get(key))
        except Exception:
            return default

    trig_hour = _int('trigger_hour', TRIG_HOUR)
    trig_min  = _int('trigger_minute', TRIG_MIN)
    trig_dow  = _int('trigger_dow', TRIG_DOW)
    channel = ENV_CHANNEL or cfg.get('report_channel_id')
    scan_channel = cfg.get('scan_channel_id')
    if not channel or not scan_channel:
        raise Exception('config %s missing report/scan channel' % CONFIG_QUERY_ID)

    state = _load_state(Variable, STATE_VAR)
    now = pendulum.now('Asia/Kolkata')
    today_key = now.format('YYYY-MM-DD')
    dow_ok = (now.isoweekday() == trig_dow)
    time_reached = (now.hour * 60 + now.minute) >= (trig_hour * 60 + trig_min)
    already = (state.get('_last_fire_date') == today_key)
    fire = dow_ok and time_reached and not already
    logger.info('[gate] IST %s dow=%d %02d:%02d target=dow%d %02d:%02d dow_ok=%s reached=%s fired=%s -> fire=%s',
                today_key, now.isoweekday(), now.hour, now.minute, trig_dow, trig_hour, trig_min,
                dow_ok, time_reached, already, fire)
    if not fire:
        logger.info('gate closed, exiting')
        return

    # window = previous complete week, Mon 00:00 -> next Mon 00:00 (covers Mon..Sun)
    this_mon = now.start_of('week')           # pendulum weeks start Monday
    wk_start = this_mon.subtract(days=7)
    wk_end = this_mon.subtract(seconds=1)     # previous Sunday 23:59:59
    oldest, latest = wk_start.timestamp(), this_mon.timestamp()

    sets = {k: _emoji_set(cfg, 'emoji_%s' % k, fb) for k, fb in FB_EMOJI.items()}

    def slack_get(method, params):
        url = 'https://slack.com/api/%s?%s' % (method, urllib.parse.urlencode(params))
        req = urllib.request.Request(url, headers={'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN_ALERTS})
        r = json.loads(urllib.request.urlopen(req, timeout=30).read())
        if not r.get('ok'):
            raise Exception('slack %s: %s' % (method, r.get('error')))
        return r

    # paginate conversations.history over the window
    msgs, cursor = [], None
    while True:
        p = {'channel': scan_channel, 'oldest': '%.6f' % oldest, 'latest': '%.6f' % latest,
             'limit': 200, 'inclusive': 'true'}
        if cursor:
            p['cursor'] = cursor
        r = slack_get('conversations.history', p)
        msgs.extend(r.get('messages', []))
        cursor = (r.get('response_metadata') or {}).get('next_cursor')
        if not cursor:
            break

    rows = {seg: {'incoming': 0, 'open': 0, 'close': 0, 'takedown': 0, 'ignored': 0}
            for seg in PLATS + ['overall']}
    for m in msgs:
        text = m.get('text', '') or ''
        atts = m.get('attachments') or []
        if 'New mentions' not in text or not atts:
            continue
        if 'brand24' not in (atts[0].get('title_link', '') or ''):
            continue
        mf = re.search(r'Filter:\s*([^\n]+)', text)
        seg = _platform(_source_from_filter(_clean(mf.group(1)) if mf else ''))
        b = _bucket(m.get('reactions', []) or [], sets)
        for s in (seg, 'overall'):
            rows[s]['incoming'] += 1
            rows[s][b] += 1

    # Render lives in Redash message query #47894: pass the counts as params, it builds the table.
    mrows = redash.fetch_query_results(MESSAGE_QUERY_ID, parameters={
        'rows': _rows_csv(rows),
        'wk_start': wk_start.to_date_string(),
        'wk_end': wk_end.to_date_string(),
    }) or []
    message = mrows[0].get('message') if mrows else None
    if not message:
        raise Exception('message query %s returned no `message`' % MESSAGE_QUERY_ID)

    SlackNotifier(SLACK_BOT_TOKEN_ALERTS).send_message(
        message, channel_id=channel, unfurl_links=False, unfurl_media=False)

    state['_last_fire_date'] = today_key
    Variable.set(STATE_VAR, json.dumps(state))
    logger.info('posted to %s (%d mentions, window %s..%s)', channel,
                rows['overall']['incoming'], wk_start.to_date_string(), wk_end.to_date_string())


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
    'social_weekly_report',
    default_args=default_args,
    description='DB-free social weekly report: scan #social-support live, config #47889, Mon 10:00 IST',
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['slack', 'social', 'brand24', 'report', 'weekly', 'cs_team'],
)
PythonOperator(task_id='run_report', python_callable=run_report, dag=dag)
