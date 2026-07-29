"""
Radio Silence & Multi-Ticket Alert - Slack DAG (every 30 min, IST)

Flags support tickets that have gone quiet or users who are piling up open tickets, so the team
can jump on them. Per user, per run it posts (into a single daily thread, not the channel):
  🔴  Radio silence — OPEN tickets with NO response from our side (agent OR Overwatch) in
      SILENCE_HOURS (default 24h).  [never-responded tickets older than that count too]
  📂  Multiple open — the user's full open-ticket list, shown when they have >= MIN_OPEN (default 3).
A user is alerted if either applies; both sections show if both apply.

ARCHITECTURE (master + daily thread, like real_l3_hygiene_slack):
  * First alert of the IST day posts a MASTER message to the channel (master_title + date).
  * Every user alert is a THREADED REPLY under that day's master — keeps the channel un-spammed.
  * COOLDOWN: once a user is alerted, they are skipped for cooldown_hours (default 2h) before
    being highlighted again.

WHERE THE LOGIC LIVES (all config in Redash, no code push):
  * FEED query #FEED_QUERY_ID  — qualifying users + their silent/open tickets. Tune SILENCE_HOURS
    and MIN_OPEN via the DECLAREs at the top of that query.
  * CONFIG query #CONFIG_QUERY_ID — channel_id, cooldown_hours, master_title. Edit anytime.
  * The DAG only does: read queries -> cooldown filter -> master/thread plumbing -> render -> post.

State (Airflow Variables): RADIO_MASTER {ist_date: thread_ts}, RADIO_ALERTED {email: last_iso}.
Schedule: '*/30 * * * *' Asia/Kolkata. Channel from config; RADIO_SLACK_CHANNEL env forces a channel.
"""

from datetime import timedelta
import logging, os, json, time

import pendulum, requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL, SLACK_BOT_TOKEN_ALERTS as SLACK_TOKEN

logger = logging.getLogger(__name__)

FEED_QUERY_ID   = 42417
CONFIG_QUERY_ID = 42418
MASTER_VAR      = 'RADIO_MASTER'    # {ist_date: thread_ts}
ALERTED_VAR     = 'RADIO_ALERTED'   # {email: last_alert_iso}
TICKET_URL      = 'https://trinity-base.internal.emergent.host/tickets/%s'
ENV_CHANNEL     = os.getenv('RADIO_SLACK_CHANNEL')   # test override
FORCE_RUN       = os.getenv('RADIO_FORCE') == '1'    # ignore cooldown (testing)


# ==================== Redash ====================
def _https(u):
    return (u or '').replace('http://', 'https://')

def redash_run(query_id):
    base = _https(REDASH_BASE_URL); h = {'Authorization': 'Key %s' % REDASH_API_KEY, 'Content-Type': 'application/json'}
    j = requests.post('%s/api/queries/%s/results' % (base, query_id), json={'parameters': {}, 'max_age': 0}, headers=h, timeout=120).json()
    if 'query_result' in j:
        return j['query_result']['data']['rows']
    jid = j['job']['id']
    for _ in range(120):
        jr = requests.get('%s/api/jobs/%s' % (base, jid), headers=h, timeout=30).json()['job']
        if jr['status'] in (3, 4):
            if jr['status'] == 4:
                raise Exception('query %s failed: %s' % (query_id, jr.get('error')))
            return requests.get('%s/api/query_results/%s.json' % (base, jr['query_result_id']), headers=h, timeout=30).json()['query_result']['data']['rows']
        time.sleep(2)
    raise Exception('query %s timed out' % query_id)


# ==================== Slack ====================
def slack_post(channel, text, thread_ts=None):
    # parse='none' stops Slack auto-linking the code-box email into a mailto link;
    # explicit <url|#num> ticket links still render.
    p = {'channel': channel, 'text': text, 'unfurl_links': False, 'unfurl_media': False, 'parse': 'none'}
    if thread_ts:
        p['thread_ts'] = thread_ts
    d = requests.post('https://slack.com/api/chat.postMessage',
                      headers={'Authorization': 'Bearer %s' % SLACK_TOKEN, 'Content-Type': 'application/json; charset=utf-8'},
                      json=p, timeout=30).json()
    if not d.get('ok'):
        raise Exception('chat.postMessage failed: %s' % d.get('error'))
    return d['ts']


# ==================== render ====================
def _lvl(v):
    return v if v else 'L?'

def _summary(row):
    s = (row.get('subject') or '').strip() or (row.get('last_message_snippet') or '').strip()
    s = ' '.join(s.split())
    return (s[:90] + '…') if len(s) > 90 else s

LIST_CAP    = 12          # cap tickets shown per section (avoids giant replies)
EMOJI_SILENT = ':hourglass_flowing_sand:'
EMOJI_OPEN   = ':card_index_dividers:'

def _ticket_line(row, silent=False):
    link = '<%s|#%d>' % (TICKET_URL % row['ticket_id'], int(row['num']))
    tags = (' · _%s_' % row['tags']) if row.get('tags') else ''
    age  = (' · *%dh silent*' % int(row['hours_since_resp'])) if silent else ''
    summ = _summary(row)
    return '   • %s `[%s]`%s%s%s' % (link, _lvl(row.get('level')), age, tags, (' — ' + summ) if summ else '')

def _lines(rows, silent=False):
    out = [_ticket_line(r, silent=silent) for r in rows[:LIST_CAP]]
    if len(rows) > LIST_CAP:
        out.append('   • _…and %d more_' % (len(rows) - LIST_CAP))
    return out

def build_user_reply(email, rows):
    silent = sorted([r for r in rows if r.get('is_silent')], key=lambda r: -r['hours_since_resp'])
    others = sorted([r for r in rows if not r.get('is_silent')], key=lambda r: int(r['num']))
    multi  = rows[0].get('user_multi_open')
    open_n = int(rows[0].get('open_count', len(rows)))
    ltv    = rows[0].get('ltv')

    meta = []
    if ltv not in (None, ''):
        meta.append('$%d LTV' % round(float(ltv)))
    meta.append('%d open' % open_n)
    parts = ['`%s`  (%s)' % (email, ' · '.join(meta))]   # email in a code box so each user block stands out

    if silent:
        parts.append('%s *No response %dh+ (%d):*' % (EMOJI_SILENT, int(silent[0]['hours_since_resp']), len(silent)))
        parts += _lines(silent, silent=True)
    if multi:
        # for a both-case user, only list the open tickets NOT already shown as silent
        if silent and others:
            parts.append('%s *Also open (%d):*' % (EMOJI_OPEN, len(others)))
            parts += _lines(others)
        elif not silent:
            parts.append('%s *Multiple open (%d):*' % (EMOJI_OPEN, open_n))
            parts += _lines(others)
    return '\n'.join(parts)

def build_master(title, ist_date):
    try:
        label = pendulum.from_format(ist_date, 'YYYY-MM-DD').format('D MMM YYYY')
    except Exception:
        label = ist_date
    return '%s  (%s)' % (title, label)


# ==================== MAIN ====================
def run_radio_silence(**context):
    cfg = redash_run(CONFIG_QUERY_ID)[0]
    channel = ENV_CHANNEL or cfg['channel_id']
    cooldown_h = float(cfg.get('cooldown_hours') or 2)
    title = cfg.get('master_title') or ':satellite: *Radio Silence & Multi-Ticket Alerts*'

    rows = redash_run(FEED_QUERY_ID)
    if not rows:
        logger.info('RADIO: no qualifying users'); return
    users = {}
    for r in rows:
        users.setdefault(r['email'], []).append(r)
    logger.info('RADIO: %d qualifying users (%d ticket rows)', len(users), len(rows))

    now = pendulum.now('Asia/Kolkata')
    ist_date = now.format('YYYY-MM-DD')

    # cooldown state (prune stale entries)
    try:
        alerted = json.loads(Variable.get(ALERTED_VAR, default_var='{}'))
    except Exception:
        alerted = {}
    fresh = {}
    for em, iso in alerted.items():
        try:
            if now.diff(pendulum.parse(iso)).in_hours() < cooldown_h:
                fresh[em] = iso
        except Exception:
            pass
    alerted = fresh

    # master for today
    try:
        master = json.loads(Variable.get(MASTER_VAR, default_var='{}'))
    except Exception:
        master = {}
    thread_ts = master.get(ist_date)

    posted = 0
    for email, urows in users.items():
        if not FORCE_RUN and email in alerted:
            continue  # within cooldown
        if not thread_ts:
            thread_ts = slack_post(channel, build_master(title, ist_date))
            master = {ist_date: thread_ts}  # keep only today
            logger.info('RADIO: posted master for %s (ts=%s)', ist_date, thread_ts)
        try:
            slack_post(channel, build_user_reply(email, urows), thread_ts=thread_ts)
            alerted[email] = now.to_iso8601_string()
            posted += 1
        except Exception as e:
            logger.error('RADIO: failed to post for %s: %s', email, e)

    Variable.set(MASTER_VAR, json.dumps(master))
    Variable.set(ALERTED_VAR, json.dumps(alerted))
    logger.info('RADIO: complete — %d user alert(s) posted, %d skipped (cooldown)', posted, len(users) - posted)


default_args = {
    'owner': 'cs_team', 'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False, 'email_on_retry': False, 'retries': 1, 'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'radio_silence_ticket_alert',
    default_args=default_args,
    description='Alert (in a daily Slack thread) on tickets with no agent/OW response in 24h, or users with >=3 open tickets',
    schedule_interval='*/30 * * * *',   # every 30 min, IST
    catchup=False, max_active_runs=1,
    dagrun_timeout=timedelta(minutes=20),
    is_paused_upon_creation=True,        # posts to a channel; unpause after validating
    tags=['slack', 'trinity', 'radio-silence', 'tickets', 'cs_team'],
)

PythonOperator(task_id='run_radio_silence', python_callable=run_radio_silence, dag=dag)
