"""
Calendly — Launch Partner Deployment booking alert -> Slack (poll DAG, IST)

Fires a Slack alert for every booking on the "Launch Partner Deployment" round-robin event type.
WHEN it fires is config-driven (edit Redash #45406, no code push):
  * lead_minutes = 0    -> alert AS SOON AS someone books  (detected within one poll cycle)
  * lead_minutes = 60   -> alert ~1 hour before the call
  * lead_minutes = 120  -> alert ~2 hours before the call
  * lead_minutes = N     -> alert ~N minutes before the call

Calendly is pull-based here (Composer can't host a webhook), so this DAG POLLS
`GET /scheduled_events` (org scope; client-side filter by event_type — the server-side event_type
param 403s) every 5 min and keeps state in an Airflow Variable:
  on-book mode: a `watermark` on created_at (first run seeds =now to skip backlog, so only NEW
                bookings alert); fire when created_at > watermark.
  lead  mode:   a `fired` set of event URIs; fire once when 0 <= (start_time - now) <= lead_minutes.

CONFIG (Redash #45406): channel_id / event_label / event_type_uri / org_uri / lead_minutes /
poll_enabled / calendly_token.
Env overrides for tests: CAL_LPD_SLACK_CHANNEL (channel), CAL_LPD_SEED_WATERMARK (ISO; forces the
on-book watermark for a one-off backfire).
Schedule: every 5 min IST. Ships paused (posts to a real channel; unpause after validating).
"""
from datetime import timedelta
import os, json, time, logging, urllib.request, urllib.parse

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.slack import RedashClient
from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL, SLACK_BOT_TOKEN_ALERTS as SLACK_TOKEN

logger = logging.getLogger(__name__)

CONFIG_QUERY_ID = 45406
STATE_VAR = 'CALENDLY_LPD_STATE'
ENV_CHANNEL = os.getenv('CAL_LPD_SLACK_CHANNEL')
SEED_WATERMARK = os.getenv('CAL_LPD_SEED_WATERMARK')
CAL = 'https://api.calendly.com'
MAX_PAGES = 3   # upcoming events pages to scan (LPD is a small subset)


# ==================== Calendly (stdlib + backoff for 429/1010 rate blips) ====================
def _cal(token, path, params=None):
    url = path if path.startswith('http') else CAL + path + ('?' + urllib.parse.urlencode(params) if params else '')
    last = None
    for i in range(6):
        try:
            req = urllib.request.Request(url, headers={'Authorization': 'Bearer ' + token,
                                                       'User-Agent': 'emergent-cs-dag/1.0', 'Accept': 'application/json'})
            return json.loads(urllib.request.urlopen(req, timeout=45).read())
        except urllib.error.HTTPError as e:
            last = e
            if e.code in (429, 403, 500, 502, 503):   # 403/1010 shows up as a transient rate blip
                time.sleep(2 + 3 * i); continue
            raise
        except Exception as e:
            last = e; time.sleep(2 + 3 * i)
    raise RuntimeError('calendly %s failed: %s' % (path, last))


def _list_upcoming(token, org, now_iso):
    """All upcoming active scheduled events (org scope), paged; caller filters by event_type."""
    out = []
    d = _cal(token, '/scheduled_events',
             {'organization': org, 'sort': 'start_time:asc', 'count': 100, 'min_start_time': now_iso})
    out += d.get('collection', [])
    for _ in range(MAX_PAGES - 1):
        nxt = (d.get('pagination') or {}).get('next_page')   # full URL — must NOT re-add params
        if not nxt:
            break
        time.sleep(0.5)
        d = _cal(token, nxt)
        out += d.get('collection', [])
    return out


def _invitees(token, event_uri):
    u = event_uri.rstrip('/').split('/')[-1]
    try:
        return (_cal(token, '/scheduled_events/%s/invitees' % u, {'count': 20}) or {}).get('collection', [])
    except Exception as e:
        logger.warning('[cal_lpd] invitees fetch failed for %s: %s', u, e); return []


# ==================== Slack ====================
def _slack(channel, text):
    body = urllib.parse.urlencode({'channel': channel, 'text': text, 'unfurl_links': 'false'}).encode()
    req = urllib.request.Request('https://slack.com/api/chat.postMessage', data=body,
                                 headers={'Authorization': 'Bearer ' + SLACK_TOKEN})
    r = json.loads(urllib.request.urlopen(req, timeout=30).read())
    if not r.get('ok'):
        logger.error('[cal_lpd] slack post failed: %s', r.get('error'))
    return r.get('ok')


def _host_mention(email, name):
    # Slack email == Calendly host email -> resolve a real @mention; fall back to plain @name.
    if email:
        try:
            req = urllib.request.Request('https://slack.com/api/users.lookupByEmail?' + urllib.parse.urlencode({'email': email}),
                                         headers={'Authorization': 'Bearer ' + SLACK_TOKEN})
            r = json.loads(urllib.request.urlopen(req, timeout=20).read())
            if r.get('ok'):
                return '<@%s>' % r['user']['id']
            logger.info('[cal_lpd] lookupByEmail %s -> %s (plain name)', email, r.get('error'))
        except Exception as e:
            logger.warning('[cal_lpd] lookupByEmail %s failed: %s', email, e)
    return '@' + (name or email or '?')


def _qa(inv, *keys):
    for qa in (inv.get('questions_and_answers') or []):
        q = (qa.get('question') or '').lower()
        if any(k in q for k in keys):
            a = (qa.get('answer') or '').strip()
            if a:
                return a
    return None


def _fmt_alert(ev, invitees, lead_minutes, now):
    start = pendulum.parse(ev['start_time'])
    ist = start.in_timezone('Asia/Kolkata')
    end = pendulum.parse(ev['end_time']) if ev.get('end_time') else None
    dur = int(round((end - start).total_minutes())) if end else None
    mins = (start - now).total_minutes()
    head = (':calendar: *New Launch Partner Deployment booked*' if lead_minutes == 0
            else ':alarm_clock: *Launch Partner Deployment starts in ~%dh %dm*' % (int(mins // 60), int(mins % 60)))
    m = (ev.get('event_memberships') or [{}])[0]
    inv = invitees[0] if invitees else {}
    lines = [
        head,
        '*Host:* %s' % _host_mention(m.get('user_email'), m.get('user_name')),
        '*Invitee:* %s  <%s>' % (inv.get('name') or '?', inv.get('email') or '?'),
    ]
    join = (ev.get('location') or {}).get('join_url')
    if join:
        lines.append('*Meeting link:* %s' % join)
    lines.append('*Booked at:* %s IST' % ist.format('ddd DD MMM, HH:mm'))
    if dur is not None:
        lines.append('*Duration:* %d min' % dur)
    lines.append('*Job ID:* %s' % (_qa(inv, 'job id', 'job_id') or '—'))
    lines.append('*Description:* %s' % (_qa(inv, 'issue', 'description') or '—'))
    return '\n'.join(lines)


# ==================== TASK ====================
def run_lpd_alert(**context):
    redash = RedashClient(api_key=REDASH_API_KEY, base_url=REDASH_BASE_URL)
    cfg = (redash.fetch_query_results(query_id=CONFIG_QUERY_ID, max_retries=3) or [{}])[0]
    if str(cfg.get('poll_enabled', True)).lower() in ('false', '0', 'no', ''):
        logger.info('[cal_lpd] poll_enabled false -> skip'); return
    token = cfg['calendly_token']; et = cfg['event_type_uri']; org = cfg['org_uri']
    channel = ENV_CHANNEL or cfg['channel_id']
    try:
        lead = int(cfg.get('lead_minutes', 0))
    except Exception:
        lead = 0
    now = pendulum.now('UTC')

    events = [e for e in _list_upcoming(token, org, now.strftime('%Y-%m-%dT%H:%M:%SZ'))
              if e.get('event_type') == et and e.get('status') == 'active']

    try:
        state = json.loads(Variable.get(STATE_VAR))
    except Exception:
        state = None
    first_run = state is None
    if first_run:
        state = {'watermark': SEED_WATERMARK or now.to_iso8601_string(), 'fired': []}

    fired = set(state.get('fired', []))
    alerted = 0

    if lead == 0:
        wm = pendulum.parse(state['watermark'])
        newmax = wm
        for e in sorted(events, key=lambda x: x['created_at']):
            c = pendulum.parse(e['created_at'])
            if c > wm:
                if _slack(channel, _fmt_alert(e, _invitees(token, e['uri']), lead, now)):
                    alerted += 1
                newmax = max(newmax, c)
        state['watermark'] = newmax.to_iso8601_string()
    else:
        upcoming_uris = {e['uri'] for e in events}
        for e in events:
            mins = (pendulum.parse(e['start_time']) - now).total_minutes()
            if 0 <= mins <= lead and e['uri'] not in fired:
                if _slack(channel, _fmt_alert(e, _invitees(token, e['uri']), lead, now)):
                    fired.add(e['uri']); alerted += 1
        state['fired'] = [u for u in fired if u in upcoming_uris]   # prune past

    Variable.set(STATE_VAR, json.dumps(state))
    logger.info('[cal_lpd] mode=%s lead=%s LPD_upcoming=%d alerted=%d first_run=%s',
                'on_book' if lead == 0 else 'lead', lead, len(events), alerted, first_run)


# ==================== DAG ====================
_default_args = {'owner': 'cs_team', 'depends_on_past': False,
                 'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
                 'email_on_failure': False, 'email_on_retry': False, 'retries': 1, 'retry_delay': timedelta(minutes=2)}
dag = DAG('calendly_launch_partner_alert', default_args=_default_args,
          description='Alert on Launch Partner Deployment bookings (on-book or N-min-before; config #45406)',
          schedule_interval='*/5 * * * *', catchup=False, is_paused_upon_creation=True,
          tags=['slack', 'alerts', 'cs_team', 'calendly'])
PythonOperator(task_id='run_lpd_alert', python_callable=run_lpd_alert, dag=dag)
