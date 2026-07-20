"""
ERC Escalation Assistant - Slack DAG (poll every 5 min, IST)

Two behaviours in one DAG, both keyed off the #tf_erc_team channel:

PHASE A - New-mail briefing
  When the ERC bot posts a "New mail to erc@emergent.sh", extract the CUSTOMER email, pull their
  context LIVE (Trinity: customer + tickets + timelines; Overwatch: verified per-ticket RCA),
  compute the hard numbers in code, run ONE gpt-4o-mini call (via the internal LiteLLM proxy),
  and post a 4-section briefing as a reply in the same thread. Then register the customer for
  30-day ticket tracking (Phase B).

PHASE B - 30-day ticket tracker
  For every customer registered in the last 30 days, diff their current Trinity tickets against
  the baseline we stored. Any NEW ticket, or any ticket that REOPENED (closed -> open), gets a
  one-line gpt-4o-mini blurb and is posted into the SAME ERC thread with a clickable Trinity link.

WHY POLL, NOT WEBHOOK: Composer/Airflow cannot host an inbound listener; no analytics-dags DAG
does. So this ticks every 5 min. Idempotency:
  - Phase A: before briefing, we read the thread and skip if our auto-brief marker is already there
    (survives watermark resets). A watermark Variable bounds how far back we scan.
  - Phase B: state lives in BigQuery (support.erc_tracked_customers); we only post ticket ids not
    already in the stored baseline, then fold them into the baseline.

WHERE THE LOGIC LIVES
  - Numbers: computed here from Trinity/Overwatch responses (never invented by the LLM).
  - Narrative: the prompts below (BRIEF_SYSTEM / build_brief_prompt / blurb prompt).
  - Connection config (URLs, service keys, model, channel, track_days): Redash config query
    #CONFIG_QUERY_ID, one row of globals - edit there, no code push. Mirrors cs_sod_counts_dag.py.

CONFIG ROW COLUMNS (Redash "[ERC] config"):
  channel_id, erc_bot_id, trinity_mcp_url, trinity_api_key, overwatch_mcp_url, overwatch_api_key,
  llm_proxy_url, llm_proxy_api_key, llm_model, track_days
  (all on a single row; read from rows[0])

TO CONFIRM AT FIRST TEST (isolated to config, not code):
  - overwatch_mcp_url + overwatch_api_key: internal base is https://overwatch.internal.emergent.host
    and the app has an OVERWATCH_API_KEY; confirm the MCP mount path + that this key authorizes it.
  - Trinity list_tickets/get_customer exact field names for reopen_count / resolution timestamps
    (defensive fallbacks below emit "Not available" if a field is absent).
  - SLACK_BOT_TOKEN_ALERTS bot must be a MEMBER of the ERC channel to read/post in-thread.
"""

from datetime import timedelta
import logging, os, json, re, time

import pendulum
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from utils.slack.slack_config import (
    REDASH_API_KEY, REDASH_BASE_URL,
    SLACK_BOT_TOKEN_ALERTS as SLACK_BOT_TOKEN,
)

logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
CONFIG_QUERY_ID   = None                                   # TODO: id of the "[ERC] config" Redash query
ENV_CHANNEL_OVER  = os.getenv('ERC_SLACK_CHANNEL')         # test-channel override for dry runs
FORCE_TS          = os.getenv('ERC_FORCE_TS')              # Phase A only: process ONLY this parent ts
SKIP_PHASE_B      = os.getenv('ERC_SKIP_PHASE_B') == '1'   # test toggle
WATERMARK_VAR     = 'ERC_LAST_PROCESSED_TS'                # Airflow Variable: last handled parent ts
BRIEF_MARKER      = 'ERC-AUTO-BRIEF'                       # hidden marker in our briefing -> idempotency
DEFAULT_MODEL     = 'gpt-4o-mini'
DEFAULT_TRACK_DAYS = 30
MAX_PER_RUN       = 5                                      # safety cap on new mails handled per tick
INTERNAL_DOMAINS  = ('emergent.sh', 'emergentagent.com', 'emergent.host')
TRINITY_TICKET_URL = 'https://trinity-base.internal.emergent.host/tickets/%s'

BQ_PROJECT  = 'emergent-default'
BQ_DATASET  = 'support'                                    # helper tables live in `support`, never `analytics`
BQ_TABLE    = 'erc_tracked_customers'
BQ_FQN      = '%s.%s.%s' % (BQ_PROJECT, BQ_DATASET, BQ_TABLE)

OPEN_STATUSES   = ('open', 'pending', 'active', 'in_progress', 'reopened', 'needs_review')
CLOSED_STATUSES = ('closed', 'solved', 'resolved', 'done')


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


# ==================== MCP-over-HTTP (Trinity + Overwatch) ====================
# Generalized from cs_sod_counts_dag.TrinityMCP: initialize once, then tools/call. Both services
# expose MCP endpoints reachable with a Bearer service key.

def _mcp_parse(text):
    if text.strip().startswith('{'):
        return json.loads(text)
    for line in text.splitlines():           # SSE framing
        if line.startswith('data:'):
            try:
                return json.loads(line[5:].strip())
            except Exception:
                pass
    raise Exception('unparseable MCP response: %s' % text[:200])


class McpHttp:
    def __init__(self, url, api_key, client_name='erc-assistant'):
        self.url = url
        self.h = {'Authorization': 'Bearer %s' % api_key,
                  'Content-Type': 'application/json',
                  'Accept': 'application/json, text/event-stream'}
        init = requests.post(self.url, headers=self.h, timeout=30, json={
            'jsonrpc': '2.0', 'id': 1, 'method': 'initialize',
            'params': {'protocolVersion': '2024-11-05', 'capabilities': {},
                       'clientInfo': {'name': client_name, 'version': '1'}}})
        sid = init.headers.get('Mcp-Session-Id')
        if sid:
            self.h['Mcp-Session-Id'] = sid
        requests.post(self.url, headers=self.h, timeout=30,
                      json={'jsonrpc': '2.0', 'method': 'notifications/initialized', 'params': {}})

    def tool(self, name, args):
        r = requests.post(self.url, headers=self.h, timeout=60, json={
            'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call',
            'params': {'name': name, 'arguments': args}})
        data = _mcp_parse(r.text)
        if data.get('error'):
            raise Exception('%s error: %s' % (name, data['error']))
        txt = data['result']['content'][0]['text']
        try:
            return json.loads(txt)
        except Exception:
            return {'_text': txt}


# ==================== LLM (internal LiteLLM proxy, OpenAI-compatible) ====================

def llm_chat(proxy_url, api_key, model, system, user, max_tokens=1800, temperature=0.2):
    url = proxy_url.rstrip('/')
    if not url.endswith('/chat/completions'):
        url = url + '/chat/completions'          # LiteLLM accepts /chat/completions and /v1/chat/completions
    body = {'model': model, 'max_tokens': max_tokens, 'temperature': temperature,
            'messages': [{'role': 'system', 'content': system}, {'role': 'user', 'content': user}]}
    r = requests.post(url, headers={'Authorization': 'Bearer %s' % api_key,
                                    'Content-Type': 'application/json'}, json=body, timeout=120)
    r.raise_for_status()
    return r.json()['choices'][0]['message']['content']


# ==================== SLACK ====================

def _slack(method, payload, http='post'):
    url = 'https://slack.com/api/%s' % method
    hdr = {'Authorization': 'Bearer %s' % SLACK_BOT_TOKEN,
           'Content-Type': 'application/json; charset=utf-8'}
    if http == 'get':
        d = requests.get(url, headers=hdr, params=payload, timeout=30).json()
    else:
        d = requests.post(url, headers=hdr, json=payload, timeout=30).json()
    if not d.get('ok'):
        raise Exception('%s failed: %s' % (method, d.get('error')))
    return d


def fetch_new_erc_parents(channel, erc_bot_id, oldest_ts):
    d = _slack('conversations.history',
               {'channel': channel, 'oldest': oldest_ts, 'limit': 50, 'inclusive': 'false'},
               http='get')
    out = [m for m in d.get('messages', [])
           if m.get('bot_id') == erc_bot_id and 'New mail' in (m.get('text') or '')]
    out.sort(key=lambda m: float(m['ts']))
    return out


def already_briefed(channel, thread_ts):
    """True if our auto-brief marker is already present in the thread (idempotency w/o reactions)."""
    try:
        d = _slack('conversations.replies',
                   {'channel': channel, 'ts': thread_ts, 'limit': 50}, http='get')
        return any(BRIEF_MARKER in (m.get('text') or '') for m in d.get('messages', []))
    except Exception as e:
        logger.warning('conversations.replies failed for %s (%s) - assuming not briefed', thread_ts, e)
        return False


def post_reply(channel, thread_ts, text):
    return _slack('chat.postMessage',
                  {'channel': channel, 'thread_ts': thread_ts, 'text': text,
                   'unfurl_links': False, 'unfurl_media': False})['ts']


# ==================== EMAIL EXTRACTION ====================

EMAIL_RE = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}')

def extract_customer_email(text, proxy_url, proxy_key, model):
    cands, seen = [], set()
    for e in EMAIL_RE.findall(text or ''):
        e = e.lower().strip('.,;:>|')
        if e in seen:
            continue
        seen.add(e)
        if any(e.endswith('@' + d) or e.endswith('.' + d) for d in INTERNAL_DOMAINS):
            continue
        cands.append(e)
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0]
    try:
        out = llm_chat(proxy_url, proxy_key, model,
                       'You pick the CUSTOMER email (not an Emergent employee/forwarder). Reply with only the address.',
                       'Candidates: %s\n\nEmail body:\n%s\n\nCustomer email:' % (', '.join(cands), text[:4000]),
                       max_tokens=30, temperature=0)
        pick = (EMAIL_RE.findall(out) or [None])[0]
        return (pick or cands[0]).lower()
    except Exception as e:
        logger.warning('email tie-break failed, using first candidate: %s', e)
        return cands[0]


# ==================== PROMPTS ====================

BRIEF_SYSTEM = """You are the ERC (Escalation Resolution Committee) analyst bot for Emergent, a platform where users build software with AI agents. An escalation email has landed on erc@emergent.sh and been posted to Slack. Produce ONE tight briefing to post as a reply in that thread so leadership grasps the case in 30 seconds.

GROUNDING RULES - non-negotiable:
- Use ONLY the facts in the INPUT (customer facts, ticket timelines, Overwatch RCA/analyses).
- NEVER invent or recompute a number. Every figure is given in `facts` - copy it verbatim. If a value is missing/empty, write "Not available". Do not estimate.
- Distinguish what the CUSTOMER claims from what Emergent VERIFIED (Overwatch RCA = our verified view).
- Synthesize to the CRUX. Do NOT go ticket-by-ticket. Merge everything across all of the user's tickets into one coherent account - collapse duplicates, keep only what matters. Brevity over completeness.
- No marketing tone, no reassurance filler. Internal briefing.

OUTPUT FORMAT - Slack mrkdwn only (single *bold*, > quotes, bullets). Exactly the four sections in the template."""


def build_brief_prompt(facts, open_tickets, similar_tickets, ow_analyses, timelines):
    j = lambda o: json.dumps(o, ensure_ascii=False, default=str)
    return """INPUT
=====
facts (authoritative - copy numbers verbatim):
{facts}

open_tickets:        {open_tickets}
similar_tickets:     {similar_tickets}
overwatch_analyses:  {ow}
ticket_timelines:    {timelines}

=====
Produce the briefing now, in exactly this structure:

*1. Executive Summary*
> 2-4 sentence TL;DR: who this is, what the whole thing is about, and what they are asking for right now. Lead with the current ask and the escalation posture (legal notice, chargeback, refund demand...).

*2. Basic Details*
- *LTV:* {ltv}
- *Region / Geography:* {region} / {geo}
- *Email:* {email}
- *Payment Gateway:* {pg}
- *Open Tickets:* {open_ct}

*3. Reason for Escalation*
- *Total tickets so far:* {total_ct}
- *Reopens:* {reopens}
- *Avg resolution time:* {art}
- *Escalation level:* {level}
- *Why it escalated:* one line - the trigger that pushed this to ERC (from the RCA/timeline).

*4. Background*  (synthesize to the crux across ALL tickets - not per-ticket)

> *a. What the user is raising / asking for*
> - the core issue(s) and the specific ask (refund amount, credits, GST invoice, completion, etc.)

> *b. What we did to resolve it*
> - verified actions from our side (fixes shipped, credits issued, POC assigned, investigations run), grounded in the Overwatch RCA and the ticket timeline.

> *c. Our limitations conveyed to the user*
> - the constraints we are up against - derived STRICTLY from what the Overwatch RCA states AND what humans actually communicated in the ticket timeline. State only limitations grounded in those two sources.""".format(
        facts=j(facts), open_tickets=j(open_tickets), similar_tickets=j(similar_tickets),
        ow=j(ow_analyses), timelines=j(timelines),
        ltv=facts.get('ltv_usd', 'Not available'), region=facts.get('region', 'Not available'),
        geo=facts.get('geography', 'Not available'), email=facts.get('email', 'Not available'),
        pg=facts.get('payment_gateway', 'Not available'), open_ct=facts.get('open_ticket_count', 'Not available'),
        total_ct=facts.get('total_ticket_count', 'Not available'), reopens=facts.get('reopen_count', 'Not available'),
        art=facts.get('avg_resolution_time', 'Not available'), level=facts.get('current_escalation_level', 'Not available'))


# ==================== CONTEXT ASSEMBLY (Trinity + Overwatch) ====================

def _first(d, *keys, default=None):
    for k in keys:
        v = d.get(k) if isinstance(d, dict) else None
        if v not in (None, '', []):
            return v
    return default

def _rows(resp):
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for k in ('items', 'tickets', 'analyses', 'results', 'data', 'rows'):
            if isinstance(resp.get(k), list):
                return resp[k]
    return []

def _norm_status(s):
    s = (s or '').lower()
    if s in CLOSED_STATUSES:
        return 'closed'
    if s in OPEN_STATUSES:
        return 'open'
    return s or 'unknown'

def _is_open(t):
    return _norm_status(_first(t, 'status', default='')) != 'closed'

def _ticket_id(t):
    return _first(t, 'id', '_id', 'ticket_id')

def _avg_resolution(closed):
    secs = []
    for t in closed:
        c0 = _first(t, 'created_at', 'createdAt', 'created')
        c1 = _first(t, 'closed_at', 'resolved_at', 'closedAt', 'updated_at', 'updatedAt')
        try:
            if c0 and c1:
                secs.append((pendulum.parse(str(c1)) - pendulum.parse(str(c0))).total_seconds())
        except Exception:
            pass
    if not secs:
        return 'Not available'
    h = (sum(secs) / len(secs)) / 3600.0
    return '%.1f h' % h if h < 48 else '%.1f d' % (h / 24.0)

def _ow_ltv(ow):
    """Fallback LTV from Overwatch RCA text when Trinity has no LTV (Trinity doesn't carry LTV;
    the Oracle RCA reliably states it, e.g. 'LTV USD 1,277.54' or '~USD $1,277.54')."""
    pats = [r'LTV[^\d$]{0,14}\$?\s*(?:USD\s*)?\$?\s*([\d,]+\.?\d*)',
            r'USD\s*\$?\s*([\d,]+\.\d{2})', r'\$\s*([\d,]+\.\d{2})\s*USD']
    for a in ow:
        blob = json.dumps(a, default=str)
        for p in pats:
            m = re.search(p, blob, re.I)
            if m:
                return '$%s (from Overwatch RCA)' % m.group(1)
    return None

def _payment_gateway(customer, region, ow):
    blob = (json.dumps(customer, default=str) + json.dumps(ow, default=str)).lower()
    for pg in ('razorpay', 'stripe', 'paddle'):
        if pg in blob:
            return pg.capitalize()
    if (region or '').lower() in ('india', 'in'):
        return 'Razorpay (inferred from region)'
    return 'Not available'

def fetch_all_tickets(trin, email):
    tickets, cursor = [], None
    for _ in range(6):
        args = {'email': email, 'limit': 50}
        if cursor:
            args['cursor'] = cursor
        resp = trin.tool('list_tickets', args)
        tickets.extend(_rows(resp))
        cursor = resp.get('next_cursor') if isinstance(resp, dict) else None
        if not cursor:
            break
    return tickets

def slim_ticket(t):
    return {'id': _ticket_id(t), 'subject': _first(t, 'subject', 'title'),
            'status': _first(t, 'status'), 'level': _first(t, 'level', 'escalation_level'),
            'created_at': _first(t, 'created_at', 'createdAt'), 'tags': _first(t, 'tags'),
            'last_message': _first(t, 'last_message', 'preview')}

def assemble_context(email, trin, over):
    customer = {}
    try:
        rows = _rows(trin.tool('get_customer', {'email': email}))
        customer = rows[0] if rows else {}
    except Exception as e:
        logger.warning('get_customer failed: %s', e)

    tickets = []
    try:
        tickets = fetch_all_tickets(trin, email)
    except Exception as e:
        logger.warning('list_tickets failed: %s', e)

    open_tickets = [t for t in tickets if _is_open(t)]
    closed_tickets = [t for t in tickets if not _is_open(t)]

    timelines = []
    for t in open_tickets[:4]:
        tid = _ticket_id(t)
        if not tid:
            continue
        try:
            tl = trin.tool('get_ticket_messages', {'ticket_id': tid, 'limit': 40})
            timelines.append({'ticket_id': tid, 'events': _rows(tl) or tl})
        except Exception as e:
            logger.warning('get_ticket_messages %s failed: %s', tid, e)

    ow_analyses = []
    try:
        ow_analyses = _rows(over.tool('list_ticket_analyses', {'email': email, 'limit': 10}))
    except Exception as e:
        logger.warning('overwatch list_ticket_analyses failed: %s', e)

    region = _first(customer, 'region', 'country', 'geo', default='Not available')
    facts = {
        'email': email,
        'ltv_usd': _first(customer, 'ltv_usd', 'ltv', 'lifetime_value_usd', default=(_ow_ltv(ow_analyses) or 'Not available')),
        'region': region,
        'geography': _first(customer, 'geography', 'city', 'state', 'country', default=region),
        'payment_gateway': _payment_gateway(customer, region, ow_analyses),
        'open_ticket_count': len(open_tickets),
        'total_ticket_count': len(tickets),
        'reopen_count': _first(customer, 'reopen_count',
                               default=(sum(int(_first(t, 'reopen_count', 'reopens', default=0) or 0) for t in tickets) or 'Not available')),
        'avg_resolution_time': _avg_resolution(closed_tickets),
        'current_escalation_level': _first((open_tickets or tickets or [{}])[0], 'level', 'escalation_level', default='Not available'),
    }
    return (facts, [slim_ticket(t) for t in open_tickets], [slim_ticket(t) for t in closed_tickets[:8]],
            ow_analyses, timelines, tickets)


# ==================== BIGQUERY TRACKER STATE ====================

def _bq():
    from google.cloud import bigquery
    return bigquery.Client(project=BQ_PROJECT)

def bq_ensure_table(client):
    ddl = """CREATE TABLE IF NOT EXISTS `{fqn}` (
      email STRING NOT NULL,
      channel_id STRING,
      thread_ts STRING,
      known_tickets STRING,          -- JSON: {{ticket_id: normalized_status}}
      registered_at TIMESTAMP,
      expires_at TIMESTAMP,
      last_checked_at TIMESTAMP,
      active BOOL
    )""".format(fqn=BQ_FQN)
    client.query(ddl).result()

def bq_register(client, email, channel, thread_ts, baseline, track_days):
    """Upsert a tracked customer. On first insert, seed known_tickets with the current baseline so we
    only alert on tickets that appear AFTER registration. On re-escalation, refresh thread + expiry."""
    from google.cloud import bigquery
    q = """MERGE `{fqn}` T
    USING (SELECT @email AS email) S ON T.email = S.email
    WHEN MATCHED THEN UPDATE SET
      channel_id=@channel, thread_ts=@thread_ts,
      expires_at=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @days DAY), active=TRUE
    WHEN NOT MATCHED THEN INSERT (email, channel_id, thread_ts, known_tickets, registered_at, expires_at, last_checked_at, active)
      VALUES (@email, @channel, @thread_ts, @baseline, CURRENT_TIMESTAMP(),
              TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @days DAY), CURRENT_TIMESTAMP(), TRUE)
    """.format(fqn=BQ_FQN)
    client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter('email', 'STRING', email),
        bigquery.ScalarQueryParameter('channel', 'STRING', channel),
        bigquery.ScalarQueryParameter('thread_ts', 'STRING', thread_ts),
        bigquery.ScalarQueryParameter('baseline', 'STRING', json.dumps(baseline)),
        bigquery.ScalarQueryParameter('days', 'INT64', int(track_days)),
    ])).result()

def bq_active_customers(client):
    q = "SELECT email, channel_id, thread_ts, known_tickets FROM `%s` WHERE active AND expires_at > CURRENT_TIMESTAMP()" % BQ_FQN
    return [dict(r) for r in client.query(q).result()]

def bq_update_known(client, email, known, deactivate=False):
    from google.cloud import bigquery
    q = """UPDATE `{fqn}` SET known_tickets=@known, last_checked_at=CURRENT_TIMESTAMP(), active=@active
           WHERE email=@email""".format(fqn=BQ_FQN)
    client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter('known', 'STRING', json.dumps(known)),
        bigquery.ScalarQueryParameter('active', 'BOOL', not deactivate),
        bigquery.ScalarQueryParameter('email', 'STRING', email),
    ])).result()

def bq_expire_stale(client):
    client.query("UPDATE `%s` SET active=FALSE WHERE active AND expires_at <= CURRENT_TIMESTAMP()" % BQ_FQN).result()


# ==================== PHASE A: new-mail briefing ====================

def process_new_mail(m, channel, trin, over, cfg, bq):
    ts = m['ts']
    if already_briefed(channel, ts):
        logger.info('ERC %s: already briefed - skip', ts)
        return
    proxy_url, proxy_key = cfg['llm_proxy_url'], cfg['llm_proxy_api_key']
    model = cfg.get('llm_model') or DEFAULT_MODEL

    email = extract_customer_email(m.get('text', ''), proxy_url, proxy_key, model)
    if not email:
        logger.warning('ERC %s: no customer email found - skip', ts)
        return
    logger.info('ERC %s: customer=%s', ts, email)

    facts, open_t, sim_t, ow, tl, all_tickets = assemble_context(email, trin, over)
    briefing = llm_chat(proxy_url, proxy_key, model, BRIEF_SYSTEM,
                        build_brief_prompt(facts, open_t, sim_t, ow, tl), max_tokens=1800)
    post_reply(channel, ts, '%s\n\n_%s_' % (briefing, BRIEF_MARKER))

    # register / refresh 30-day tracker with the current tickets as baseline
    baseline = {tid: _norm_status(_first(t, 'status')) for t in all_tickets if (tid := _ticket_id(t))}
    try:
        bq_register(bq, email, channel, ts, baseline, cfg.get('track_days') or DEFAULT_TRACK_DAYS)
        logger.info('ERC %s: registered %s for tracking (%d baseline tickets)', ts, email, len(baseline))
    except Exception as e:
        logger.exception('ERC %s: tracker registration failed (briefing already posted): %s', ts, e)


# ==================== PHASE B: 30-day ticket tracker ====================

def ticket_blurb(proxy_url, proxy_key, model, kind, ticket, ow_analyses):
    """One-line, grounded description of a new/reopened ticket for leadership."""
    ctx = {'ticket': slim_ticket(ticket),
           'overwatch': next((a for a in ow_analyses if _ticket_id(a) == _ticket_id(ticket)), None)}
    try:
        return llm_chat(proxy_url, proxy_key, model,
                        'Write ONE short line (max 20 words) describing what this support ticket is about, for a leadership Slack alert. No preamble, just the line. Ground it only in the data given.',
                        '%s ticket:\n%s' % (kind, json.dumps(ctx, ensure_ascii=False, default=str)[:3000]),
                        max_tokens=60, temperature=0.2).strip().replace('\n', ' ')
    except Exception as e:
        logger.warning('blurb failed: %s', e)
        return _first(ticket, 'subject', 'title', default='(no subject)')

def run_tracker(channel_default, trin, over, cfg, bq):
    proxy_url, proxy_key = cfg['llm_proxy_url'], cfg['llm_proxy_api_key']
    model = cfg.get('llm_model') or DEFAULT_MODEL
    bq_expire_stale(bq)
    customers = bq_active_customers(bq)
    logger.info('Tracker: %d active customers', len(customers))

    for c in customers:
        email = c['email']
        thread_ts = c['thread_ts']
        channel = c.get('channel_id') or channel_default
        try:
            known = json.loads(c.get('known_tickets') or '{}')
        except Exception:
            known = {}
        try:
            current = fetch_all_tickets(trin, email)
        except Exception as e:
            logger.warning('tracker list_tickets(%s) failed: %s', email, e)
            continue

        ow = None
        events = []       # (kind, ticket)
        cur_map = {}
        for t in current:
            tid = _ticket_id(t)
            if not tid:
                continue
            st = _norm_status(_first(t, 'status'))
            cur_map[tid] = st
            prev = known.get(tid)
            if prev is None:
                events.append(('New ticket raised', t))
            elif prev == 'closed' and st == 'open':
                events.append(('Ticket reopened', t))

        if events:
            try:
                ow = _rows(over.tool('list_ticket_analyses', {'email': email, 'limit': 10}))
            except Exception:
                ow = []
        for kind, t in events:
            tid = _ticket_id(t)
            emoji = '🆕' if kind.startswith('New') else '🔄'
            link = TRINITY_TICKET_URL % tid
            blurb = ticket_blurb(proxy_url, proxy_key, model, kind, t, ow)
            msg = '%s *%s:* <%s|#%s> — %s' % (emoji, kind, link, str(tid)[-6:], blurb)
            try:
                post_reply(channel, thread_ts, msg)
                logger.info('Tracker %s: posted %s %s', email, kind, tid)
            except Exception as e:
                logger.warning('tracker post failed (%s %s): %s', email, tid, e)

        # fold current state into baseline (so we don't re-alert next tick)
        merged = dict(known)
        merged.update(cur_map)
        try:
            bq_update_known(bq, email, merged)
        except Exception as e:
            logger.warning('tracker state update failed (%s): %s', email, e)


# ==================== MAIN TASK ====================

def run_erc(**context):
    if not CONFIG_QUERY_ID:
        raise Exception('CONFIG_QUERY_ID not set - create the [ERC] config Redash query first')
    cfg = redash_run(CONFIG_QUERY_ID)[0]
    channel = ENV_CHANNEL_OVER or cfg['channel_id']
    erc_bot = cfg['erc_bot_id']
    trin = McpHttp(cfg['trinity_mcp_url'], cfg['trinity_api_key'])
    over = McpHttp(cfg['overwatch_mcp_url'], cfg['overwatch_api_key'])
    bq = _bq()
    bq_ensure_table(bq)

    # ---- Phase A: new-mail briefings ----
    if FORCE_TS:
        d = _slack('conversations.history',
                   {'channel': channel, 'latest': FORCE_TS, 'oldest': FORCE_TS,
                    'inclusive': 'true', 'limit': 1}, http='get')
        parents = d.get('messages', [])
    else:
        watermark = Variable.get(WATERMARK_VAR, default_var='0')
        parents = fetch_new_erc_parents(channel, erc_bot, watermark)[:MAX_PER_RUN]

    max_ts = None
    for m in parents:
        try:
            process_new_mail(m, channel, trin, over, cfg, bq)
        except Exception as e:
            logger.exception('ERC %s: FAILED (%s)', m.get('ts'), e)
        max_ts = m['ts']
    if not FORCE_TS and max_ts:
        Variable.set(WATERMARK_VAR, max_ts)

    # ---- Phase B: 30-day tracker ----
    if not SKIP_PHASE_B:
        try:
            run_tracker(channel, trin, over, cfg, bq)
        except Exception as e:
            logger.exception('Tracker phase FAILED: %s', e)


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
    'erc_escalation_assistant_slack',
    default_args=default_args,
    description='ERC: post a customer briefing on new erc@ mail, then track new/reopened tickets for 30 days (Trinity + Overwatch RCA -> gpt-4o-mini via LiteLLM proxy)',
    schedule_interval='*/5 * * * *',    # poll every 5 min IST
    catchup=False,
    max_active_runs=1,                  # no overlapping polls
    is_paused_upon_creation=True,       # posts to a sensitive channel; unpause after validation
    tags=['slack', 'trinity', 'overwatch', 'erc', 'cs_team'],
)

PythonOperator(task_id='run_erc', python_callable=run_erc, dag=dag)
