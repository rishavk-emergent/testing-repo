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
  - Phase A: before briefing, we read the thread and skip if our briefing header is already there
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
  llm_proxy_url, llm_proxy_api_key, llm_model, track_days, oncall_tag
  (all on a single row; read from rows[0])
  oncall_tag: emitted verbatim as the first line of every briefing + tracker post to page on-call.
    Put a Slack mention token (e.g. "<!subteam^S0B8KFV3Y2G>" for @cs_associates) to actually notify;
    editable here with no code change. Empty = no tag line.

STATUS: end-to-end validated on live data (Trinity + Overwatch MCP + gpt-4o-mini). Remaining:
  - SLACK_BOT_TOKEN_ALERTS bot (the "Daily Report" bot) must be a MEMBER of the ERC channel to
    read replies + post in-thread.
  - Config #41566 currently points llm_proxy_* at OpenAI direct (TESTING) — swap to the internal
    LiteLLM proxy for prod, and rotate the test OpenAI key.
  - Phase B tracker needs the Composer BQ service account (creates support.erc_tracked_customers).
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

try:
    from utils.secrets import get_secret          # Composer Secret Manager helper (first_job_quality uses it)
except Exception:                                  # pragma: no cover - local/test fallback
    def get_secret(_k):
        return None

def _env_override(name):
    """Composer-provided value (Secret Manager first, then Airflow Variable); None if neither set."""
    try:
        v = get_secret(name)
        if v:
            return v
    except Exception:
        pass
    return Variable.get(name, default_var=None)

# ==================== CONFIG ====================
CONFIG_QUERY_ID   = 41566                                  # Redash "[ERC] config" (single-row globals)
ENV_CHANNEL_OVER  = os.getenv('ERC_SLACK_CHANNEL')         # test-channel override for dry runs
FORCE_TS          = os.getenv('ERC_FORCE_TS')              # Phase A only: process ONLY this parent ts
SKIP_PHASE_B      = os.getenv('ERC_SKIP_PHASE_B') == '1'   # test toggle
WATERMARK_VAR     = 'ERC_LAST_PROCESSED_TS'                # Airflow Variable: last handled parent ts
BRIEF_SIGNATURE   = '*1. Executive Summary*'               # briefing's own header; detected for idempotency (no visible marker text)
NOTFOUND_MSG      = ':warning: *ERC:* customer email not found in the forwarded mail — skipped.'
NOTFOUND_SIG      = 'customer email not found'             # idempotency signature for the not-found post
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
    """True if our briefing (its Executive Summary header) is already in the thread (idempotency w/o reactions)."""
    try:
        d = _slack('conversations.replies',
                   {'channel': channel, 'ts': thread_ts, 'limit': 50}, http='get')
        return any((BRIEF_SIGNATURE in (m.get('text') or '') or NOTFOUND_SIG in (m.get('text') or ''))
                   for m in d.get('messages', []))
    except Exception as e:
        logger.warning('conversations.replies failed for %s (%s) - assuming not briefed', thread_ts, e)
        return False


def post_reply(channel, thread_ts, text):
    return _slack('chat.postMessage',
                  {'channel': channel, 'thread_ts': thread_ts, 'text': text,
                   'unfurl_links': False, 'unfurl_media': False})['ts']


# ==================== EMAIL EXTRACTION ====================

EMAIL_RE = re.compile(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}')
# "From:" lines in the mail — the forwarded original sender (the customer) is the first EXTERNAL one;
# the top "From:" is the internal person who forwarded the mail to erc@.
FROM_RE = re.compile(r'From:\s*[^<\n]*?<?\s*([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})', re.I)

def _is_internal(e):
    return any(e.endswith('@' + d) or e.endswith('.' + d) for d in INTERNAL_DOMAINS)

def extract_customer_email(text, proxy_url, proxy_key, model):
    text = text or ''
    # ERC = someone forwards a CUSTOMER's mail to erc@. The customer is whose mail was forwarded,
    # i.e. the first EXTERNAL address on a "From:" line (top "From:" is the internal forwarder).
    for e in FROM_RE.findall(text):
        e = e.lower().strip('.,;:>|')
        if not _is_internal(e):
            return e
    # Fallback: any external address in the body (LLM tie-break if several)
    cands, seen = [], set()
    for e in EMAIL_RE.findall(text):
        e = e.lower().strip('.,;:>|')
        if e in seen:
            continue
        seen.add(e)
        if _is_internal(e):
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

BRIEF_SYSTEM = """You are the ERC (Escalation Resolution Committee) analyst bot for Emergent, a platform where users build software with AI agents. An escalation email has landed on erc@emergent.sh. Extract the NARRATIVE for a leadership briefing (the numeric fields are rendered separately by code — you only write prose).

GROUNDING RULES - non-negotiable:
- Use ONLY the INPUT (ticket timelines, Overwatch RCA/analyses). Never invent facts.
- Distinguish what the CUSTOMER claims from what Emergent VERIFIED (Overwatch RCA = our verified view).
- Synthesize to the CRUX across ALL of the user's tickets - do NOT go ticket-by-ticket; collapse duplicates, keep only what matters. Brevity over completeness.
- No marketing tone, no reassurance filler. Internal briefing.

OUTPUT: return ONLY a JSON object (no markdown, no code fences, no prose around it) with these string keys, each a SINGLE line with no newlines:
- "exec_summary": 2-4 sentences - who this is, what the whole thing is about, and the current ask + escalation posture (legal notice, chargeback, refund demand...).
- "why_escalated": one line - the trigger that pushed this to ERC.
- "bg_asking": 1-3 sentences - the core issue(s) and the specific ask (refund amount, credits, GST invoice, completion...).
- "bg_did": 1-3 sentences - verified actions from our side (fixes shipped, credits issued, POC assigned, investigations run), grounded in the RCA/timeline.
- "bg_limits": 1-3 sentences - constraints we are up against, STRICTLY from what the Overwatch RCA states AND what humans actually communicated in the ticket timeline."""


def build_brief_prompt(facts, open_tickets, similar_tickets, ow_analyses, timelines):
    j = lambda o: json.dumps(o, ensure_ascii=False, default=str)
    return ("INPUT\n=====\n"
            "open_tickets:        %s\nsimilar_tickets:     %s\noverwatch_analyses:  %s\nticket_timelines:    %s\n\n"
            "=====\nReturn ONLY the JSON object described above (keys: exec_summary, why_escalated, "
            "bg_asking, bg_did, bg_limits)." % (
                j(open_tickets), j(similar_tickets), j(ow_analyses), j(timelines)))


def _parse_brief_json(text):
    """Extract the JSON object from the model output (tolerates code fences / surrounding text)."""
    m = re.search(r'\{.*\}', text or '', re.S)
    if not m:
        raise ValueError('no JSON object in model output')
    d = json.loads(m.group(0))
    return {k: ' '.join(str(d.get(k, '') or '').split()) for k in
            ('exec_summary', 'why_escalated', 'bg_asking', 'bg_did', 'bg_limits')}


def render_briefing(f, p):
    """Assemble the Slack mrkdwn briefing deterministically: code owns all formatting/numbers,
    the model only supplied the prose in `p`. Guarantees blockquotes/bullets are exactly right."""
    na = 'Not available'
    return "\n".join([
        "*1. Executive Summary*",
        "> " + (p.get('exec_summary') or na),
        "",
        "*2. Basic Details*",
        "> • *LTV:* %s" % f.get('ltv_usd', na),
        "> • *Region / Geography:* %s / %s" % (f.get('region', na), f.get('geography', na)),
        "> • *Email:* %s" % f.get('email', na),
        "> • *Payment Gateway:* %s" % f.get('payment_gateway', na),
        "> • *Open Tickets:* %s" % f.get('open_ticket_count', na),
        "> • *Ticket numbers:* %s" % f.get('open_ticket_links', '—'),
        "",
        "*3. Reason for Escalation*",
        "> • *Total tickets so far:* %s" % f.get('total_ticket_count', na),
        "> • *Reopens:* %s" % f.get('reopen_count', na),
        "> • *Resolution time (active/open-only):* P50 %s · P75 %s" % (f.get('resolution_p50', na), f.get('resolution_p75', na)),
        "> • *Escalation level:* %s" % f.get('current_escalation_level', na),
        "> • *Why it escalated:* %s" % (p.get('why_escalated') or na),
        "",
        "*4. Background*",
        "> *a. What the user is raising / asking for*",
        "> " + (p.get('bg_asking') or na),
        "",
        "> *b. What we did to resolve it*",
        "> " + (p.get('bg_did') or na),
        "",
        "> *c. Our limitations conveyed to the user*",
        "> " + (p.get('bg_limits') or na),
    ])


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

def _fmt_dur(secs):
    h = secs / 3600.0
    return '%.1f h' % h if h < 48 else '%.1f d' % (h / 24.0)

def _pct(vals, p):
    d = sorted(vals)
    if not d:
        return None
    k = (len(d) - 1) * p / 100.0
    f = int(k); c = min(f + 1, len(d) - 1)
    return d[f] + (d[c] - d[f]) * (k - f)

def _resolution_pcts(bq, ticket_ids):
    """P50/P75 of ACTIVE-OPEN resolution time across the customer's CLOSED tickets. 'Active' = time
    in OPEN status only; PENDING and CLOSED intervals are both treated as downtime and excluded
    (so closed->reopen gaps and awaiting-customer pauses don't inflate it). Built from the Trinity
    v_ticket_events status timeline (created_at = first OPEN, walk status_changed, sum OPEN spans)."""
    from google.cloud import bigquery
    ids = [i for i in ticket_ids if i]
    if not ids:
        return {'p50': 'Not available', 'p75': 'Not available'}
    q = """
    WITH ev AS (
      SELECT ticket_id, created_at AS ts, UPPER(JSON_VALUE(new_value)) AS st
      FROM `emergent-default.trinity_database.v_ticket_events`
      WHERE ticket_id IN UNNEST(@ids) AND action='status_changed'
      UNION ALL
      SELECT _id, created_at, 'OPEN' FROM `emergent-default.trinity_database.v_tickets`
      WHERE _id IN UNNEST(@ids)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY _id ORDER BY source_timestamp DESC)=1
    ),
    seq AS (
      SELECT ticket_id, ts, st, LEAD(ts) OVER (PARTITION BY ticket_id ORDER BY ts) AS nxt FROM ev
    ),
    per_ticket AS (
      SELECT ticket_id,
        SUM(IF(st='OPEN' AND nxt IS NOT NULL, TIMESTAMP_DIFF(nxt, ts, SECOND), 0)) AS open_secs,
        ARRAY_AGG(st ORDER BY ts DESC LIMIT 1)[OFFSET(0)] AS final_st
      FROM seq GROUP BY ticket_id
    )
    SELECT open_secs FROM per_ticket WHERE final_st='CLOSED'"""
    try:
        rows = list(bq.query(q, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter('ids', 'STRING', ids)])).result())
        vals = [r.get('open_secs') for r in rows if r.get('open_secs') is not None]
        if not vals:
            return {'p50': 'Not available', 'p75': 'Not available'}
        return {'p50': _fmt_dur(_pct(vals, 50)), 'p75': _fmt_dur(_pct(vals, 75))}
    except Exception as e:
        logger.warning('resolution percentiles failed: %s', e)
        return {'p50': 'Not available', 'p75': 'Not available'}

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

def _signup_geo(bq, email):
    """Region/geography from the customer's IP, already resolved in analytics.signups_raw_dataset
    (country/region/city derived from ip_address). Returns {} if no signup row / on error."""
    from google.cloud import bigquery
    try:
        q = ("SELECT country, region, city FROM `emergent-default.analytics.signups_raw_dataset` "
             "WHERE email=@e ORDER BY created_at DESC LIMIT 1")
        rows = list(bq.query(q, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter('e', 'STRING', email)])).result())
        return {'country': rows[0].get('country'), 'region': rows[0].get('region'),
                'city': rows[0].get('city')} if rows else {}
    except Exception as e:
        logger.warning('signup geo lookup failed for %s: %s', email, e)
        return {}

def _reopen_count(bq, ticket_ids):
    """Reopen count = CLOSED->not-CLOSED status_changed events on the customer's tickets, from the
    Trinity v_ticket_events reopens snapshot (same definition as Polaris reopen feed #36960)."""
    from google.cloud import bigquery
    ids = [i for i in ticket_ids if i]
    if not ids:
        return 'Not available'
    try:
        q = ("SELECT COUNT(*) AS c FROM `emergent-default.trinity_database.v_ticket_events` "
             "WHERE ticket_id IN UNNEST(@ids) AND action='status_changed' "
             "AND JSON_VALUE(old_value)='CLOSED' AND JSON_VALUE(new_value)<>'CLOSED'")
        rows = list(bq.query(q, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter('ids', 'STRING', ids)])).result())
        return rows[0].get('c') if rows else 'Not available'
    except Exception as e:
        logger.warning('reopen count failed: %s', e)
        return 'Not available'

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
    return {'id': _ticket_id(t), 'num': _first(t, 'num'), 'subject': _first(t, 'subject', 'title'),
            'status': _first(t, 'status'), 'level': _first(t, 'level', 'escalation_level'),
            'created_at': _first(t, 'created_at', 'createdAt'), 'tags': _first(t, 'tags'),
            'last_message': _first(t, 'last_message', 'preview')}

def _ticket_link(t):
    return '<%s|#%s>' % (TRINITY_TICKET_URL % _ticket_id(t), _first(t, 'num', 'id'))

def assemble_context(email, trin, over, bq):
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

    # LTV + subscription gateway live ONLY on the full get_ticket snapshot's custom_fields
    # (list_tickets/get_customer don't carry them). Fetch the primary ticket for these.
    ccf = {}
    primary = (open_tickets or tickets or [None])[0]
    if primary and _ticket_id(primary):
        try:
            snap = trin.tool('get_ticket', {'ticket_id': _ticket_id(primary)})
            ccf = (snap.get('customer') or {}).get('custom_fields') or {}
        except Exception as e:
            logger.warning('get_ticket %s failed: %s', _ticket_id(primary), e)

    geo = _signup_geo(bq, email)
    res = _resolution_pcts(bq, [_ticket_id(t) for t in tickets])
    region = geo.get('country') or _first(customer, 'region', 'country', default='Not available')
    geography = ', '.join([x for x in (geo.get('city'), geo.get('region')) if x]) or region
    ltv = ccf.get('user_revenue')
    facts = {
        'email': email,
        'ltv_usd': ('$%s' % ltv) if ltv not in (None, '') else _first(customer, 'ltv_usd', 'ltv', default=(_ow_ltv(ow_analyses) or 'Not available')),
        'region': region,
        'geography': geography,
        'payment_gateway': ccf.get('subscription_gateway') or _payment_gateway(customer, region, ow_analyses),
        'open_ticket_count': len(open_tickets),
        'open_ticket_links': ', '.join(_ticket_link(t) for t in open_tickets if _ticket_id(t)) or '—',
        'total_ticket_count': len(tickets),
        'reopen_count': _reopen_count(bq, [_ticket_id(t) for t in tickets]),
        'resolution_p50': res['p50'],
        'resolution_p75': res['p75'],
        'current_escalation_level': _first((open_tickets or tickets or [{}])[0], 'level', 'escalation_level', default='Not available'),
    }
    return (facts, [slim_ticket(t) for t in open_tickets], [slim_ticket(t) for t in closed_tickets[:8]],
            ow_analyses, timelines, tickets)


# ==================== BIGQUERY TRACKER STATE ====================

def _bq():
    from utils.slack.bigquery_client import get_bigquery_client   # house helper (Composer ADC)
    return get_bigquery_client()

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
        logger.warning('ERC %s: user email not found - posting notice, no briefing/tracking', ts)
        post_reply(channel, ts, NOTFOUND_MSG)
        return
    logger.info('ERC %s: customer=%s', ts, email)

    facts, open_t, sim_t, ow, tl, all_tickets = assemble_context(email, trin, over, bq)
    try:
        parts = _parse_brief_json(llm_chat(proxy_url, proxy_key, model, BRIEF_SYSTEM,
                                           build_brief_prompt(facts, open_t, sim_t, ow, tl), max_tokens=1200))
    except Exception as e:
        logger.exception('ERC %s: briefing JSON parse failed, rendering facts-only: %s', ts, e)
        parts = {}
    tag = (cfg.get('oncall_tag') or '').strip()      # e.g. "<!subteam^S...>" — on-call group ping; editable in Redash
    body = render_briefing(facts, parts)
    post_reply(channel, ts, (tag + '\n' + body) if tag else body)

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
    tag = (cfg.get('oncall_tag') or '').strip()      # same on-call ping as the briefing; editable in Redash
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
            blurb = ticket_blurb(proxy_url, proxy_key, model, kind, t, ow)
            msg = '%s *%s:* %s — %s' % (emoji, kind, _ticket_link(t), blurb)
            if tag:
                msg = tag + '\n' + msg
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

    # Prefer Composer's internal LiteLLM proxy creds (Secret Manager / Airflow Variable) when present,
    # so a merged PR uses the internal proxy automatically. Locally these are unset -> fall back to the
    # config-query values (OpenAI-direct for testing). No code/config change needed at go-live.
    cfg['llm_proxy_url'] = _env_override('LLM_PROXY_URL') or cfg.get('llm_proxy_url')
    cfg['llm_proxy_api_key'] = _env_override('LLM_PROXY_API_KEY') or cfg.get('llm_proxy_api_key')

    # ---- Phase A: new-mail briefings ----
    if FORCE_TS:
        d = _slack('conversations.history',
                   {'channel': channel, 'latest': FORCE_TS, 'oldest': FORCE_TS,
                    'inclusive': 'true', 'limit': 1}, http='get')
        parents = d.get('messages', [])
    else:
        watermark = Variable.get(WATERMARK_VAR, default_var=None)
        if not watermark:
            # First run after deploy: start the clock at NOW so we only brief mails that arrive
            # from here on — never the pre-existing backlog of ERC posts.
            Variable.set(WATERMARK_VAR, repr(pendulum.now('UTC').float_timestamp))
            logger.info('ERC: first run — watermark set to now; ignoring backlog, tracking only new mails')
            parents = []
        else:
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
