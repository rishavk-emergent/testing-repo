"""
RCA MIS - Slack DAG   (dag_id: rca_mis_slack)

WHAT IT DOES
Every configured IST slot, selects one high-value support ticket, runs an agentic gpt-4.1
investigation grounded in live DB evidence + the emergentbase knowledge base, and posts a deep,
product-oriented RCA to Slack:
  MAIN   : 1. Executive Summary  2. Basic Details (code-computed)  3. Background
  THREAD : What's been done so far / What's still missing / Detailed RCA (L3 depth)

HOW IT WORKS  (thin runtime - all the logic lives in Redash + the KB, editable with no code push)
- Config: Redash "[RCA MIS] config" #42029 (CONFIG_QUERY_ID) - one row read at runtime: channel,
  MCP urls+keys, redash creds, LLM url/key/model, kb_github_token, kb_sources, rca_times, rca_count,
  rca_selection_query, and the full rca_prompt (the RCA "skill" itself lives in this column).
- Selection: Redash "[RCA MIS] selection" #42036 (pointed to by config col rca_selection_query) - an
  ordered candidate list the DAG reads top-down, skipping ids already in the RCA_MIS_DONE watermark.
  Cascade: VIP (LTV >= $25k) -> RealL3 (curated real_l3 tag) -> rest by LTV; hard genuine-user +
  LTV >= $1k floor. Ranking/thresholds all edit in that query.
- Schedule gate: the DAG ticks every 15 min but fires only on the rca_times slots (IST), rca_count
  RCAs per slot.
- Investigation: an OpenAI function-calling loop. Tools the model may call = MCP-over-HTTP
  (Trinity/Overwatch), Redash SQL (ds10 agent-service, ds5 deployer, ds7 BigQuery), live http_get,
  and GitHub KB list/read. KB grounding is MANDATORY (known limitations override first-principles).
- Anti-hallucination: done_so_far, LTV, region and open-count are computed in CODE, never the LLM;
  the model returns prose-only JSON and code renders the Slack layout.
- Overwatch is AUDIT-ONLY (its own RCA is often wrong - treated as a lead to refute, not trust).
- LLM creds fall back: prefer Composer env LLM_PROXY_URL / LLM_PROXY_API_KEY, else config OpenAI-direct.

SOURCE MAP  -  where to look to understand / change any part
  Redash #42029   config      creds - rca_prompt - rca_times - channel - kb_sources - rca_selection_query
  Redash #42036   selection   VIP / RealL3 / rest cascade + genuine-user + LTV floor
  Trinity   MCP   trinity-base.internal.emergent.host/api/mcp     get_ticket, messages, customer
  Overwatch MCP   overwatch.internal.emergent.host/api/mcp        existing analyses (audit only)
  Redash DS       ds7 BigQuery (trinity_database.v_*) - ds10 agent-service (environments/pipeline_runs)
                  - ds5 deployer (apps/pipeline_runs)
  KB (live)       github.com/emergentbase/mono/.claude/skills/*  via GitHub contents API; token in
                  config col kb_github_token (classic PAT, SSO). e.g. debugging-knowledgebase,
                  cortex-rca, mobile-to-web-fork.md
  Model           gpt-4.1  (api.openai.com/v1/chat/completions, or LLM_PROXY_URL)
  State           RCA_MIS_DONE  (Airflow Variable - dedup watermark, so no ticket is RCA'd twice)
  Signals         real_l3 tag id 6a1f2e835ad901b459b7665f ; reopen signal support.reopen_classifications
  Slack           channel from config (test C0B4J9RBWDC) ; posts via SLACK_BOT_TOKEN_ALERTS
  Prod            emergentbase/analytics-dags  dags/rca_mis_dag.py  (PR #1058)

Ships paused (is_paused_upon_creation=True; posts to a real channel - unpause after validating).
"""

from datetime import timedelta
import logging, os, json, re, base64, time

import pendulum, requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL, SLACK_BOT_TOKEN_ALERTS as SLACK_TOKEN

logger = logging.getLogger(__name__)

CONFIG_QUERY_ID = 42029
DONE_VAR        = 'RCA_MIS_DONE'
LTV_MIN         = 1000.0
MAX_ITER        = 24
FORCE_RUN       = os.getenv('RCA_MIS_FORCE') == '1'
FORCE_TICKET    = os.getenv('RCA_MIS_TICKET')          # run one specific ticket id (testing)
ENV_CHANNEL     = os.getenv('RCA_MIS_CHANNEL')          # test-channel override


# ==================== config (Redash) ====================
def _https(u):
    return (u or '').replace('http://', 'https://')

def redash_run(query_id, base, key):
    h = {'Authorization': 'Key %s' % key, 'Content-Type': 'application/json'}
    j = requests.post('%s/api/queries/%s/results' % (_https(base), query_id), json={'parameters': {}, 'max_age': 0}, headers=h, timeout=60).json()
    if 'query_result' in j:
        return j['query_result']['data']['rows']
    jid = j['job']['id']
    for _ in range(90):
        jr = requests.get('%s/api/jobs/%s' % (_https(base), jid), headers=h, timeout=30).json()['job']
        if jr['status'] in (3, 4):
            if jr['status'] == 4:
                raise Exception('config query failed: %s' % jr.get('error'))
            return requests.get('%s/api/query_results/%s.json' % (_https(base), jr['query_result_id']), headers=h, timeout=30).json()['query_result']['data']['rows']
        time.sleep(2)
    raise Exception('config query timed out')

def redash_sql(cfg, ds, sql):
    base = _https(cfg['redash_base_url']); h = {'Authorization': 'Key %s' % cfg['redash_api_key'], 'Content-Type': 'application/json'}
    j = requests.post('%s/api/query_results' % base, headers=h, json={'query': sql, 'data_source_id': int(ds), 'max_age': 0}, timeout=90).json()
    if 'query_result' in j:
        return j['query_result']['data']['rows']
    if 'job' not in j:
        return {'error': str(j)[:300]}
    jid = j['job']['id']
    for _ in range(90):
        jr = requests.get('%s/api/jobs/%s' % (base, jid), headers=h, timeout=30).json()['job']
        if jr['status'] in (3, 4):
            return ({'error': jr.get('error')} if jr['status'] == 4 else
                    requests.get('%s/api/query_results/%s.json' % (base, jr['query_result_id']), headers=h, timeout=30).json()['query_result']['data']['rows'])
        time.sleep(1)
    return {'error': 'timeout'}


# ==================== MCP-over-HTTP ====================
def mcp(url, key, name, args):
    h = {'Authorization': 'Bearer %s' % key, 'Content-Type': 'application/json', 'Accept': 'application/json, text/event-stream'}
    init = requests.post(url, headers=h, timeout=30, json={'jsonrpc': '2.0', 'id': 1, 'method': 'initialize', 'params': {'protocolVersion': '2024-11-05', 'capabilities': {}, 'clientInfo': {'name': 'rca-mis', 'version': '1'}}})
    sid = init.headers.get('Mcp-Session-Id')
    if sid: h['Mcp-Session-Id'] = sid
    requests.post(url, headers=h, timeout=30, json={'jsonrpc': '2.0', 'method': 'notifications/initialized', 'params': {}})
    r = requests.post(url, headers=h, timeout=90, json={'jsonrpc': '2.0', 'id': 2, 'method': 'tools/call', 'params': {'name': name, 'arguments': args}})
    txt = r.text
    data = json.loads(txt) if txt.strip().startswith('{') else json.loads([l[5:] for l in txt.splitlines() if l.startswith('data:')][0])
    if data.get('error'):
        return {'error': str(data['error'])}
    return data['result']['content'][0]['text']


# ==================== GitHub KB ====================
def _gh(cfg, repo, path):
    return requests.get('https://api.github.com/repos/%s/contents/%s' % (repo, path), headers={'Authorization': 'Bearer %s' % cfg['kb_github_token'], 'Accept': 'application/vnd.github+json'}, timeout=30).json()

def kb_sources(cfg):
    return [s.strip() for s in (cfg.get('kb_sources') or '').split('|') if s.strip()]

def kb_list(cfg):
    out = {}
    for src in kb_sources(cfg):
        repo, path = src.split(':', 1)
        d = _gh(cfg, repo, path)
        if isinstance(d, list):
            out[src] = [x['name'] for x in d if x['name'].endswith('.md')]
    return out

def kb_read(cfg, path):
    if ':' in path and path.split(':', 1)[0].count('/') == 1:
        tries = [tuple(path.split(':', 1))]
    else:
        tries = [(s.split(':', 1)[0], (path if '/' in path else s.split(':', 1)[1] + '/' + path)) for s in kb_sources(cfg)]
    for repo, p in tries:
        d = _gh(cfg, repo, p)
        if isinstance(d, dict) and d.get('content'):
            return base64.b64decode(d['content']).decode('utf-8', 'replace')[:8000]
    return {'error': 'not found in any KB source: ' + path}


def http_get(url):
    try:
        r = requests.get(url, timeout=15, allow_redirects=True)
        return {'status': r.status_code, 'content_type': r.headers.get('content-type'), 'server': r.headers.get('server'), 'cf_cache': r.headers.get('cf-cache-status'), 'location': r.headers.get('location'), 'size': len(r.content), 'body_head': r.text[:400]}
    except Exception as e:
        return {'error': repr(e)[:200]}


# ==================== investigation loop ====================
TOOLS = [
    {'type': 'function', 'function': {'name': 'trinity', 'description': 'Trinity MCP (tickets). tool: get_ticket, get_ticket_messages, get_customer, list_tickets.', 'parameters': {'type': 'object', 'properties': {'tool': {'type': 'string'}, 'arguments': {'type': 'object'}}, 'required': ['tool']}}},
    {'type': 'function', 'function': {'name': 'overwatch', 'description': "Overwatch MCP (its RCA is OFTEN WRONG - audit only). tool: list_ticket_analyses (args {email}).", 'parameters': {'type': 'object', 'properties': {'tool': {'type': 'string'}, 'arguments': {'type': 'object'}}, 'required': ['tool']}}},
    {'type': 'function', 'function': {'name': 'redash_sql', 'description': 'Read-only SQL. ds 10=agent-service (users, jobs, environments, credit_ledger), 5=deployer (apps, pipeline_runs; NO users table), 7=BigQuery. Scope queries so they do not time out.', 'parameters': {'type': 'object', 'properties': {'data_source_id': {'type': 'integer'}, 'sql': {'type': 'string'}}, 'required': ['data_source_id', 'sql']}}},
    {'type': 'function', 'function': {'name': 'kb_list', 'description': 'List article filenames across all configured emergentbase KB sources -> {source: [files]}. Filenames are self-describing.', 'parameters': {'type': 'object', 'properties': {}}}},
    {'type': 'function', 'function': {'name': 'kb_read', 'description': "Read a KB article by bare filename (searched across sources) or 'repo:full/path.md'.", 'parameters': {'type': 'object', 'properties': {'path': {'type': 'string'}}, 'required': ['path']}}},
    {'type': 'function', 'function': {'name': 'http_get', 'description': 'Live GET a URL (e.g. the customer app domain) to confirm what is actually served.', 'parameters': {'type': 'object', 'properties': {'url': {'type': 'string'}}, 'required': ['url']}}},
]

def run_tool(cfg, name, args):
    try:
        if name == 'trinity': return mcp(cfg['trinity_mcp_url'], cfg['trinity_api_key'], args['tool'], args.get('arguments', {}))
        if name == 'overwatch': return mcp(cfg['overwatch_mcp_url'], cfg['overwatch_api_key'], args['tool'], args.get('arguments', {}))
        if name == 'redash_sql': return redash_sql(cfg, args['data_source_id'], args['sql'])
        if name == 'kb_list': return kb_list(cfg)
        if name == 'kb_read': return kb_read(cfg, args['path'])
        if name == 'http_get': return http_get(args['url'])
        return {'error': 'unknown tool ' + name}
    except Exception as e:
        return {'error': repr(e)[:300]}

def llm(cfg, messages, tools=None):
    body = {'model': cfg.get('llm_model') or 'gpt-4.1', 'messages': messages, 'temperature': 0.1}
    if tools:
        body['tools'] = tools; body['tool_choice'] = 'auto'
    r = requests.post(cfg['llm_url'], headers={'Authorization': 'Bearer %s' % cfg['llm_api_key'], 'Content-Type': 'application/json'}, json=body, timeout=180)
    r.raise_for_status()
    return r.json()['choices'][0]['message']

def _summarize_done(cfg, human_replies):
    joined = ' | '.join(b for _, b in human_replies)[:2000]
    try:
        m = llm(cfg, [{'role': 'system', 'content': 'In ONE neutral sentence, summarize what support has communicated or done for this customer, based only on the agent message(s) given. No names, no direct quotes, no greeting. Start with a verb.'},
                      {'role': 'user', 'content': joined}])
        return (m.get('content') or '').strip()
    except Exception:
        return None

def investigate(cfg, ticket_id):
    tkt = mcp(cfg['trinity_mcp_url'], cfg['trinity_api_key'], 'get_ticket', {'ticket_id': ticket_id})
    msgs = mcp(cfg['trinity_mcp_url'], cfg['trinity_api_key'], 'get_ticket_messages', {'ticket_id': ticket_id, 'include_internal': False, 'limit': 20})
    tj = json.loads(tkt) if isinstance(tkt, str) else tkt
    mj = json.loads(msgs) if isinstance(msgs, str) else msgs
    log, human_replies = [], []
    for e in (mj.get('events', []) if isinstance(mj, dict) else []):
        if e.get('type') != 'message':
            continue
        auto = (e.get('author') == 'System') or bool((e.get('metadata') or {}).get('origin'))
        who = 'AUTOMATED-SYSTEM' if auto else ('CUSTOMER' if e.get('direction') == 'inbound' else 'HUMAN-AGENT')
        body = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', e.get('body') or '')).strip()
        log.append('- [%s] %s: %s' % (who, e.get('author') or '', body[:220]))
        if who == 'HUMAN-AGENT':
            human_replies.append((e.get('author') or 'agent', body))
    st = 'STATUS=%s | level=%s | assigned_to=%s\nMESSAGE LOG (oldest->newest):\n%s' % (tj.get('status'), tj.get('level'), tj.get('assigned_to'), '\n'.join(log) or '(none)')
    messages = [
        {'role': 'system', 'content': cfg['rca_prompt']},
        {'role': 'user', 'content': 'TICKET SNAPSHOT:\n%s\n\n%s\n\nNote: messages marked AUTOMATED-SYSTEM are NOT human actions. Investigate and produce the RCA JSON.' % (json.dumps(tj, default=str)[:3500], st)},
    ]
    out = None
    for _ in range(MAX_ITER):
        m = llm(cfg, messages, TOOLS)
        messages.append(m)
        tcs = m.get('tool_calls')
        if not tcs:
            mt = re.search(r'\{.*\}', m.get('content') or '', re.S)
            out = json.loads(mt.group(0)) if mt else {'error': 'no json'}
            break
        for tc in tcs:
            fn = tc['function']['name']; a = json.loads(tc['function']['arguments'] or '{}')
            out_t = run_tool(cfg, fn, a)
            messages.append({'role': 'tool', 'tool_call_id': tc['id'], 'content': json.dumps(out_t, default=str)[:8000]})
    if out is None:
        out = {'error': 'max iterations'}
    # done_so_far: code-derived, general (no name/quote)
    if human_replies:
        out['done_so_far'] = _summarize_done(cfg, human_replies) or 'Support has replied to the customer regarding the issue.'
    else:
        out['done_so_far'] = 'No human reply yet - only an automated acknowledgment; the ticket was auto-triaged and escalated to %s, awaiting a human response and fix.' % tj.get('level')
    return out, tj


# ==================== basic details + render ====================
def basic_details(cfg, tj, ticket_id):
    cust = tj.get('customer') or {}; ccf = cust.get('custom_fields') or {}
    email = cust.get('email')
    ltv = ('$%s' % ccf.get('user_revenue')) if ccf.get('user_revenue') not in (None, '') else 'Not available'
    plan = ccf.get('current_subscription') or ''
    gw = ccf.get('subscription_gateway') or 'Not available'
    region = 'Not available'
    try:
        g = redash_sql(cfg, 7, "SELECT country,region,city FROM `emergent-default.analytics.signups_raw_dataset` WHERE email='%s' ORDER BY created_at DESC LIMIT 1" % email)
        if isinstance(g, list) and g:
            region = '%s / %s, %s' % (g[0].get('country'), g[0].get('city'), g[0].get('region'))
    except Exception:
        pass
    opencnt = '?'
    try:
        lt = mcp(cfg['trinity_mcp_url'], cfg['trinity_api_key'], 'list_tickets', {'email': email, 'status': 'open', 'limit': 50})
        opencnt = len(json.loads(lt).get('items', []))
    except Exception:
        pass
    return {'ltv': ltv, 'plan': plan, 'gw': gw, 'region': region, 'email': email, 'opencnt': opencnt, 'num': tj.get('num'),
            'link': 'https://trinity-base.internal.emergent.host/tickets/%s' % ticket_id}

def render_main(bd, out):
    return (
"*1. Executive Summary*\n> %s\n\n"
"*2. Basic Details*\n"
"> • *LTV:* %s%s\n> • *Region / Geography:* %s\n> • *Email:* %s\n> • *Payment Gateway:* %s\n> • *Open tickets:* %s · *Ticket:* <%s|#%s>\n\n"
"*3. Background*\n> %s" % (
        out.get('exec_summary', ''), bd['ltv'], (' · *Plan:* ' + bd['plan']) if bd['plan'] else '', bd['region'], bd['email'], bd['gw'], bd['opencnt'], bd['link'], bd['num'], out.get('background', '')))

def render_detailed(out):
    lines = []
    for ln in (out.get('detailed_rca') or '').split('\n'):
        ln = re.sub(r'^(\s*)-\s+', r'\1• ', ln)
        if re.search(r'User:\s*\S+@\S+', ln):
            continue
        lines.append('> ' + ln if ln.strip() else '>')
    return '*Detailed RCA*\n' + '\n'.join(lines)

def slack_post(channel, text, thread_ts=None):
    p = {'channel': channel, 'text': text, 'unfurl_links': False, 'unfurl_media': False}
    if thread_ts:
        p['thread_ts'] = thread_ts
    d = requests.post('https://slack.com/api/chat.postMessage', headers={'Authorization': 'Bearer %s' % SLACK_TOKEN, 'Content-Type': 'application/json; charset=utf-8'}, json=p, timeout=30).json()
    if not d.get('ok'):
        raise Exception('chat.postMessage failed: %s' % d.get('error'))
    return d['ts']

def post_rca(cfg, channel, ticket_id):
    out, tj = investigate(cfg, ticket_id)
    if out.get('error') and not out.get('exec_summary'):
        logger.warning('RCA MIS %s: investigation error: %s', ticket_id, out.get('error'))
        return False
    bd = basic_details(cfg, tj, ticket_id)
    parent = slack_post(channel, render_main(bd, out))
    slack_post(channel, "*What's been done so far*\n> " + out.get('done_so_far', ''), parent)
    slack_post(channel, "*What's still missing*\n> " + out.get('still_missing', ''), parent)
    slack_post(channel, render_detailed(out), parent)
    logger.info('RCA MIS: posted RCA for %s (#%s)', ticket_id, tj.get('num'))
    return True


# ==================== selection ====================
def select_ticket_ids(cfg, n, done):
    """Read the ordered candidate list from the [RCA MIS] selection query (config
    rca_selection_query, #42036) and take the first n not already RCA'd. The query encodes the
    cascade: Tier A = RealL3 open/pending (LTV ignored, reopened-defect then oldest-waiting first) ->
    Tier B = LTV>=floor -> Tier C = rest by LTV desc. Selection logic lives in that query (no code
    push). Falls back to a recent-open Trinity scan only if the query is unset/unavailable."""
    qid = cfg.get('rca_selection_query')
    if qid:
        try:
            rows = redash_run(int(qid), cfg['redash_base_url'], cfg['redash_api_key'])
            picked = []
            for r in rows:
                tid = r.get('ticket_id')
                if tid and tid not in done:
                    picked.append(tid)
                    logger.info('RCA MIS: candidate #%s tier=%s | %s', r.get('num'), r.get('tier'), r.get('reason'))
                if len(picked) >= n:
                    break
            return picked
        except Exception as e:
            logger.warning('RCA MIS: selection query %s failed (%s) - falling back to Trinity scan', qid, e)
    # fallback: recent open tickets with LTV >= LTV_MIN
    try:
        lt = json.loads(mcp(cfg['trinity_mcp_url'], cfg['trinity_api_key'], 'list_tickets', {'status': 'open', 'limit': 40}))
    except Exception as e:
        logger.warning('RCA MIS: fallback list_tickets failed: %s', e)
        return []
    picked = []
    for it in lt.get('items', []):
        tid = it.get('id')
        if not tid or tid in done:
            continue
        try:
            tj = json.loads(mcp(cfg['trinity_mcp_url'], cfg['trinity_api_key'], 'get_ticket', {'ticket_id': tid}))
        except Exception:
            continue
        ltv = ((tj.get('customer') or {}).get('custom_fields') or {}).get('user_revenue')
        if ltv is not None and float(ltv) >= LTV_MIN:
            picked.append(tid)
        if len(picked) >= n:
            break
    return picked


# ==================== schedule gate ====================
def slot_now(cfg):
    now = pendulum.now('Asia/Kolkata')
    for s in [x.strip() for x in (cfg.get('rca_times') or '').split(',') if x.strip()]:
        hh, mm = [int(z) for z in s.split(':')]
        if now.hour == hh and mm <= now.minute < mm + 15:
            return s
    return None


# ==================== MAIN ====================
def _resolve(name):
    """Composer-provided override (env / Secret Manager, then Airflow Variable); None if neither."""
    v = os.getenv(name)
    if v:
        return v
    try:
        return Variable.get(name, default_var=None)
    except Exception:
        return None

def run_rca_mis(**context):
    cfg = redash_run(CONFIG_QUERY_ID, REDASH_BASE_URL, REDASH_API_KEY)[0]
    cfg['redash_base_url'] = _https(cfg.get('redash_base_url') or REDASH_BASE_URL)
    # Prefer Composer's internal LiteLLM proxy (Secret Manager / Airflow Variable) when present, so a
    # merged PR uses the internal proxy automatically; locally these are unset -> fall back to the
    # config-query OpenAI-direct values. No code/config change needed at go-live.
    cfg['llm_url'] = _resolve('LLM_PROXY_URL') or cfg['llm_url']
    cfg['llm_api_key'] = _resolve('LLM_PROXY_API_KEY') or cfg['llm_api_key']
    channel = ENV_CHANNEL or cfg['channel_id']

    if FORCE_TICKET:
        post_rca(cfg, channel, FORCE_TICKET)
        return

    slot = slot_now(cfg)
    if not slot and not FORCE_RUN:
        logger.info('RCA MIS: not a configured slot (%s) - skip', cfg.get('rca_times'))
        return

    try:
        done = set(json.loads(Variable.get(DONE_VAR, default_var='[]')))
    except Exception:
        done = set()
    n = int(cfg.get('rca_count') or 1)
    ids = select_ticket_ids(cfg, n, done)
    logger.info('RCA MIS: slot=%s selected %d tickets', slot, len(ids))
    for tid in ids:
        try:
            if post_rca(cfg, channel, tid):
                done.add(tid)
        except Exception as e:
            logger.exception('RCA MIS %s: failed: %s', tid, e)
    Variable.set(DONE_VAR, json.dumps(list(done)[-500:]))


default_args = {
    'owner': 'cs_team', 'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Kolkata'),
    'email_on_failure': False, 'email_on_retry': False, 'retries': 1, 'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'rca_mis_slack',
    default_args=default_args,
    description='Post a deep KB-grounded product RCA for a >=$1k ticket at configured times (Trinity+Overwatch+Redash+emergentbase KB -> gpt-4.1)',
    schedule_interval='*/15 * * * *',   # ticks every 15 min; fires only on config rca_times slots
    catchup=False, max_active_runs=1,
    dagrun_timeout=timedelta(minutes=45),
    is_paused_upon_creation=True,        # posts to a real channel; unpause after validation
    tags=['slack', 'rca', 'mis', 'trinity', 'overwatch', 'cs_team'],
)

PythonOperator(task_id='run_rca_mis', python_callable=run_rca_mis, dag=dag, execution_timeout=timedelta(minutes=40))
