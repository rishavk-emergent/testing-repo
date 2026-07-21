"""
RCA Command - Slack DAG (poll every 2 min, IST)

On-demand RCA/briefing triggered by a human in Slack. When someone mentions the bot and types RCA
plus a customer email, e.g.:

    @Daily Report on prod-traj-error RCA someone@example.com

this DAG replies in that thread with the same 4-section briefing the ERC DAG produces (Trinity +
Overwatch RCA -> gpt-4o-mini). Not ERC-specific: works for any customer email, in any channel the
bot is in.

WHY POLL (Airflow-only): Slack @mention triggers normally need the Events API (an always-on
listener), which we don't run. Bot tokens also can't `search.messages`. So this polls, exactly like
erc_escalation_dag: every 2 min it lists the channels the bot belongs to (users.conversations) and
scans each for new command messages.

DYNAMIC CHANNELS: the channel set is discovered LIVE from `users.conversations` each run, so adding
the bot to a new channel makes RCA work there automatically (and removing it drops the channel).
The Redash `rca_channels` column is an OPTIONAL allowlist: if non-empty, only those channels are
polled; if empty (default), all channels the bot is in are polled.

NO WINDOW / ALWAYS FRESH: there is no dedup window. A per-channel watermark ensures each command
message is answered once; re-asking later is a NEW message -> a NEW, freshly generated RCA (the RCA
can change over time). No 30-day tracking is registered here — this is an on-demand lookup.

Reuses the whole briefing pipeline from erc_escalation_dag (config, MCP clients, assemble_context,
render_briefing). Config lives in the same Redash [ERC] config #41566.
"""

from datetime import timedelta
import logging, json, re

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

import erc_escalation_dag as erc   # shared pipeline (flat dags/ on Composer path)

logger = logging.getLogger(__name__)

CONFIG_QUERY_ID = 41566                 # same [ERC] config row (trinity/overwatch/llm/tag + rca_channels)
WATERMARK_VAR   = 'RCA_CMD_WATERMARKS'  # Airflow Variable: JSON {channel_id: last_ts}
MAX_PER_CHANNEL = 5                     # safety cap on commands handled per channel per run
RCA_RE          = re.compile(r'\brca\b', re.I)


def _bot_user_id():
    return erc._slack('auth.test', {})['user_id']


def _discover_channels(cfg):
    """Channels the bot is a member of (live). If rca_channels is set, use it as an allowlist filter."""
    allow = [c.strip() for c in (cfg.get('rca_channels') or '').split(',') if c.strip()]
    ids, cursor = [], None
    for _ in range(20):
        args = {'types': 'public_channel,private_channel', 'limit': 200, 'exclude_archived': 'true'}
        if cursor:
            args['cursor'] = cursor
        d = erc._slack('users.conversations', args, http='get')
        ids += [c['id'] for c in d.get('channels', [])]
        cursor = (d.get('response_metadata') or {}).get('next_cursor')
        if not cursor:
            break
    return [c for c in ids if c in allow] if allow else ids


def _is_command(m, bot_id):
    """A human message that @mentions the bot and contains 'RCA'."""
    if m.get('bot_id') or m.get('subtype'):
        return False
    text = m.get('text') or ''
    return ('<@%s>' % bot_id) in text and bool(RCA_RE.search(text))


def _command_email(text):
    """The email the requester typed (first external-looking address; fall back to first address)."""
    cands = [e.lower().strip('.,;:<>|') for e in erc.EMAIL_RE.findall(text or '')]
    for e in cands:
        if not erc._is_internal(e):
            return e
    return cands[0] if cands else None


def handle_command(channel, m, bot_id, trin, over, bq, cfg):
    thread_ts = m.get('thread_ts') or m['ts']
    email = _command_email(m.get('text', ''))
    if not email:
        erc.post_reply(channel, thread_ts, ':information_source: *RCA:* please include a customer email, e.g. `@Daily Report on prod-traj-error RCA someone@example.com`')
        return
    logger.info('RCA cmd in %s thread %s -> email=%s', channel, thread_ts, email)
    facts, open_t, sim_t, ow, tl, all_tickets = erc.assemble_context(email, trin, over, bq)
    if not all_tickets and not ow:
        erc.post_reply(channel, thread_ts, ':warning: *RCA:* `%s` identified but %s — skipped.' % (email, erc.NOACCOUNT_SIG))
        return
    proxy_url, proxy_key = cfg['llm_proxy_url'], cfg['llm_proxy_api_key']
    model = cfg.get('llm_model') or erc.DEFAULT_MODEL
    try:
        parts = erc._parse_brief_json(erc.llm_chat(proxy_url, proxy_key, model, erc.BRIEF_SYSTEM,
                                                   erc.build_brief_prompt(facts, open_t, sim_t, ow, tl), max_tokens=1200))
    except Exception as e:
        logger.exception('RCA %s: briefing JSON parse failed, facts-only: %s', email, e)
        parts = {}
    erc.post_reply(channel, thread_ts, erc.render_briefing(facts, parts))   # on-demand: no oncall tag, no tracking


def run_rca(**context):
    cfg = erc.redash_run(CONFIG_QUERY_ID)[0]
    cfg['llm_proxy_url'] = erc._env_override('LLM_PROXY_URL') or cfg.get('llm_proxy_url')
    cfg['llm_proxy_api_key'] = erc._env_override('LLM_PROXY_API_KEY') or cfg.get('llm_proxy_api_key')
    trin = erc.McpHttp(cfg['trinity_mcp_url'], cfg['trinity_api_key'])
    over = erc.McpHttp(cfg['overwatch_mcp_url'], cfg['overwatch_api_key'])
    bq = erc._bq()
    bq  # (unused table ensure not needed; assemble_context only reads)
    bot_id = _bot_user_id()
    channels = _discover_channels(cfg)
    logger.info('RCA: polling %d channels', len(channels))

    wm = {}
    try:
        wm = json.loads(Variable.get(WATERMARK_VAR, default_var='{}'))
    except Exception:
        wm = {}

    for ch in channels:
        last = wm.get(ch)
        if not last:
            # first time we see this channel -> start the clock now, ignore backlog
            wm[ch] = repr(pendulum.now('UTC').float_timestamp)
            continue
        try:
            d = erc._slack('conversations.history', {'channel': ch, 'oldest': last, 'limit': 50, 'inclusive': 'false'}, http='get')
        except Exception as e:
            logger.warning('RCA: history(%s) failed: %s', ch, e)
            continue
        msgs = sorted(d.get('messages', []), key=lambda x: float(x['ts']))
        newest = last
        handled = 0
        for m in msgs:
            newest = max(newest, m['ts'], key=lambda t: float(t))
            if handled < MAX_PER_CHANNEL and _is_command(m, bot_id):
                try:
                    handle_command(ch, m, bot_id, trin, over, bq, cfg)
                    handled += 1
                except Exception as e:
                    logger.exception('RCA: handle failed in %s: %s', ch, e)
        wm[ch] = newest

    Variable.set(WATERMARK_VAR, json.dumps(wm))


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
    'rca_command_slack',
    default_args=default_args,
    description='On-demand RCA briefing when a user mentions the bot with "RCA <email>"; polls the bot\'s channels (Trinity + Overwatch -> gpt-4o-mini)',
    schedule_interval='*/2 * * * *',    # poll every 2 min IST
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,       # posts to real channels; unpause after validation
    tags=['slack', 'trinity', 'overwatch', 'rca', 'cs_team'],
)

PythonOperator(task_id='run_rca', python_callable=run_rca, dag=dag)
