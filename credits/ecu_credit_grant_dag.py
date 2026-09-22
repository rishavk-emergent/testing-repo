"""
ECU credit-grant DAG — a config-driven, human-in-the-loop Slack approval poster.

FLOW (all per-run config lives in Redash #48782, no code push to change values):
  1. Once/day at the configured IST time, post a MASTER message in the main channel with a
     "Request ECU" button (Slack Workflow-Builder form link).
  2. A teammate fills that form -> Slack Workflow Builder drops a structured "ECU REQUEST" message
     into the intake channel (credit-audit-channel). That message's ts IS the request_id.
  3. Each 5-min tick the DAG polls the intake channel; for every NEW request it posts a
     card (customer/amount/reason) as a threaded reply under the day's master message, with
     two buttons — ✅ Approve / ❌ Reject — whose `value` is the intake ts (so a click threads
     back under the right request in the intake channel).
  4. A click goes Slack -> the Emergent-CS app's Interactivity Request URL (a Zapier catch-hook)
     -> Zapier posts a `CLICK action=... user=... name=...` reply under the intake request.
  5. Each tick the DAG reads those CLICK replies. If a click came from an APPROVER on the
     allowlist (config `approver_emails`): ✅ -> grant + approve the ECU via the support-tool
     API; ❌ -> mark rejected (no credit). Anyone else's click is ignored.
  6. On success the card is rewritten to a locked state (buttons removed) showing
     "✅ Approved by <email> · N ECU · <IST>" (or "❌ Rejected by <email>"). If the grant/approve
     API call FAILS (bad/expired token, API error) the card keeps its buttons and shows a
     "⚠️ Approval failed — will retry" line; the request stays pending and is retried next tick.
  7. A request with no approver action within `stale_hours` (72) becomes "⌛ Expired" (no credit).

IDEMPOTENCY: a request is marked done (approved/rejected/expired) in the ECU_CREDIT_STATE
Airflow Variable ONLY after the action truly succeeds, so partial failures always retry safely
and a request is acted on exactly once. If grant returns PENDING_APPROVAL its approval id is
stored so a retry re-approves that same id instead of re-granting (no duplicate issuance).

AUTH: the Emergent-CS bot token lives in config query #48782 as `slack_bot_token`. The support-tool approver
bearer is NOT stored — it is MINTED via Supabase password grant (grant_type=password) from `supabase_url` +
`supabase_anon_key` (public) + `supabase_email` + `supabase_password`, all in the config query (slack_bot_token
and supabase_password are live secrets — anyone with Redash access to the query can read them). Stateless: no
refresh-token rotation. The mint happens ONCE/DAY at the daily master post and the bearer is cached in
state['bearer'] and reused for every grant/approve that day (a minted token is valid ~7 days). Ships paused.

Top level stays DB/API-free (Airflow re-parses every ~30s): only constants + the DAG object.
"""

from datetime import timedelta
import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

CONFIG_QUERY_ID = 48782            # [ECU Credits] config (Redash, BigQuery ds 7)
STATE_VAR       = 'ECU_CREDIT_STATE'
SUPPORT_HOST    = 'http://support-tool.cloudrun.internal.prod.emergentagent.com'
ACTION_APPROVE  = 'ecu_approve'
ACTION_REJECT   = 'ecu_reject'

# Fallbacks used only if the config query is unreachable on a tick (keeps a non-trigger tick alive).
FB_MAIN_CHANNEL   = 'C0B4J9RBWDC'
FB_INTAKE_CHANNEL = 'C0C2QNTBHD5'
FB_MARKER         = 'ECU REQUEST'
FB_STALE_HOURS    = 72


# ----------------------------- small config helpers -----------------------------
def _cfg(cfg, key, default=None):
    return next((r.get(key) for r in (cfg or []) if r.get(key) not in (None, '')), default)


def _int(cfg, key, default):
    try:
        return int(_cfg(cfg, key, default))
    except Exception:
        return default


# ----------------------------- the task -----------------------------
def run_ecu(config_query_id, state_var, **context):
    # Heavy / credential-bearing deps imported lazily so a DAG parse mid plugin-sync can't
    # ImportError the whole file (house rule: runtime-only SDKs stay inside the task).
    import json, re
    import requests
    from airflow.models import Variable as V
    from utils.redash import RedashClient

    IST = 'Asia/Kolkata'
    now = pendulum.now(IST)

    # ---- config (incl. the Emergent-CS bot token — by design the token lives in the query, #48782) ----
    cfg = []
    try:
        cfg = RedashClient().fetch_query_results(config_query_id) or []
    except Exception as e:
        logger.warning('config query %s fetch failed, using fallbacks: %s', config_query_id, e)
    slack_token = _cfg(cfg, 'slack_bot_token')
    if not slack_token:
        raise Exception('slack_bot_token missing from config query %s (Redash unreachable or column removed)' % config_query_id)
    main_ch   = _cfg(cfg, 'main_channel_id', FB_MAIN_CHANNEL)
    intake_ch = _cfg(cfg, 'intake_channel_id', FB_INTAKE_CHANNEL)
    workflow_url = _cfg(cfg, 'workflow_url')
    approver_emails = [e.strip().lower() for e in (_cfg(cfg, 'approver_emails', '') or '').split(',') if e.strip()]
    trig_hour = _int(cfg, 'trigger_hour', 0)
    trig_min  = _int(cfg, 'trigger_minute', 0)
    stale_hours = _int(cfg, 'stale_hours', FB_STALE_HOURS)
    per_request_cap = _int(cfg, 'per_request_cap', 0)
    reason_default = _cfg(cfg, 'reason_default', 'Customer Support')
    marker = _cfg(cfg, 'request_marker', FB_MARKER)
    excluded_ts = {x.strip() for x in (_cfg(cfg, 'excluded_request_ts', '') or '').split(',') if x.strip()}  # test/legacy submissions to ignore
    supa_url      = _cfg(cfg, 'supabase_url')
    supa_anon     = _cfg(cfg, 'supabase_anon_key')
    supa_email    = _cfg(cfg, 'supabase_email')
    supa_password = _cfg(cfg, 'supabase_password')
    click_bot_id  = _cfg(cfg, 'click_bot_id')   # Slack bot_id of the Zapier app that posts CLICK ledger replies

    # ---- tiny Slack + support-tool helpers (utils/ is intentionally NOT modified) ----
    def slack_get(method, **params):
        r = requests.get('https://slack.com/api/' + method,
                         headers={'Authorization': 'Bearer ' + slack_token},
                         params=params, timeout=30)
        return r.json()

    def slack_post(method, payload):
        r = requests.post('https://slack.com/api/' + method,
                          headers={'Authorization': 'Bearer ' + slack_token,
                                   'Content-Type': 'application/json; charset=utf-8'},
                          json=payload, timeout=30)
        return r.json()

    _bearer_cache = {}

    def mint_bearer():
        """Mint a fresh support-tool approver bearer via Supabase password grant
        (grant_type=password) using supabase_url/anon_key/email/password from the config query.
        Stateless — no refresh-token rotation. The minted access token is valid ~7 days; cached
        for the rest of this run. Returns None (logged) on failure so the caller can retry."""
        if _bearer_cache.get('tok'):
            return _bearer_cache['tok']
        if not (supa_url and supa_anon and supa_email and supa_password):
            logger.error('cannot mint bearer: missing supabase_url/anon_key/email/password in config query')
            return None
        try:
            resp = requests.post(supa_url.rstrip('/') + '/auth/v1/token?grant_type=password',
                                 headers={'apikey': supa_anon, 'Content-Type': 'application/json'},
                                 json={'email': supa_email, 'password': supa_password}, timeout=30)
        except Exception as e:
            logger.error('supabase password grant request error: %s', e)
            return None
        if resp.status_code != 200:
            logger.error('supabase password grant failed: %s %s', resp.status_code, resp.text[:200])
            return None
        _bearer_cache['tok'] = resp.json().get('access_token')
        return _bearer_cache['tok']

    def get_bearer():
        # The bearer is refreshed ONCE/DAY at the master post (stored in state['bearer']); reuse
        # it all day. Fall back to minting on demand only if it's missing (e.g. that refresh failed).
        return state.get('bearer') or mint_bearer()

    def grant_tokens(email, amount, detail):
        bearer = get_bearer()
        if not bearer:
            return None, {'error': 'no_bearer'}
        r = requests.post(SUPPORT_HOST + '/api/grant-tokens',
                          headers={'Authorization': 'Bearer ' + bearer, 'Content-Type': 'application/json'},
                          json={'email': email, 'ecu_amount': amount, 'ecu_type': 'ecu',
                                'reason': reason_default, 'reason_detail': detail}, timeout=30)
        return r.status_code, (r.json() if r.content else {})

    def approve_refund(refund_id):
        bearer = get_bearer()
        if not bearer:
            return None, {'error': 'no_bearer'}
        r = requests.post(SUPPORT_HOST + '/api/approve-refund',
                          headers={'Authorization': 'Bearer ' + bearer, 'Content-Type': 'application/json'},
                          json={'refund_id': refund_id}, timeout=30)
        return r.status_code, (r.json() if r.content else {})

    # ---- state ----
    raw = V.get(state_var, default_var=None)
    state = json.loads(raw) if raw else {}
    if not isinstance(state, dict):
        raise ValueError('state var %s is not a dict' % state_var)
    state.setdefault('requests', {})   # intake_ts -> request record
    state.setdefault('master', {})     # YYYY-MM-DD -> master message ts

    # ---- resolve approver emails -> slack user ids (+ id->email cache for display) ----
    approver_ids, id_to_email = set(), {}
    for em in approver_emails:
        resp = slack_get('users.lookupByEmail', email=em)
        if resp.get('ok'):
            uid = resp['user']['id']
            approver_ids.add(uid)
            id_to_email[uid] = em

    def email_for(uid):
        if uid in id_to_email:
            return id_to_email[uid]
        info = slack_get('users.info', user=uid)
        return (info.get('user', {}).get('profile', {}).get('email') if info.get('ok') else None) or uid

    # ---- intake message parser ----
    def unwrap(v):
        m = re.search(r'<mailto:([^|>]+)', v or '')
        return m.group(1) if m else (v or '').strip()

    def field(text, label):
        m = re.search(r'\*?' + re.escape(label) + r':\*?\s*(.+)', text or '')
        return unwrap(m.group(1).strip()) if m else None

    def reason_full(text):
        # Reason may be multi-line — capture everything from the Reason label up to the next
        # field label (Requested By) or end of message, so the WHOLE reason is preserved.
        m = re.search(r'\*?Reason:\*?\s*(.*?)\s*(?=\n\*?Requested By:|\Z)', text or '', re.DOTALL)
        return m.group(1).strip() if m else None

    def _chunk(s, n=2900):
        # split into <=n-char pieces on line boundaries (Slack section text max is 3000 chars)
        out, cur = [], ''
        for line in (s or '').split('\n'):
            while len(line) > n:
                if cur:
                    out.append(cur); cur = ''
                out.append(line[:n]); line = line[n:]
            if len(cur) + len(line) + 1 > n:
                out.append(cur); cur = line
            else:
                cur = (cur + '\n' + line) if cur else line
        if cur:
            out.append(cur)
        return out or ['']

    # ---- card blocks ----
    def card_blocks(req, status_line=None, with_buttons=True):
        head = (":inbox_tray: *New Request*\n"
                "• *Customer Email:* %s\n• *Amount:* %s ECU\n• *Requested By:* %s"
                % (req.get('customer_email'), req.get('amount'),
                   req.get('requested_by_mention') or req.get('requested_by')))
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": head}}]
        # Reason gets its own block(s) so the FULL (possibly long/multi-line) reason is shown,
        # chunked to stay under Slack's 3000-char-per-section limit.
        reason = req.get('reason') or '_(none)_'
        for c in _chunk('*Reason:*\n' + reason, 1000):
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": c}})
        if with_buttons:
            rid = req['intake_ts']
            blocks.append({"type": "actions", "block_id": rid, "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": "✅ Approve", "emoji": True},
                 "style": "primary", "action_id": ACTION_APPROVE, "value": rid},
                {"type": "button", "text": {"type": "plain_text", "text": "❌ Reject", "emoji": True},
                 "style": "danger", "action_id": ACTION_REJECT, "value": rid}]})
        if status_line:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": status_line}]})
        return blocks

    def update_card(req, status_line, with_buttons):
        # best-effort: a chat.update failure must never abort the run or wrongly lock terminal state
        try:
            r = slack_post('chat.update', {"channel": main_ch, "ts": req['card_ts'],
                                           "text": "ECU request", "blocks": card_blocks(req, status_line, with_buttons)})
            return bool(r.get('ok'))
        except Exception as e:
            logger.error('card update failed for %s: %s', req.get('intake_ts'), e)
            return False

    def ist_now_str():
        return pendulum.now(IST).format('YYYY-MM-DD HH:mm') + ' IST'

    # ---- 1. ensure today's master message (posts once/day at/after trigger time) ----
    today = now.format('YYYY-MM-DD')
    time_reached = (now.hour * 60 + now.minute) >= (trig_hour * 60 + trig_min)
    master_ts = state['master'].get(today)
    if master_ts is None and time_reached:
        blocks = [{"type": "section", "text": {"type": "mrkdwn",
                   "text": ":credit_card: *ECU Credit Requests — %s*\nClick below to submit a request for a customer." % now.format('ddd, DD MMM YYYY')}}]
        if workflow_url:
            blocks.append({"type": "actions", "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": "➕ Request ECU", "emoji": True},
                 "url": workflow_url, "style": "primary"}]})
        resp = slack_post('chat.postMessage', {"channel": main_ch,
                          "text": "ECU Credit Requests — click to submit", "blocks": blocks})
        if resp.get('ok'):
            master_ts = resp['ts']
            state['master'][today] = master_ts
            state['bearer'] = mint_bearer()   # refresh the support-tool bearer once/day, here at the master post
            V.set(state_var, json.dumps(state))   # PERSIST master immediately — a later error must not cause a duplicate master next run
            logger.info('posted master for %s -> %s (bearer refreshed=%s)', today, master_ts, bool(state.get('bearer')))
    if master_ts is None:
        # Before trigger time on a fresh day: nothing to thread cards under yet.
        V.set(state_var, json.dumps(state))
        logger.info('no master yet today (trigger %02d:%02d IST); exiting tick', trig_hour, trig_min)
        return

    # ---- 2. poll intake for NEW requests -> post cards under today's master ----
    lookback = now.subtract(hours=stale_hours + 8)
    hist = slack_get('conversations.history', channel=intake_ch, limit=200,
                     oldest=str(lookback.timestamp()))
    for m in hist.get('messages', []):
        text = m.get('text', '')
        if marker not in text:
            continue
        intake_ts = m['ts']
        if intake_ts in excluded_ts:
            continue  # test/legacy submission, treat as already handled
        if intake_ts in state['requests']:
            continue
        amount_raw = field(text, 'ECU Amount')
        try:
            amount = float(amount_raw)
        except Exception:
            amount = None
        rb = field(text, 'Requested By')
        rb_mention = rb  # fall back to raw text if it can't be resolved to a Slack user
        if rb and '@' in rb:
            lk = slack_get('users.lookupByEmail', email=rb)
            if lk.get('ok'):
                rb_mention = '<@%s>' % lk['user']['id']
        req = {
            'intake_ts': intake_ts,
            'customer_email': field(text, 'Customer Email'),
            'amount': amount,
            'reason': reason_full(text),
            'requested_by': rb,
            'requested_by_mention': rb_mention,
            'status': 'pending',
            'posted_at': now.to_iso8601_string(),
            'approval_request_id': None,
        }
        resp = slack_post('chat.postMessage', {"channel": main_ch, "thread_ts": master_ts,
                          "text": "New ECU request", "blocks": card_blocks(req, with_buttons=True)})
        if resp.get('ok'):
            req['card_ts'] = resp['ts']
            state['requests'][intake_ts] = req
            V.set(state_var, json.dumps(state))   # PERSIST each posted card immediately — never re-post it
            logger.info('posted card for request %s (card %s)', intake_ts, req['card_ts'])

    # ---- 3. process pending requests: read CLICK replies, act, update card ----
    # Each request is handled in isolation and state is persisted after EVERY request (success or
    # error) so a single failing Slack/API call can never abort the run or lose the day's state.
    def _process_pending(intake_ts, req):
        # Read the CLICK ledger FIRST — a valid approver click near the 72h deadline must not be lost
        # to expiry. Only trust replies posted by the Zapier app identity (click_bot_id); the user id
        # embedded in plain text alone is forgeable by anyone who can post in the intake channel.
        replies = slack_get('conversations.replies', channel=intake_ch, ts=intake_ts, limit=50)
        decision = None  # (action, clicker_uid)
        for r in replies.get('messages', []):
            t = r.get('text', '')
            if not t.startswith('CLICK'):
                continue
            if click_bot_id and r.get('bot_id') != click_bot_id:
                continue  # not from the trusted Zapier identity -> ignore
            am = re.search(r'action=(\S+)', t)
            um = re.search(r'user=(\S+)', t)
            if not am or not um:
                continue
            if um.group(1) in approver_ids:
                decision = (am.group(1), um.group(1))
                break

        if not decision:
            # no qualifying approver click yet -> expire only once past the deadline
            posted = pendulum.parse(req['posted_at'])
            if now.diff(posted).in_hours() >= stale_hours:
                if update_card(req, ":hourglass: *Expired* — no approver action within %dh (no credit issued)" % stale_hours, with_buttons=False):
                    req['status'] = 'expired'   # terminal only after the card updated (no billing -> safe to retry)
                    logger.info('request %s expired', intake_ts)
            return

        action, clicker = decision
        who = email_for(clicker)

        if action == ACTION_REJECT:
            if update_card(req, ":x: *Rejected* by %s — no credit issued · _%s_" % (who, ist_now_str()), with_buttons=False):
                req['status'] = 'rejected'   # terminal only after the card updated (no billing -> safe to retry)
                req['decided_by'] = who
                logger.info('request %s rejected by %s', intake_ts, who)
            return

        if action != ACTION_APPROVE:
            return

        # DAG-side cap guard (server SOP is separate): never auto-approve above per_request_cap
        if per_request_cap and req.get('amount') and req['amount'] > per_request_cap:
            update_card(req, ":warning: amount %s exceeds DAG cap (%s) — handle manually" % (req['amount'], per_request_cap), with_buttons=True)
            logger.warning('request %s amount %s over cap %s', intake_ts, req['amount'], per_request_cap)
            return

        if not get_bearer():
            update_card(req, ":warning: *Approval failed* — could not mint support token, will retry · _%s_" % ist_now_str(), with_buttons=True)
            logger.error('could not mint bearer (supabase creds?) for %s', intake_ts)
            return

        # grant (idempotent): reuse a stored approval id if grant already ran
        approval_id = req.get('approval_request_id')
        ok = False
        detail = req.get('reason') or ''
        if not approval_id:
            sc, body = grant_tokens(req['customer_email'], req['amount'], detail)
            if sc == 200 and body.get('success'):
                if body.get('status') == 'PENDING_APPROVAL' and body.get('approval_request_id'):
                    approval_id = body['approval_request_id']
                    req['approval_request_id'] = approval_id
                    V.set(state_var, json.dumps(state))  # persist approval id NOW so a retry re-approves, never re-grants
                else:
                    ok = True  # under SOP: applied immediately, no approve step
                    req['status'] = 'approved'; req['decided_by'] = who
                    V.set(state_var, json.dumps(state))  # persist BEFORE proceeding so a crash can't re-grant
            else:
                update_card(req, ":warning: *Approval failed* — grant error, will retry · _%s_" % ist_now_str(), with_buttons=True)
                logger.error('grant failed for %s: %s %s', intake_ts, sc, body)
                return

        if approval_id and not ok:
            sc, body = approve_refund(approval_id)
            ok = (sc == 200 and body.get('success'))
            if not ok:
                update_card(req, ":warning: *Approval failed* — approve error, will retry · _%s_" % ist_now_str(), with_buttons=True)
                logger.error('approve failed for %s: %s %s', intake_ts, sc, body)
                return

        # success — credit is already issued, so persist 'approved' regardless (never re-grant);
        # the card update is best-effort (cosmetic) and only stale if chat.update fails.
        req['status'] = 'approved'
        req['decided_by'] = who
        update_card(req, ":white_check_mark: *Approved* by %s · %s ECU issued · _%s_" % (who, req.get('amount'), ist_now_str()), with_buttons=False)
        logger.info('request %s APPROVED by %s (%s ECU)', intake_ts, who, req.get('amount'))

    for intake_ts, req in list(state['requests'].items()):
        if intake_ts in excluded_ts:
            if req.get('status') == 'pending':
                req['status'] = 'excluded'   # excluded at runtime AFTER being tracked -> never act on it
                V.set(state_var, json.dumps(state))
            continue
        if req.get('status') != 'pending' or not req.get('card_ts'):
            continue
        try:
            _process_pending(intake_ts, req)
        except Exception as e:
            logger.error('error processing request %s (left pending for next tick): %s', intake_ts, e)
        finally:
            V.set(state_var, json.dumps(state))  # persist after EVERY request, success or error

    V.set(state_var, json.dumps(state))
    logger.info('tick done: %d tracked requests', len(state['requests']))


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
    'ecu_credit_grant',
    default_args=default_args,
    description='Human-in-the-loop ECU credit grants: form -> card w/ approve/reject -> grant+approve (config #%d)' % CONFIG_QUERY_ID,
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['slack', 'credits', 'ecu', 'cs_team', 'approval'],
)
PythonOperator(
    task_id='run_ecu', python_callable=run_ecu,
    op_kwargs={'config_query_id': CONFIG_QUERY_ID, 'state_var': STATE_VAR},
    dag=dag,
)
