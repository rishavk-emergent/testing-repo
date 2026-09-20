"""
ECU credit-grant DAG — a config-driven, human-in-the-loop Slack approval poster.

FLOW (all per-run config lives in Redash #48782, no code push to change values):
  1. Once/day at the configured IST time, post a MASTER message in the main channel with a
     "Request ECU" button (Slack Workflow-Builder form link).
  2. A teammate fills that form -> Slack Workflow Builder drops a structured "ECU REQUEST" message
     into the intake channel (credit-audit-channel). That message's ts IS the request_id.
  3. Each 15-min tick the DAG polls the intake channel; for every NEW request it posts a
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

SECRETS are NOT in the config query — they are env-backed Airflow Variables:
  SLACK_BOT_TOKEN_EMERGENT_CS  (the dedicated Emergent-CS bot; needs chat:write, channels/groups:history,
                                users:read.email) and  ECU_SUPPORT_BEARER  (support-tool approver JWT).
Provision both in Composer as AIRFLOW_VAR_* before unpausing. Ships paused.

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
    from utils.slack.slack_config import REDASH_API_KEY, REDASH_BASE_URL
    from utils.slack.redash_client import RedashClient

    slack_token   = V.get('SLACK_BOT_TOKEN_EMERGENT_CS', default_var=None)
    support_bearer = V.get('ECU_SUPPORT_BEARER', default_var=None)
    if not slack_token:
        raise Exception('Airflow Variable SLACK_BOT_TOKEN_EMERGENT_CS is not set')

    IST = 'Asia/Kolkata'
    now = pendulum.now(IST)

    # ---- config ----
    cfg = []
    try:
        cfg = RedashClient(REDASH_API_KEY, REDASH_BASE_URL).fetch_query_results(config_query_id) or []
    except Exception as e:
        logger.warning('config query %s fetch failed, using fallbacks: %s', config_query_id, e)
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

    def grant_tokens(email, amount, detail):
        r = requests.post(SUPPORT_HOST + '/api/grant-tokens',
                          headers={'Authorization': 'Bearer ' + (support_bearer or ''),
                                   'Content-Type': 'application/json'},
                          json={'email': email, 'ecu_amount': amount, 'ecu_type': 'ecu',
                                'reason': reason_default, 'reason_detail': detail}, timeout=30)
        return r.status_code, (r.json() if r.content else {})

    def approve_refund(refund_id):
        r = requests.post(SUPPORT_HOST + '/api/approve-refund',
                          headers={'Authorization': 'Bearer ' + (support_bearer or ''),
                                   'Content-Type': 'application/json'},
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

    # ---- card blocks ----
    def card_blocks(req, status_line=None, with_buttons=True):
        body = (":inbox_tray: *New Request*\n"
                "• customer: `%s`\n• amount: *%s ECU*\n• reason: %s\n• requested_by: %s"
                % (req.get('customer_email'), req.get('amount'), req.get('reason'), req.get('requested_by')))
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": body}}]
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
        slack_post('chat.update', {"channel": main_ch, "ts": req['card_ts'],
                                   "text": "ECU request", "blocks": card_blocks(req, status_line, with_buttons)})

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
            logger.info('posted master for %s -> %s', today, master_ts)
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
        if intake_ts in state['requests']:
            continue
        amount_raw = field(text, 'ECU Amount')
        try:
            amount = float(amount_raw)
        except Exception:
            amount = None
        req = {
            'intake_ts': intake_ts,
            'customer_email': field(text, 'Customer Email'),
            'amount': amount,
            'reason': field(text, 'Reason'),
            'requested_by': field(text, 'Requested By'),
            'status': 'pending',
            'posted_at': now.to_iso8601_string(),
            'approval_request_id': None,
        }
        resp = slack_post('chat.postMessage', {"channel": main_ch, "thread_ts": master_ts,
                          "text": "New ECU request", "blocks": card_blocks(req, with_buttons=True)})
        if resp.get('ok'):
            req['card_ts'] = resp['ts']
            state['requests'][intake_ts] = req
            logger.info('posted card for request %s (card %s)', intake_ts, req['card_ts'])

    # ---- 3. process pending requests: read CLICK replies, act, update card ----
    for intake_ts, req in list(state['requests'].items()):
        if req.get('status') != 'pending' or not req.get('card_ts'):
            continue

        # expiry
        posted = pendulum.parse(req['posted_at'])
        if now.diff(posted).in_hours() >= stale_hours:
            req['status'] = 'expired'
            update_card(req, ":hourglass: *Expired* — no approver action within %dh (no credit issued)" % stale_hours, with_buttons=False)
            logger.info('request %s expired', intake_ts)
            continue

        # read CLICK ledger in the intake thread; take the first click by an allowlisted approver
        replies = slack_get('conversations.replies', channel=intake_ch, ts=intake_ts, limit=50)
        decision = None  # (action, clicker_uid)
        for r in replies.get('messages', []):
            t = r.get('text', '')
            if not t.startswith('CLICK'):
                continue
            am = re.search(r'action=(\S+)', t)
            um = re.search(r'user=(\S+)', t)
            if not am or not um:
                continue
            if um.group(1) in approver_ids:
                decision = (am.group(1), um.group(1))
                break
        if not decision:
            continue  # still pending, check next tick

        action, clicker = decision
        who = email_for(clicker)

        if action == ACTION_REJECT:
            req['status'] = 'rejected'
            req['decided_by'] = who
            update_card(req, ":x: *Rejected* by %s — no credit issued · _%s_" % (who, ist_now_str()), with_buttons=False)
            logger.info('request %s rejected by %s', intake_ts, who)
            continue

        if action != ACTION_APPROVE:
            continue

        # DAG-side cap guard (server SOP is separate): never auto-approve above per_request_cap
        if per_request_cap and req.get('amount') and req['amount'] > per_request_cap:
            update_card(req, ":warning: amount %s exceeds DAG cap (%s) — handle manually" % (req['amount'], per_request_cap), with_buttons=True)
            logger.warning('request %s amount %s over cap %s', intake_ts, req['amount'], per_request_cap)
            continue

        if not support_bearer:
            update_card(req, ":warning: *Approval failed* — support token missing, will retry", with_buttons=True)
            logger.error('ECU_SUPPORT_BEARER not set; cannot grant %s', intake_ts)
            continue

        # grant (idempotent): reuse a stored approval id if grant already ran
        approval_id = req.get('approval_request_id')
        ok = False
        detail = req.get('reason') or ''
        if not approval_id:
            sc, body = grant_tokens(req['customer_email'], req['amount'], detail)
            if sc == 200 and body.get('success'):
                if body.get('status') == 'PENDING_APPROVAL' and body.get('approval_request_id'):
                    approval_id = body['approval_request_id']
                    req['approval_request_id'] = approval_id  # persisted below; safe to re-approve
                else:
                    ok = True  # under SOP: applied immediately, no approve step
            else:
                update_card(req, ":warning: *Approval failed* — grant error, will retry · _%s_" % ist_now_str(), with_buttons=True)
                logger.error('grant failed for %s: %s %s', intake_ts, sc, body)
                V.set(state_var, json.dumps(state))  # persist approval_id if any
                continue

        if approval_id and not ok:
            sc, body = approve_refund(approval_id)
            ok = (sc == 200 and body.get('success'))
            if not ok:
                update_card(req, ":warning: *Approval failed* — approve error, will retry · _%s_" % ist_now_str(), with_buttons=True)
                logger.error('approve failed for %s: %s %s', intake_ts, sc, body)
                V.set(state_var, json.dumps(state))
                continue

        # success — lock the card
        req['status'] = 'approved'
        req['decided_by'] = who
        update_card(req, ":white_check_mark: *Approved* by %s · %s ECU issued · _%s_" % (who, req.get('amount'), ist_now_str()), with_buttons=False)
        logger.info('request %s APPROVED by %s (%s ECU)', intake_ts, who, req.get('amount'))

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
    schedule_interval='*/15 * * * *',
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
