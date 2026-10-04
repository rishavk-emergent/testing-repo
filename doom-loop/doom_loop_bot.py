"""
Doom-Loop / Live-Call process bot - core state machine.

Append-only thread. Nothing already sent is deleted or has its TEXT rewritten;
the only in-place edit is to a message's BUTTON block (buttons -> resolved text).

Layout:
  (top, once)   [Others join]                         <- global, anyone, never locks
  (tag msg)     @csm1 -> support call for you. Can you join?   [CSM join] [CSM not available]

  * CSM not available  (tagged CSM click, OR timeout):
      - the tag msg's buttons resolve in place to text (~CSM join~  ❌ CSM not available)
      - append a line:  ❌ @csm1 not available   (or "⏱️ System marked @csm1 not available")
      - append a NEW tag msg:  @csm2 -> support call for you. Can you join?  [buttons]
  * CSM join  (tagged CSM only):
      - the tag msg's buttons resolve in place to text (✅ CSM joined)
      - mark busy; open the in-call flow:
          in-call -> [No show]/[User dropped] -> free
                  -> [Closure] -> [Summary posted]+[Ticket link posted] -> free
  * Others join  (anyone, any time): posts "@clicker joined". Never locks.

TECH FALLBACK: when every CSM is busy/not-available (exhausted, or none free at
claim), append a tag msg tagging the WHOLE slot Tech list at once:
  ⚠️ No CSM available - @tech1 @tech2 ... please join.   [CSM join]
Any tagged Tech can click CSM join (auth widens to the tech set).

ENFORCEMENT: CSM join / CSM not available -> the currently-tagged CSM (or any tagged
Tech in fallback); in-call buttons -> the assigned person; Others join -> anyone.
Unauthorized clicks are silently ignored (ENFORCE_NOTIFY flips to a note).

Transport + state only; tested in-process (by={"id","name"}; timeout via
on_csm_not_available(state, system=True)). In production, relayed clicks + poller
drive the same handlers. State -> Mongo.
"""
import json
import time
import urllib.request
import urllib.parse

SLACK_API = "https://slack.com/api/"
ENFORCE_NOTIFY = False

# ─────────────────────────── Slack transport ───────────────────────────

def slack_call(token, method, payload, as_json=True):
    url = SLACK_API + method
    if as_json:
        data = json.dumps(payload).encode()
        headers = {"Authorization": "Bearer " + token,
                   "Content-Type": "application/json; charset=utf-8"}
    else:
        data = urllib.parse.urlencode(payload).encode()
        headers = {"Authorization": "Bearer " + token,
                   "Content-Type": "application/x-www-form-urlencoded"}
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req) as r:
        out = json.load(r)
    if not out.get("ok"):
        raise RuntimeError("Slack %s failed: %s" % (method, out.get("error")))
    return out


def post_thread(token, channel, thread_ts, text, blocks=None):
    payload = {"channel": channel, "thread_ts": thread_ts, "text": text}
    if blocks is not None:
        payload["blocks"] = blocks
    return slack_call(token, "chat.postMessage", payload)


def update_msg(token, channel, ts, text, blocks=None):
    payload = {"channel": channel, "ts": ts, "text": text}
    if blocks is not None:
        payload["blocks"] = blocks
    return slack_call(token, "chat.update", payload)


# ─────────────────────────── message detection ───────────────────────────

CLAIMED = "Support call claimed!"
REPEAT = "clicked again and is waiting in the same room"


def alert_kind(text):
    if not text:
        return None
    if CLAIMED in text:
        return "claimed"
    if REPEAT in text:
        return "repeat"
    return None


# ─────────────────────────── roster / availability ───────────────────────────

def render_mention(person):
    """Roster person display - real <@slack_id> ping when we have it, else bold name."""
    sid = person.get("slack_id")
    return "<@%s>" % sid if sid else "*%s*" % person["name"]


def mention_user(by):
    if by and by.get("id"):
        return "<@%s>" % by["id"]
    return "*%s*" % (by or {}).get("name", "someone")


def pick_available(ladder, busy, start=0):
    for i in range(start, len(ladder)):
        if ladder[i]["name"] not in busy:
            return i, ladder[i]
    return None, None


# ─────────────────────────── blocks ───────────────────────────

def section(md):
    return {"type": "section", "text": {"type": "mrkdwn", "text": md}}


def render_actions(specs, value):
    elems = []
    for text, aid, style in specs:
        b = {"type": "button", "text": {"type": "plain_text", "text": text, "emoji": True},
             "action_id": aid, "value": value}
        if style:
            b["style"] = style
        elems.append(b)
    return {"type": "actions", "elements": elems}


def fmt_mins(mins):
    if mins < 1:
        return "<1 min"
    if mins < 60:
        return "%d min" % mins
    return "%dh %dm" % (mins // 60, mins % 60)


OTHERS_BTN = ("Others join", "dl_others_join", "primary")
SKIP_BTN = ("Skip", "dl_skip", "danger")   # repeat ("clicked again") only: dismiss a duplicate call + free this thread
CSM_JOIN_BTN = ("CSM join", "dl_csm_join", "primary")
CSM_NA_BTN = ("CSM not available", "dl_csm_not_available", "danger")
TAG_SPECS = [CSM_JOIN_BTN, CSM_NA_BTN]
TAG_SPECS_TECH = [CSM_JOIN_BTN]
OUTCOME_SPECS = [("🚫 No show", "dl_no_show", None),
                 ("🌐 User dropped", "dl_user_dropped", None),
                 ("💬 Closure", "dl_closure", "primary")]
LOG_SPECS = [("📝 Summary posted", "dl_summary", "primary"),
             ("🎫 Ticket link posted", "dl_ticket", "primary")]


# ─────────────────────────── engine ───────────────────────────

class DoomLoopEngine:
    def __init__(self, token, channel, ladder, tech_ladder=None):
        self.token = token
        self.channel = channel
        self.ladder = ladder
        self.tech = tech_ladder or []
        self.busy = set()
        self.states = {}

    def _post(self, state, text, blocks=None):
        return post_thread(self.token, self.channel, state["thread_ts"], text, blocks)

    def _post_buttons(self, state, prompt, specs):
        r = self._post(state, prompt, [section(prompt), render_actions(specs, state["id"])])
        state["reg"][r["ts"]] = {"prompt": prompt, "specs": specs}
        return r["ts"]

    def _resolve_buttons(self, state, msg_ts, resolved_line):
        """In place: keep the message's prompt text, replace its BUTTON block with
        a resolved text line. Nothing is deleted; only the buttons change."""
        reg = state["reg"][msg_ts]
        update_msg(self.token, self.channel, msg_ts, reg["prompt"],
                   [section(reg["prompt"] + "\n" + resolved_line)])

    def _resolve(self, state, msg_ts, clicked, exclusive=True):
        """Outcome/log messages: chosen -> normal text, unchosen -> struck
        (exclusive) or kept live (non-exclusive)."""
        reg = state["reg"][msg_ts]
        done, live = [], []
        for text, aid, style in reg["specs"]:
            if aid in clicked:
                done.append(text)
            elif exclusive:
                done.append("~%s~" % text)
            else:
                live.append((text, aid, style))
        body = reg["prompt"] + ("\n" + "    ".join(done) if done else "")
        blocks = [section(body)]
        if live:
            blocks.append(render_actions(live, state["id"]))
        update_msg(self.token, self.channel, msg_ts, reg["prompt"], blocks)

    def _cur_person(self, state):
        return self.ladder[state["attempt_idx"]]

    # ---- tag messages (each CSM / the tech group gets its own) ----
    def _tag_csm(self, state, person):
        prompt = "%s → support call for you. Can you join?" % render_mention(person)
        state["tag_msg_ts"] = self._post_buttons(state, prompt, TAG_SPECS)

    def _tag_tech(self, state):
        state["mode"] = "tech"
        state["tech_sids"] = {t.get("slack_id") for t in self.tech}
        group = " ".join(render_mention(t) for t in self.tech)
        prompt = "%s - no CSM available, please join the call." % group
        state["tag_msg_ts"] = self._post_buttons(state, prompt, TAG_SPECS_TECH)

    # ---- authorization ----
    def _authorized(self, by, sid):
        if not sid:
            return True
        return bool(by) and by.get("id") == sid

    def _reject(self, state, by, who):
        if ENFORCE_NOTIFY:
            self._post(state, "⚠️ Only %s can use that button." % who)
        return state

    # ---- entry: "Support call claimed!" ----
    def on_claimed(self, thread_ts, alert_text="", is_repeat=False):
        sid = "dl:%s" % thread_ts
        state = {"id": sid, "thread_ts": thread_ts, "channel": self.channel, "is_repeat": is_repeat,
                 "mode": "csm", "attempt_idx": None, "tagged_sid": None, "tag_ts": time.time(),
                 "tech_sids": set(), "assigned": None, "assigned_person": None, "assigned_sid": None,
                 "step": "tagging", "join_ts": None,
                 "summary_posted": False, "ticket_posted": False,
                 "reg": {}, "others_ts": None, "tag_msg_ts": None,
                 "outcome_msg_ts": None, "log_msg_ts": None}
        self.states[sid] = state
        # global controls at top - Others join always; Skip only on a repeat ("clicked again")
        top = [OTHERS_BTN, SKIP_BTN] if is_repeat else [OTHERS_BTN]
        header = "Anyone can jump on this call:  _(repeat - Skip if duplicate)_" if is_repeat else "Anyone can jump on this call:"
        state["others_ts"] = self._post_buttons(state, header, top)
        idx, person = pick_available(self.ladder, self.busy, 0)
        if person is not None:
            state["attempt_idx"] = idx
            state["tagged_sid"] = person.get("slack_id")
            self._tag_csm(state, person)
        elif self.tech:
            self._tag_tech(state)
        else:
            state["step"] = "exhausted"
            self._post(state, "⚠️ No CSM or Tech available for this call.")
        return state

    # ---- global: Others join ----
    def on_others_join(self, state, by=None):
        self._post(state, "%s joined." % mention_user(by))
        return state

    # ---- global: Skip (repeat/duplicate call) - anyone; ends this thread + frees its people ----
    def on_skip(self, state, by=None):
        if state["step"] in ("skipped", "done"):
            return state
        freed = state.get("assigned_person")
        if freed:
            self.busy.discard(freed["name"])
        # strip buttons from whichever message is currently live (no stale clicks after skip)
        for key in ("tag_msg_ts", "outcome_msg_ts", "log_msg_ts"):
            ts = state.get(key)
            if ts and ts in state["reg"]:
                self._resolve_buttons(state, ts, "⏭️ skipped")
        state["step"] = "skipped"
        if freed:
            self._post(state, "⏭️ Call skipped by %s (possible duplicate). %s is free for the next call."
                       % (mention_user(by), render_mention(freed)))
        else:
            self._post(state, "⏭️ Call skipped by %s (possible duplicate). No one was occupied by this call."
                       % mention_user(by))
        return state

    # ---- CSM not available (tagged CSM click, OR timeout) ----
    def on_csm_not_available(self, state, by=None, system=False):
        if state["step"] != "tagging" or state["mode"] != "csm":
            return state
        if not system and not self._authorized(by, state["tagged_sid"]):
            return self._reject(state, by, "the tagged CSM")
        person = self._cur_person(state)
        # resolve the current tag msg's buttons -> text (not deleted, only buttons change)
        self._resolve_buttons(state, state["tag_msg_ts"], "~CSM join~    ❌ CSM not available")
        self._post(state, ("⏱️ System marked %s not available (no response in time)."
                           if system else "❌ %s not available.") % render_mention(person))
        nxt_idx, nxt = pick_available(self.ladder, self.busy, state["attempt_idx"] + 1)
        if nxt is not None:
            state["attempt_idx"] = nxt_idx
            state["tagged_sid"] = nxt.get("slack_id")
            state["tag_ts"] = time.time()
            self._tag_csm(state, nxt)          # append a NEW tag msg for the next CSM
        elif self.tech:
            self._post(state, "⚠️ All CSMs unavailable - falling back to Tech.")
            self._tag_tech(state)              # append the tech group tag msg
        else:
            state["step"] = "exhausted"
            self._post(state, "⚠️ No more CSMs available on the rota for this call.")
        return state

    # ---- CSM join (tagged CSM, or any tagged Tech in fallback) ----
    def on_csm_join(self, state, by=None):
        if state["step"] != "tagging":
            return state
        if state["mode"] == "csm":
            if not self._authorized(by, state["tagged_sid"]):
                return self._reject(state, by, "the tagged CSM")
            person = self._cur_person(state)
            resolved = "✅ CSM joined    ~CSM not available~"
        else:
            person = next((t for t in self.tech if t.get("slack_id") == (by or {}).get("id")), None)
            if person is None:
                return self._reject(state, by, "a tagged Tech")
            resolved = "✅ CSM joined"
        self._resolve_buttons(state, state["tag_msg_ts"], resolved)
        self.busy.add(person["name"])
        state["assigned"] = person["name"]
        state["assigned_person"] = person
        state["assigned_sid"] = person.get("slack_id")
        state["join_ts"] = time.time()
        state["step"] = "in_call"
        self._post(state, "✅ %s joined the call." % render_mention(person))
        prompt = "%s is on the call - how did it go?" % render_mention(person)
        state["outcome_msg_ts"] = self._post_buttons(state, prompt, OUTCOME_SPECS)
        return state

    def _assigned(self, state):
        return state["assigned_person"]

    def _free(self, state, person, line):
        self.busy.discard(person["name"])
        state["step"] = "done"
        self._post(state, line)

    # ---- No show / User dropped (assigned person only) ----
    def on_no_show(self, state, by=None):
        if not self._authorized(by, state["assigned_sid"]):
            return self._reject(state, by, "the person on the call")
        p = self._assigned(state)
        self._resolve(state, state["outcome_msg_ts"], {"dl_no_show"})
        self._free(state, p, "🚫 No show - logged by %s. %s is free for the next call."
                   % (render_mention(p), render_mention(p)))
        return state

    def on_user_dropped(self, state, by=None):
        if not self._authorized(by, state["assigned_sid"]):
            return self._reject(state, by, "the person on the call")
        p = self._assigned(state)
        self._resolve(state, state["outcome_msg_ts"], {"dl_user_dropped"})
        self._free(state, p, "🌐 User dropped - logged by %s. %s is free for the next call."
                   % (render_mention(p), render_mention(p)))
        return state

    # ---- Closure (assigned person only) ----
    def on_closure(self, state, by=None):
        if not self._authorized(by, state["assigned_sid"]):
            return self._reject(state, by, "the person on the call")
        person = self._assigned(state)
        mins = int((time.time() - state["join_ts"]) / 60) if state["join_ts"] else 0
        state["call_mins"] = mins
        state["step"] = "awaiting_logs"
        self._resolve(state, state["outcome_msg_ts"], {"dl_closure"})
        self._post(state, "💬 Closure - the call ran *%s* (%s)."
                   % (fmt_mins(mins), render_mention(person)))
        prompt = "%s → post both when the call ends:" % render_mention(person)
        state["log_msg_ts"] = self._post_buttons(state, prompt, LOG_SPECS)
        return state

    # ---- Summary / Ticket (assigned person only; independent) ----
    def on_summary(self, state, by=None):
        if not self._authorized(by, state["assigned_sid"]):
            return self._reject(state, by, "the person on the call")
        state["summary_posted"] = True
        self._post(state, "📝 Summary posted by %s." % render_mention(self._assigned(state)))
        return self._after_log(state)

    def on_ticket(self, state, by=None):
        if not self._authorized(by, state["assigned_sid"]):
            return self._reject(state, by, "the person on the call")
        state["ticket_posted"] = True
        self._post(state, "🎫 Ticket link posted by %s." % render_mention(self._assigned(state)))
        return self._after_log(state)

    def _after_log(self, state):
        person = self._assigned(state)
        clicked = set()
        if state["summary_posted"]:
            clicked.add("dl_summary")
        if state["ticket_posted"]:
            clicked.add("dl_ticket")
        self._resolve(state, state["log_msg_ts"], clicked, exclusive=False)
        if state["summary_posted"] and state["ticket_posted"]:
            self._free(state, person,
                       "✅ Thanks %s - summary + ticket logged. %s is free for the next call."
                       % (render_mention(person), render_mention(person)))
        return state
