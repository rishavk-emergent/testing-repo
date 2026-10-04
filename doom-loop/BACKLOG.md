# Doom-Loop — queued feature requests (build in bulk)

Status key: 🟡 queued · 🔵 building · ✅ done

## ✅ 1. "Others join" global button — BUILT + tested (engine)
DECIDED: pure log line — posts `@clicker joined` (real `<@id>` mention), button never disables, stays live on tag messages. Does NOT mark busy, does NOT change Closure/Summary/Ticket ownership.
(original spec below)

### (orig) "Others join" global button
- Plain text button labelled **Others join** (NO emoji).
- **Global**: anyone can click it (not restricted to the tagged CSM).
- **Stays active** — never disabled/struck; multiple people can click it over time.
- On each click → post small thread message: **"@<clicker name> joined"** (uses relay `user`).
- Sits on the tag message alongside `Joined` / `Not available`.
- OPEN: does clicking it also mark that person **busy** (pull them from the available ladder) and/or make them an owner of Closure/Summary/Ticket? — awaiting Rishav.

## ✅ 2. Per-person button enforcement — BUILT + tested (engine)
DECIDED: extends downstream (Joined/Not available → tagged CSM; No show/User dropped/Closure/Summary/Ticket → assigned CSM; Others join → anyone). Unauthorized = silent ignore (ENFORCE_NOTIFY flag flips to a note). STILL NEEDS real name→Slack-ID map (placeholder ids in roster_test.json for now).
(original spec below)

### (orig) Per-person button enforcement
- `Joined` / `Not available` on the tag message are only honoured when the **clicker's Slack ID == the tagged CSM's Slack ID** (relay gives `Payload User Id`).
- `Others join` is exempt (global).
- OPEN: does the restriction extend to the downstream buttons (No show / User dropped / Closure / Summary / Ticket = only the assigned CSM)? — awaiting Rishav.
- OPEN: unauthorized click → silently ignore vs. small "only @tagged can respond" note? — awaiting Rishav.
- **DEPENDENCY (hard):** requires the name→Slack-ID map we deferred. Need emails (then `users.lookupByEmail`) or a `slack_id` field per roster person.

## 🟡 3. Auto "not available" on no response (timeout)
- When a CSM is tagged on a "Support call claimed!" alert, start a **1-min timer**.
- If they don't click `Joined` or `Not available` within 1 min → system auto-marks them **not available**.
- Post small message: **"System marked <name> not available (no response in 1 min)."**
- Then tag the **next** person in the ladder (fresh 1-min timer for them).
- Timeout value configurable (default 60s).
- **Polaris UI control**: ✅ BUILT (preview) — number input "Auto not available after [60] seconds" + Save in the Doom-Loop Process Bot panel; stored in `doomloop_config.response_timeout_seconds` (clamp 10–3600). Backend `status`/`settings` updated; `api.js doomloopSettings` now takes a patch object. (Still needs: prod deploy.)
- ✅ Mechanics HOOK built (engine): `on_csm_not_available(state, system=True)` posts "System marked @X not available" + re-tags; `state["tag_ts"]` reset on each (re)tag. 🟡 Remaining: the poller loop must call it when `now - tag_ts > response_timeout_seconds`.

## ✅ 5. Simplified 3-button control model — BUILT + tested (engine)
One persistent control message `[Others join] [CSM join] [CSM not available]` + tag line. CSM not available (click or timeout) re-tags next, buttons persist; CSM join locks the two CSM buttons to "CSM joined" text, Others join stays; in-call flow (outcome/closure/logs) unchanged. Replaces the earlier per-tag Joined/Not-available layout.
- OPEN: when a tag times out, does the `Others join` button on that old message stay live (call still unclaimed), or lock with it? — awaiting Rishav.

## ✅ 4. Editable channel(s) in Polaris UI — BUILT (preview)
- Two editable, validated inputs + "Save channels" in the panel:
  - **Post/alert channel** = `main_channel_id` (default `C0C5YULP61G`).
  - **Relay channel** = `relay_channel_id` (default `C0C6DHJU8SX`).
- Backend `status`/`settings` handle both; validates `^[CGD][A-Z0-9]{6,}$`, auto-uppercases.
- TODO later: membership check (confirm Emergent-CS is in the channel — needs the bot token, do with engine); prod deploy.

## ✅ 6. Tech fallback — BUILT + tested (engine)
When all CSMs are busy or marked not available (exhausted via "not available", OR none free at claim), the control tags the whole slot Tech list at once: `[Others join] [CSM join]` + "No CSM available - <@tech...> please join". Any tagged Tech can CSM join (auth widens to the tech set); CSM not-available/timeout disabled in tech mode. Needs `tech_ladder` in roster data (added to roster_test.json; real roster artifact already has per-slot tech).

## ✅ 7. Availability store (Polaris Mongo) — BUILT + tested (preview)
`doomloop_availability` collection = source of truth. Route `/app/backend/routes/doomloop_availability.py` live, registered in server.py. Endpoints: GET /doomloop/availability[?role], POST /seed, PUT /status, GET /next?role. Helpers: av_next_available, av_all_unavailable, av_claim, av_free, av_set, av_upsert. Verified: next skips busy, all-busy -> tech fallback signal, free/out transitions, persists in Mongo. Seeded 3 CSM + 2 Tech (test), reset to all-available. TODO: poller wiring to use it (replace engine in-memory busy); real roster seed; prod deploy.

## ✅ 8. Full-week roster in Polaris DB — DONE
`doomloop_roster` collection seeded (mon-fri, 7 IST slots each) from the artifact, every person resolved to a real Slack ID + email via Slack MCP (34 unique). Each doc: {_id:day, people:{id:{name,role,slack_id,email}}, slots:{sKK:{label,csm[],tech[]}}}. Local copies: doom-loop/<day>_resolved.json. Update by editing the collection in prod (artifact<->DB not auto-synced). Bot maps current IST -> weekday+slot -> ids -> slack_id for real @pings.
- s00 belongs to the weekday's night (12-2 AM). Slot keys: s12/s14/s16/s18/s20/s22/s00.
- Still TODO: poller wiring to read roster+availability and tag; name->slack mapping now solved for the current roster.

## ✅ 9. Repeat ("clicked again") flow + Skip — BUILT + tested (engine, real pings)
on_claimed(is_repeat=True): same tagging/escalation/fallback flow, but the top control adds a global **Skip** button (danger). on_skip(anyone): frees everyone this thread made busy (assigned_person -> available), strips buttons from the active tag/outcome/log message to "skipped", step=skipped. Skip only on repeat messages. alert_kind() distinguishes claimed vs repeat.

## ✅ 10. Real roster-driven tagging + real @pings — tested (engine)
render_mention now emits <@slack_id>. Tested full claimed flow, tech fallback, and repeat+skip in the master/repeat threads using the live Polaris doomloop_roster (current IST slot -> ladder -> real pings). Slot rule reminder for the poller: 12AM-2AM (s00) belongs to the PREVIOUS weekday (per artifact slotAt); naive weekday is wrong.

---
_Add new asks below as they come; nothing is built until Rishav says "build the batch."_
