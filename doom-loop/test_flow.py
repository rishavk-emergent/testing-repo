"""
Live test driver for the Doom-Loop bot (clicks simulated; Zapier relay comes later).
Posts with the Emergent-CS bot token into the alert's thread so you can watch live.

Exercises: the persistent 3-button control message, enforcement (unauthorized
click ignored), Others join (global), manual "CSM not available", system timeout
"not available", CSM join, and the full in-call -> closure -> logs -> free flow.

Usage: python3 test_flow.py <channel_id> <alert_ts> [pause_seconds]
"""
import json
import os
import sys
import time

from doom_loop_bot import DoomLoopEngine

TOKEN = open(os.path.expanduser("~/.emergent_cs_token")).read().strip()
RISHAV = {"id": "U0ASTT4F14L", "name": "Rishav"}   # real id -> renders a real mention


def main():
    channel, alert_ts = sys.argv[1], sys.argv[2]
    pause = float(sys.argv[3]) if len(sys.argv) > 3 else 4.0

    roster = json.load(open(os.path.join(os.path.dirname(__file__), "roster_test.json")))
    ladder = roster["csm_ladder"]
    eng = DoomLoopEngine(TOKEN, channel, ladder)
    by = {p["name"]: {"id": p["slack_id"], "name": p["name"]} for p in ladder}

    def step(label, fn):
        print("->", label); fn(); time.sleep(pause)

    print("Ladder:", [p["name"] for p in ladder])

    # 1. Trigger -> control message tags P1 (Nandani)
    state = eng.on_claimed(alert_ts, "Support call claimed!")
    print("   tagged:", eng._cur_person(state)["name"]); time.sleep(pause)

    # 2. ENFORCEMENT: non-tagged user clicks CSM join -> ignored
    step("Unauthorized CSM join by Rishav (not tagged) -> ignored",
         lambda: eng.on_csm_join(state, RISHAV))
    print("   busy (should be empty):", eng.busy, "| step:", state["step"])

    # 3. OTHERS JOIN: anyone -> posts "@clicker joined"
    step("Others join by Rishav -> posts '@Rishav joined'",
         lambda: eng.on_others_join(state, RISHAV))

    # 4. Nandani clicks CSM not available (manual) -> re-tag Khyati, buttons persist
    step("Nandani clicks CSM not available (manual) -> re-tag Khyati",
         lambda: eng.on_csm_not_available(state, by["Nandani"]))
    print("   now tagged:", eng._cur_person(state)["name"])

    # 5. Khyati times out (system) -> system marked not available -> re-tag Aarav
    step("Khyati no response -> SYSTEM marks not available -> re-tag Aarav",
         lambda: eng.on_csm_not_available(state, system=True))
    print("   now tagged:", eng._cur_person(state)["name"])

    # 6. Aarav clicks CSM join -> in-call (CSM buttons -> text, Others join stays)
    step("Aarav clicks CSM join (authorized) -> in-call",
         lambda: eng.on_csm_join(state, by["Aarav"]))
    print("   assigned:", state["assigned"], "| busy:", eng.busy)

    # 7. Aarav clicks Closure
    step("Aarav clicks Closure -> ask for Summary + Ticket",
         lambda: eng.on_closure(state, by["Aarav"]))

    # 8a. Summary (stays busy)
    step("Summary posted by Aarav (stays busy)",
         lambda: eng.on_summary(state, by["Aarav"]))
    print("   busy (should still have Aarav):", eng.busy)

    # 8b. Ticket -> both -> freed
    step("Ticket posted by Aarav (both done -> free)",
         lambda: eng.on_ticket(state, by["Aarav"]))
    print("   busy (should be empty):", eng.busy, "| step:", state["step"])

    print("\nDone. Full flow posted to thread", alert_ts)


if __name__ == "__main__":
    main()
