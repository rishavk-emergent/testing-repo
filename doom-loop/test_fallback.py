"""
Test the Tech fallback: when all CSMs are busy or marked not available, the control
tags the whole Tech list and any tagged Tech can CSM join. Posts its own dummy
alerts. Two scenarios: (A) CSMs exhausted via not-available, (B) all CSMs busy at
claim time.

Usage: python3 test_fallback.py <channel_id> [pause_seconds]
"""
import json
import os
import sys
import time

from doom_loop_bot import DoomLoopEngine, slack_call

TOKEN = open(os.path.expanduser("~/.emergent_cs_token")).read().strip()
RISHAV = {"id": "U0ASTT4F14L", "name": "Rishav"}
ALERT = ":telephone_receiver: *Support call claimed!*  _(TEST - {label})_"


def dummy(channel, label):
    return slack_call(TOKEN, "chat.postMessage",
                      {"channel": channel, "text": ALERT.format(label=label)})["ts"]


def main():
    channel = sys.argv[1]
    pause = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
    roster = json.load(open(os.path.join(os.path.dirname(__file__), "roster_test.json")))
    csm, tech = roster["csm_ladder"], roster["tech_ladder"]
    by = {p["name"]: {"id": p["slack_id"], "name": p["name"]} for p in csm + tech}

    # ---- Scenario A: exhaust all CSMs via "not available" -> Tech fallback ----
    print("== A: CSM exhaustion -> Tech fallback ==")
    eng = DoomLoopEngine(TOKEN, channel, csm, tech)
    sA = eng.on_claimed(dummy(channel, "A: exhaust CSMs"), "Support call claimed!")
    time.sleep(pause)
    for n in ("Nandani", "Khyati", "Aarav"):
        eng.on_csm_not_available(sA, by[n]); print("   %s not available" % n); time.sleep(pause)
    print("   mode now:", sA["mode"], "(expect tech)")
    eng.on_csm_join(sA, RISHAV); print("   non-tech Rishav CSM join -> ignored, step:", sA["step"]); time.sleep(pause)
    eng.on_csm_join(sA, by["Rohit"]); print("   Rohit (tech) CSM join -> assigned:", sA["assigned"], "busy:", eng.busy); time.sleep(pause)
    eng.on_closure(sA, by["Rohit"]); time.sleep(pause)
    eng.on_summary(sA, by["Rohit"]); time.sleep(pause)
    eng.on_ticket(sA, by["Rohit"]); print("   after logs busy:", eng.busy, "step:", sA["step"])

    # ---- Scenario B: all CSMs busy at claim -> straight to Tech ----
    print("\n== B: all CSMs busy at claim -> Tech ==")
    eng2 = DoomLoopEngine(TOKEN, channel, csm, tech)
    eng2.busy = {"Nandani", "Khyati", "Aarav"}            # pretend all CSMs on calls
    sB = eng2.on_claimed(dummy(channel, "B: all CSMs busy"), "Support call claimed!")
    print("   mode at claim:", sB["mode"], "(expect tech)"); time.sleep(pause)
    eng2.on_csm_join(sB, by["Meera"]); print("   Meera (tech) joined:", sB["assigned"]); time.sleep(pause)
    eng2.on_no_show(sB, by["Meera"]); print("   no show -> busy:", eng2.busy, "step:", sB["step"])

    print("\nDone.")


if __name__ == "__main__":
    main()
