"""
Test the two in-call outcome branches that the main flow skips: No show and
User dropped. Posts its own dummy "Support call claimed!" alerts so the master
thread stays clean. Each: claimed -> CSM join -> <outcome> -> free.

Usage: python3 test_outcomes.py <channel_id> [pause_seconds]
"""
import json
import os
import sys
import time

from doom_loop_bot import DoomLoopEngine, post_thread, slack_call

TOKEN = open(os.path.expanduser("~/.emergent_cs_token")).read().strip()

ALERT = (":telephone_receiver: *Support call claimed!*\n"
         ":bust_in_silhouette: *User:* test.customer@example.com\n"
         ":earth_africa: *Country:* India   :moneybag: *LTV:* $0.00\n"
         "_(TEST alert - {label})_")


def run_branch(channel, ladder, label, outcome_fn, pause):
    # post a standalone dummy alert as the thread parent
    parent = slack_call(TOKEN, "chat.postMessage",
                        {"channel": channel, "text": ALERT.format(label=label)})["ts"]
    eng = DoomLoopEngine(TOKEN, channel, ladder)
    first = {"id": ladder[0]["slack_id"], "name": ladder[0]["name"]}
    print("\n== %s == parent=%s" % (label, parent))
    state = eng.on_claimed(parent, "Support call claimed!")
    time.sleep(pause)
    eng.on_csm_join(state, first)
    print("   %s joined, busy=%s" % (ladder[0]["name"], eng.busy)); time.sleep(pause)
    outcome_fn(eng, state, first)
    print("   after outcome, busy=%s step=%s" % (eng.busy, state["step"]))
    return parent


def main():
    channel = sys.argv[1]
    pause = float(sys.argv[2]) if len(sys.argv) > 2 else 3.0
    roster = json.load(open(os.path.join(os.path.dirname(__file__), "roster_test.json")))
    ladder = roster["csm_ladder"]

    run_branch(channel, ladder, "No show branch",
               lambda e, s, by: e.on_no_show(s, by), pause)
    run_branch(channel, ladder, "User dropped branch",
               lambda e, s, by: e.on_user_dropped(s, by), pause)
    print("\nDone.")


if __name__ == "__main__":
    main()
