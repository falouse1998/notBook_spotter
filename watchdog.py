#!/usr/bin/env python3
"""Tells you on Telegram when the sniper stops, wedges, or crashes.

`Restart=always` means a crash heals itself in seconds and you would never know
it happened, so this reports crashes as well as outages. It runs from a systemd
timer rather than inside the container, because a watchdog that shares a process
with the thing it watches dies with it.

Only transitions are announced. A service that is down stays down without
sending the same message every minute.
"""

import json
import os
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

UNIT        = "notebook_spotter.service"
STATE_FILE  = "/opt/notebook_spotter/state/watchdog.json"

# A restart takes ~3 minutes to bootstrap five browsers before the first cycle
# is logged, so anything under this is silence we expect, not a wedge.
BOOTSTRAP_GRACE = 420
# Cycles land every ~10s. A gap this long means it is running but not working.
STALE_AFTER     = 300
# Proof of life, so total silence is itself a signal that the box is gone.
HEARTBEAT_EVERY = 86400


def credentials():
    """Same env vars main.py reads, via the same telegram.env — so there is
    only ever one copy of the real token, and neither file hardcodes it."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat  = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat:
        sys.exit("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not set in the environment")
    return token, chat


def send(text):
    token, chat = credentials()
    data = urllib.parse.urlencode({"chat_id": chat, "text": text}).encode()
    req  = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/sendMessage", data=data)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status == 200
    except Exception as exc:
        print(f"telegram failed: {exc}", file=sys.stderr)
        return False


def sh(*args):
    try:
        return subprocess.run(args, capture_output=True, text=True,
                              timeout=20).stdout.strip()
    except Exception:
        return ""


def prop(name):
    return sh("systemctl", "show", UNIT, "-p", name, "--value")


def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


def uptime_seconds():
    """How long the unit has been active, or None if it is not."""
    raw = prop("ActiveEnterTimestamp")
    if not raw:
        return None
    for fmt in ("%a %Y-%m-%d %H:%M:%S %Z", "%a %Y-%m-%d %H:%M:%S UTC"):
        try:
            t = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return (datetime.now(timezone.utc) - t).total_seconds()
        except ValueError:
            continue
    return None


def last_cycle_age():
    """Seconds since the last completed fetch cycle, or None if none found."""
    out = sh("journalctl", "-u", UNIT, "--no-pager", "-o", "short-unix",
             "--since", "-30min", "-g", "Done in")
    stamps = [float(m) for m in re.findall(r"^(\d+\.\d+)", out, re.M)]
    return time.time() - max(stamps) if stamps else None


def recent_errors():
    out = sh("journalctl", "-u", UNIT, "--no-pager", "-o", "cat", "-n", "6")
    return "\n".join(out.splitlines()[-6:])


def main():
    state    = load_state()
    active   = prop("ActiveState")
    sub      = prop("SubState")
    restarts = prop("NRestarts") or "0"
    up       = uptime_seconds()

    problem = None
    if active != "active":
        problem = f"service is {active}/{sub}"
    elif up is not None and up > BOOTSTRAP_GRACE:
        age = last_cycle_age()
        if age is None:
            problem = "running but has not logged a single cycle"
        elif age > STALE_AFTER:
            problem = f"running but last cycle was {int(age // 60)}m ago"

    was_ok       = state.get("ok", True)
    now_ok       = problem is None
    prev_restart = state.get("restarts", restarts)

    # A crash that healed itself still deserves a mention.
    if restarts != prev_restart and now_ok:
        send(f"⚠️ notebook_spotter crashed and restarted itself "
             f"(restart #{restarts})\n\nIt is running normally again.\n\n"
             f"{recent_errors()}")

    elif was_ok and not now_ok:
        send(f"🔴 notebook_spotter PROBLEM\n\n{problem}\n\n"
             f"Recent log:\n{recent_errors()}\n\n"
             f"Check:  sudo systemctl status notebook_spotter")

    elif not was_ok and now_ok:
        mins = int(up // 60) if up else 0
        send(f"🟢 notebook_spotter recovered\n\nRunning again, up {mins}m.")

    # Silence should not be mistaken for health, so say something once a day.
    last_beat = state.get("heartbeat", 0)
    if now_ok and time.time() - last_beat > HEARTBEAT_EVERY:
        age      = last_cycle_age()
        age_text = f"{int(age)}s ago" if age is not None else "not logged yet"
        mins     = int(up // 60) if up else 0
        send(f"✅ notebook_spotter daily check\n\n"
             f"Up {mins // 60}h{mins % 60:02d}m, last cycle "
             f"{age_text}, {restarts} restart(s) total.")
        state["heartbeat"] = time.time()

    state["ok"]       = now_ok
    state["restarts"] = restarts
    save_state(state)
    print(f"{'OK' if now_ok else 'PROBLEM: ' + problem}  "
          f"(active={active}/{sub}, restarts={restarts})")


if __name__ == "__main__":
    main()
