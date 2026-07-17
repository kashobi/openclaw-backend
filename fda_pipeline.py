"""
Apex Q cron runner.

RAILWAY SETUP: one cron service, schedule = "0 * * * *" (top of every hour),
start command = python cron_runner.py

Each hourly run fires only the endpoints DUE that hour, so hourly / twice-daily /
daily / weekly cadences all come from a single hourly cron. To change what runs
when, edit the SCHEDULE below -- all times are UTC.

MUST DO: set CRON_SECRET in THIS service's Variables. Railway env vars are
per-service; the web service having it is not enough. If it's missing, every
call 403s and this script aborts with a non-zero exit so the run shows red.

TIMEZONE NOTE: ET is UTC-4 in summer (EDT) and UTC-5 in winter (EST). The UTC
hours below are tuned for EDT. After the November DST change, everything fires
one clock-hour later in ET -- shift the hours by +1 then, or accept the drift.
"""

import os
import sys
import requests
from datetime import datetime, timezone

BASE = os.environ.get("CRON_BASE", "https://apexq.io")
TOKEN = os.environ.get("CRON_SECRET", "")

now = datetime.now(timezone.utc)
HOUR = now.hour        # 0-23 UTC
DOW = now.weekday()    # Mon=0 ... Sun=6

# endpoint : a test that returns True when it should run THIS hour
SCHEDULE = {
    # hourly -- runs on every pass
    "/cron/analyst-snapshot":  lambda: True,
    "/cron/legislative-risk":  lambda: True,   # pull prediction markets every hour

    # twice daily -- ~7:00am ET (pre-open) and ~5:00pm ET (post-close)
    "/cron/portfolio-refresh": lambda: HOUR in (11, 21),

    # once daily -- evening ET batch (~7pm ET)
    "/cron/dod-contracts":     lambda: HOUR == 22,   # ~6pm ET
    "/cron/label-outcomes":    lambda: HOUR == 23,
    "/cron/sec-edgar":         lambda: HOUR == 23,
    "/cron/dod-label-impact":  lambda: HOUR == 23,
    "/cron/clinical-trials":   lambda: HOUR == 23,
    "/cron/ld1-registrations": lambda: HOUR == 23,
    "/cron/earnings":          lambda: HOUR == 23,
    "/cron/earnings-label":    lambda: HOUR == 23,
    "/cron/fda-approvals":     lambda: HOUR == 23,
    "/cron/house-trades":      lambda: HOUR == 23,
    "/cron/senate-trades":     lambda: HOUR == 23,
    "/cron/lda-lobbying":      lambda: HOUR == 23,
    "/cron/morning-briefing":  lambda: HOUR == 11,   # ~7am ET with the open

    # once weekly -- Sunday only (SEC blocks IPs that hammer 10-Ks)
    "/cron/dod-exhibit21":     lambda: DOW == 6 and HOUR == 8,
}


def main():
    if not TOKEN:
        print("CRON_SECRET is empty in this service -- every call would 403. Aborting.")
        sys.exit(1)

    due = [ep for ep, ready in SCHEDULE.items() if ready()]
    if not due:
        print("%s  nothing scheduled this hour" % now.strftime("%Y-%m-%d %H:%M UTC"))
        return

    failures = 0
    for ep in due:
        try:
            r = requests.get("%s%s" % (BASE, ep), params={"token": TOKEN}, timeout=300)
            ok = 200 <= r.status_code < 300
            failures += 0 if ok else 1
            print("%s: %s%s" % (ep, r.status_code, "" if ok else "  <-- FAIL"))
        except Exception as e:
            failures += 1
            print("%s: ERROR %s" % (ep, e))

    if failures:
        sys.exit(1)  # non-zero => Railway marks the run failed, so you SEE it


if __name__ == "__main__":
    main()
