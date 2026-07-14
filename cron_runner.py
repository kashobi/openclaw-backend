import os, requests

BASE = "https://apexq.io"
TOKEN = os.environ.get("CRON_SECRET", "")

endpoints = [
    "/cron/analyst-snapshot",
    "/cron/label-outcomes",
    "/cron/sec-edgar",
    "/cron/dod-contracts",
    "/cron/portfolio-refresh",
]

for ep in endpoints:
    try:
        r = requests.get(f"{BASE}{ep}?token={TOKEN}", timeout=300)
        print(f"{ep}: {r.status_code}")
    except Exception as e:
        print(f"{ep}: ERROR {e}")
