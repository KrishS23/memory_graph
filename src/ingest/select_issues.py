from dotenv import load_dotenv
import os
import requests
import json
import time

# -------------------------
# Config
# -------------------------
OWNER = "rust-lang"
REPO = "rust"
MIN_COMMENTS = 5
TARGET = 400
OUTPUT_PATH = "data/raw/selected_issues.json"

# -------------------------
# Auth
# -------------------------
load_dotenv()
TOKEN = os.getenv("GITHUB_TOKEN")

if not TOKEN:
    raise RuntimeError("Missing GITHUB_TOKEN in .env")

headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {TOKEN}",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "layer10-ingestor",
}

# -------------------------
# Main sampling loop
# -------------------------
selected = []
page = 1

while len(selected) < TARGET:
    print(f"Fetching page {page}...")

    url = f"https://api.github.com/repos/{OWNER}/{REPO}/issues"
    params = {
        "state": "all",
        "sort": "updated",
        "direction": "desc",
        "per_page": 100,
        "page": page,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=60)

    # basic rate-limit handling
    if resp.status_code in (403, 429):
        reset = resp.headers.get("x-ratelimit-reset")
        if reset:
            sleep_for = max(0, int(reset) - int(time.time()) + 2)
            print(f"Rate limited. Sleeping {sleep_for}s...")
            time.sleep(sleep_for)
            continue
        else:
            time.sleep(60)
            continue

    resp.raise_for_status()
    items = resp.json()

    if not items:
        break

    for it in items:
        # ❌ skip pull requests
        if "pull_request" in it:
            continue

        # ✅ keep only interesting issues
        if (it.get("comments") or 0) < MIN_COMMENTS:
            continue

        selected.append(it["number"])

        if len(selected) >= TARGET:
            break

    page += 1
os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

with open(OUTPUT_PATH, "w") as f:
    json.dump(selected, f, indent=2)

print(f"\n✅ Selected {len(selected)} issues")
print(f"Saved to {OUTPUT_PATH}")
print("Example:", selected[:10])