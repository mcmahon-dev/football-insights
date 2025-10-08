import os
import sys
import json
import time
import re
import requests
from datetime import datetime
from pathlib import Path

# === 🔧 Config via env ===
API_KEY = os.getenv("API_FOOTBALL_KEY")
LEAGUE_ID = int(os.getenv("LEAGUE_ID", 39))  # EPL default
SEASON = int(os.getenv("SEASON", 2023))
ROUND = os.getenv("ROUND", "Regular Season - 1")

if not API_KEY:
    print("❌ Missing API_FOOTBALL_KEY in environment.")
    sys.exit(1)

# === Constants/paths ===
BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

ROOT_DIR = Path(__file__).resolve().parent
RAW_DIR = ROOT_DIR / "raw-data" / "api-football"

# Make a run folder: raw-data/api-football/2023_Regular_Season_1/20251008_153400/
def safe(s: str) -> str:
    s = re.sub(r"\s+", "_", s.strip())
    s = s.replace("-", "_")
    return re.sub(r"[^A-Za-z0-9_]", "", s)

ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
run_base = RAW_DIR / f"{SEASON}_{safe(ROUND)}" / ts
run_base.mkdir(parents=True, exist_ok=True)

def get_json(path, params=None):
    r = requests.get(f"{BASE}{path}", headers=HEADERS, params=params or {})
    print(f"👉 GET {r.url} [status={r.status_code}]")
    try:
        j = r.json()
    except Exception as e:
        print(f"❌ JSON parse error: {e}")
        print("Body:", r.text[:500])
        sys.exit(1)
    # Helpful API-FOOTBALL top-level fields
    if isinstance(j, dict):
        print("   results:", j.get("results"), "| paging:", j.get("paging"), "| errors:", j.get("errors"))
        if j.get("errors"):
            print("❌ API errors present, stopping.")
            sys.exit(1)
    if r.status_code != 200:
        print("❌ HTTP error. Body snippet:", str(j)[:300])
        sys.exit(1)
    return j

print(f"⚽ Fetching RAW for league={LEAGUE_ID}, season={SEASON}, round='{ROUND}'")
print("📂 Run folder:", run_base)

# --- 0) Status probe (auth sanity) ---
status = get_json("/status")
with open(run_base / "status.json", "w", encoding="utf-8") as f:
    json.dump(status, f, ensure_ascii=False, indent=2)

# --- 1) Fixtures for the round ---
fixtures_json = get_json("/fixtures", params={"league": LEAGUE_ID, "season": SEASON, "round": ROUND})
with open(run_base / "fixtures.json", "w", encoding="utf-8") as f:
    json.dump(fixtures_json, f, ensure_ascii=False, indent=2)

fixtures = fixtures_json.get("response", []) or []
print(f"📈 Fixtures found: {len(fixtures)}")

if not fixtures:
    # Save valid rounds to help debug round mismatches
    rounds_json = get_json("/fixtures/rounds", params={"league": LEAGUE_ID, "season": SEASON})
    with open(run_base / "valid_rounds.json", "w", encoding="utf-8") as f:
        json.dump(rounds_json, f, ensure_ascii=False, indent=2)
    print("⚠️ No fixtures for that round. Wrote valid_rounds.json for inspection.")
    sys.exit(0)

# --- 2) Players per fixture (raw) ---
players_dir = run_base / "players_by_fixture"
players_dir.mkdir(exist_ok=True)

count = 0
for fx in fixtures:
    fixture_id = fx["fixture"]["id"]
    j = get_json("/fixtures/players", params={"fixture": fixture_id})
    with open(players_dir / f"players_{fixture_id}.json", "w", encoding="utf-8") as f:
        json.dump(j, f, ensure_ascii=False, indent=2)
    count += 1
    # small delay to be gentle with rate limits
    time.sleep(0.25)

print(f"✅ Saved raw JSON: fixtures.json + {count} players_{'{fixture_id}'}.json files")
print("🧭 Inspect these files to verify the API really returned data before any ETL.")
