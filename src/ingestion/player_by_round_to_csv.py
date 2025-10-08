import os
import sys
import requests
import pandas as pd
from datetime import datetime

# === 🔧 Config ===
API_KEY = os.getenv("API_FOOTBALL_KEY")  # from GitHub Secrets
LEAGUE_ID = int(os.getenv("LEAGUE_ID", 39))     # default EPL
SEASON = int(os.getenv("SEASON", 2023))
ROUND = os.getenv("ROUND", "Regular Season - 1")

# === Paths ===
OUT_DIR = os.path.join(os.path.dirname(__file__), "raw-data")
OUT_PATH = os.path.join(OUT_DIR, "latest_player_by_round.csv")

# === Checks ===
if not API_KEY:
    print("❌ Missing API_FOOTBALL_KEY in environment.")
    sys.exit(1)

os.makedirs(OUT_DIR, exist_ok=True)

# === API setup ===
BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

print(f"⚽ Fetching players for League {LEAGUE_ID}, Season {SEASON}, Round '{ROUND}'")
print("🔗 Base URL:", BASE)
print("📦 Output path:", OUT_PATH)

# --- 1️⃣ Get fixtures for the round ---
print("\n--- Step 1: Fetching fixtures ---")
fixtures_response = requests.get(
    f"{BASE}/fixtures",
    headers=HEADERS,
    params={"league": LEAGUE_ID, "season": SEASON, "round": ROUND}
)

print("📡 Fixtures request URL:", fixtures_response.url)
print("📊 Fixtures status code:", fixtures_response.status_code)

try:
    fixtures_json = fixtures_response.json()
except Exception as e:
    print("❌ Error parsing fixtures JSON:", e)
    sys.exit(1)

print("🧩 Raw fixtures JSON keys:", list(fixtures_json.keys()))
fixtures = fixtures_json.get("response", [])
print(f"📈 Fixtures found: {len(fixtures)}")

if not fixtures:
    print("⚠️  No fixtures found. Check season/round name or API quota.")
    sys.exit(0)

rows = []

# --- 2️⃣ Get players for each fixture ---
print("\n--- Step 2: Fetching players per fixture ---")
for fx_idx, fx in enumerate(fixtures, start=1):
    fixture_id = fx["fixture"]["id"]
    print(f"\n➡️ [{fx_idx}/{len(fixtures)}] Fixture ID: {fixture_id}")

    players_response = requests.get(
        f"{BASE}/fixtures/players",
        headers=HEADERS,
        params={"fixture": fixture_id}
    )

    print("   ↳ Players request URL:", players_response.url)
    print("   ↳ Status code:", players_response.status_code)

    try:
        data_json = players_response.json()
    except Exception as e:
        print("   ❌ Error parsing players JSON:", e)
        continue

    if "response" not in data_json:
        print("   ⚠️ 'response' key missing in JSON.")
        continue

    data = data_json.get("response", [])
    print(f"   🧩 Teams in response: {len(data)}")

    for team in data:
        team_name = team["team"]["name"]
        player_count = len(team["players"])
        print(f"   👥 Team '{team_name}' has {player_count} players")
        for p in team["players"]:
            player = p["player"]
            stats = p["statistics"][0] if p["statistics"] else {}
            games = stats.get("games", {})
            goals = stats.get("goals", {})
            rows.append({
                "season": SEASON,
                "round": ROUND,
                "fixture_id": fixture_id,
                "team": team_name,
                "player": player.get("name"),
                "position": games.get("position"),
                "minutes": games.get("minutes"),
                "rating": games.get("rating"),
                "goals": goals.get("total"),
                "assists": goals.get("assists"),
            })

print("\n--- Step 3: Data summary ---")
print(f"🧾 Total player rows collected: {len(rows)}")

# --- 3️⃣ Create DataFrame and add timestamp ---
df = pd.DataFrame(rows)
if df.empty:
    print("⚠️  DataFrame is empty. No player data to save.")
else:
    print("✅ DataFrame created successfully with columns:")
    print(df.columns.tolist())
    print(df.head(5))

df["fetched_datetime"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# --- 4️⃣ Save to CSV ---
df.to_csv(OUT_PATH, index=False)
print(f"\n✅ Saved {len(df)} player rows → {OUT_PATH}")
print("🏁 Script complete.")
