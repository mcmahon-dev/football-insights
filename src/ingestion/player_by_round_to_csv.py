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

print(f"Fetching players for League {LEAGUE_ID}, Season {SEASON}, Round '{ROUND}'")

# --- 1️⃣ Get fixtures for the round ---
fixtures = requests.get(
    f"{BASE}/fixtures",
    headers=HEADERS,
    params={"league": LEAGUE_ID, "season": SEASON, "round": ROUND}
).json().get("response", [])

if not fixtures:
    print("⚠️  No fixtures found. Check season/round name.")
    sys.exit(0)

rows = []

# --- 2️⃣ Get players for each fixture ---
for fx in fixtures:
    fixture_id = fx["fixture"]["id"]
    data = requests.get(
        f"{BASE}/fixtures/players",
        headers=HEADERS,
        params={"fixture": fixture_id}
    ).json().get("response", [])

    for team in data:
        team_name = team["team"]["name"]
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

# --- 3️⃣ Create DataFrame and add timestamp ---
df = pd.DataFrame(rows)
df["fetched_datetime"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

# --- 4️⃣ Save to CSV ---
df.to_csv(OUT_PATH, index=False)

print(f"✅ Saved {len(df)} player rows → {OUT_PATH}")
