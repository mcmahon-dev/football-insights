import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
import pandas as pd

# ==== Config (env-first, sensible defaults for local runs) ====
API_KEY   = os.getenv("API_FOOTBALL_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "").strip()  # e.g. "api-football-v1.p.rapidapi.com"
LEAGUE_ID = int(os.getenv("LEAGUE_ID", 39))
SEASON    = int(os.getenv("SEASON", 2023))
ROUND     = os.getenv("ROUND", "Regular Season - 1")

OUT_DIR  = os.path.join(os.path.dirname(__file__), "raw-data")
OUT_PATH = os.path.join(OUT_DIR, "latest_player_by_round.csv")

API_BASE = "https://v3.football.api-sports.io"
HEADERS = (
    {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    if RAPIDAPI_HOST and API_KEY else
    {"x-apisports-key": API_KEY} if API_KEY else {}
)

# ==== Helpers ====
def _require_env():
    if not API_KEY:
        print("❌ Missing API_FOOTBALL_KEY in environment.", file=sys.stderr)
        sys.exit(1)

def _get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """GET with basic retry + status check; returns parsed JSON."""
    url = f"{API_BASE}/{path.lstrip('/')}"
    last_err = None
    for attempt in range(5):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=45)
            # Retry on 429/5xx
            if r.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} from API", response=r)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.5 * (2 ** attempt))
    # One last attempt to show body for debugging if available
    try:
        txt = r.text[:4000]
    except Exception:
        txt = "<no body>"
    raise RuntimeError(f"GET {url} failed. Last error: {last_err}. Body: {txt}")

def _safe_first(lst):
    return lst[0] if isinstance(lst, list) and lst else {}

def _coerce_float(x):
    try:
        return float(x) if x is not None and x != "" else None
    except Exception:
        return None

# ==== Main flow ====
def main():
    _require_env()
    os.makedirs(OUT_DIR, exist_ok=True)

    # 1) Fixtures for round
    fx_json = _get("/fixtures", {"league": LEAGUE_ID, "season": SEASON, "round": ROUND})
    fixtures = fx_json.get("response", []) or []
    if not fixtures:
        print("⚠️  No fixtures found. Check LEAGUE_ID/SEASON/ROUND.")
        # Write an empty CSV with headers so downstream steps don’t break
        pd.DataFrame(columns=[
            "season","round","fixture_id","team","player","position","minutes",
            "rating","goals","assists","fetched_datetime"
        ]).to_csv(OUT_PATH, index=False, encoding="utf-8")
        print(f"✅ Wrote empty CSV → {OUT_PATH}")
        return

    rows: List[Dict[str, Any]] = []
    fetched_ts = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    # 2) Players per fixture
    for f in fixtures:
        fixture = f.get("fixture") or {}
        fixture_id = fixture.get("id")
        if not fixture_id:
            continue

        pl_json = _get("/fixtures/players", {"fixture": fixture_id})
        teams = pl_json.get("response", []) or []

        for t in teams:
            team_info = t.get("team") or {}
            team_name = team_info.get("name")
            players = t.get("players") or []

            for p in players:
                player_info = p.get("player") or {}
                stats = _safe_first(p.get("statistics") or [])
                games = stats.get("games", {}) or {}
                goals = stats.get("goals", {}) or {}

                rows.append({
                    "season": SEASON,
                    "round": ROUND,
                    "fixture_id": fixture_id,
                    "team": team_name,
                    "player": player_info.get("name"),
                    "position": games.get("position"),
                    "minutes": games.get("minutes"),
                    "rating": _coerce_float(games.get("rating")),  # coerce to float if possible
                    "goals": goals.get("total"),
                    "assists": goals.get("assists"),
                    "fetched_datetime": fetched_ts,
                })
        # gentle pacing
        time.sleep(0.15)

    # 3) DataFrame + CSV
    df = pd.DataFrame(rows, columns=[
        "season","round","fixture_id","team","player","position",
        "minutes","rating","goals","assists","fetched_datetime"
    ])
    df.to_csv(OUT_PATH, index=False, encoding="utf-8")
    print(f"✅ Saved {len(df)} rows → {OUT_PATH}")

if __name__ == "__main__":
    main()
