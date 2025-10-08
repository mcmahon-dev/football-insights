import os
import time
import argparse
import logging
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

# If your events job already imports supabase like this, keep it identical:
from supabase import create_client, Client

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

# ---- ENV (same as your events job) ----
API_KEY = os.getenv("API_FOOTBALL_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST", "").strip()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE") or os.getenv("SUPABASE_KEY")

if not API_KEY:
    raise RuntimeError("Missing API_FOOTBALL_KEY")
if not (SUPABASE_URL and SUPABASE_KEY):
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE/KEY")

# ---- HTTP setup (mirrors events: direct API-Football OR RapidAPI) ----
API_BASE = "https://v3.football.api-sports.io"
HEADERS = (
    {"X-RapidAPI-Key": API_KEY, "X-RapidAPI-Host": RAPIDAPI_HOST}
    if RAPIDAPI_HOST else
    {"x-apisports-key": API_KEY}
)

def apifootball_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{API_BASE}/{path.lstrip('/')}"
    for attempt in range(5):
        r = requests.get(url, headers=HEADERS, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 502, 503, 504):
            wait = 2 ** attempt
            logging.warning("API %s. Retrying in %ss… (%s)", r.status_code, wait, url)
            time.sleep(wait)
            continue
        r.raise_for_status()
    raise RuntimeError(f"GET {url} failed after retries")

# ---- Supabase client (same call signature as events) ----
def supa() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ---- Round resolution & pulls (round -> fixtures -> players) ----
def list_rounds(league_id: int, season: int, current_only: bool=False) -> List[str]:
    params = {"league": league_id, "season": season}
    if current_only:
        params["current"] = "true"
    d = apifootball_get("/fixtures/rounds", params)
    rounds = d.get("response", []) or []
    return [rounds] if isinstance(rounds, str) else rounds

def fixtures_for_round(league_id: int, season: int, round_name: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page = 1
    while True:
        d = apifootball_get("/fixtures", {"league": league_id, "season": season, "round": round_name, "page": page})
        out.extend(d.get("response", []) or [])
        paging = d.get("paging", {}) or {}
        if paging.get("current", 1) >= paging.get("total", 1):
            break
        page += 1
        time.sleep(0.3)
    return out

def players_for_fixture(fixture_id: int) -> List[Dict[str, Any]]:
    d = apifootball_get("/fixtures/players", {"fixture": fixture_id})
    return d.get("response", []) or []

# ---- Transform (flatten to DB rows) ----
def to_player_rows(fixture_id: int, payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for team_block in payload:
        team = team_block.get("team", {}) or {}
        team_id = team.get("id")
        for p in team_block.get("players", []) or []:
            player = p.get("player", {}) or {}
            stats_list = p.get("statistics") or []
            stats = stats_list[0] if stats_list else {}

            games = stats.get("games", {}) or {}
            shots = stats.get("shots", {}) or {}
            goals = stats.get("goals", {}) or {}
            cards = stats.get("cards", {}) or {}
            pen   = stats.get("penalty", {}) or {}

            # Saves sometimes live under goalkeeper or under goals
            saves = (stats.get("goalkeeper", {}) or {}).get("saves")
            if saves is None:
                saves = (stats.get("goals", {}) or {}).get("saves")

            row = {
                "fixture_id":        fixture_id,
                "team_id":           team_id,
                "player_id":         player.get("id"),
                "player_name":       player.get("name"),
                "position":          games.get("position"),
                "minutes":           games.get("minutes"),
                "rating":            games.get("rating"),
                "shots_total":       shots.get("total"),
                "shots_on":          shots.get("on"),
                "goals":             goals.get("total"),
                "assists":           goals.get("assists"),
                "conceded":          goals.get("conceded"),
                "saves":             saves,
                "yellow":            cards.get("yellow"),
                "red":               cards.get("red"),
                "penalties_won":     pen.get("won"),
                "penalties_scored":  pen.get("scored"),
                "penalties_missed":  pen.get("missed"),
                "raw_json":          p
            }
            if row["player_id"] is not None:
                rows.append(row)
    return rows

# ---- Upsert (same pattern as events) ----
def upsert_players(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sb = supa()
    sb.table("fixture_player_stats").upsert(rows).execute()
    return len(rows)

# ---- Orchestration (same CLI shape, just 'round' instead of 'date') ----
def ingest_round(league: int, season: int, round_name: str) -> None:
    fixtures = fixtures_for_round(league, season, round_name)
    total = 0
    for f in fixtures:
        fx_id = (f.get("fixture") or {}).get("id")
        if not fx_id:
            continue
        payload = players_for_fixture(fx_id)
        rows = to_player_rows(fx_id, payload)
        n = upsert_players(rows)
        total += n
        logging.info("Fixture %s → %d player rows", fx_id, n)
        time.sleep(0.25)  # gentle buffer between calls
    logging.info("Round '%s' complete → %d player rows", round_name, total)

def main():
    parser = argparse.ArgumentParser(description="Ingest player stats by ROUND (round-driven, FPL-focused)")
    parser.add_argument("--league", type=int, required=True, help="League id (e.g., 39 for EPL)")
    parser.add_argument("--season", type=int, required=True, help="Season year (e.g., 2024)")
    parser.add_argument("--round", type=str, default="current", help="'current' or exact (e.g., 'Regular Season - 5')")
    args = parser.parse_args()

    if args.round.lower() == "current":
        rounds = list_rounds(args.league, args.season, current_only=True)
        if not rounds:
            raise SystemExit("Could not resolve current round.")
        logging.info("Resolved current round(s): %s", rounds)
    elif args.round:
        rounds = [args.round]
    else:
        rounds = list_rounds(args.league, args.season, current_only=False)
        logging.info("Fetched %d rounds for league %s season %s", len(rounds), args.league, args.season)

    for r in rounds:
        logging.info("=== Ingesting round: %s ===", r)
        ingest_round(args.league, args.season, r)

if __name__ == "__main__":
    main()
