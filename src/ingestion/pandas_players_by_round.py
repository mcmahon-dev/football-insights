#!/usr/bin/env python3
import os, sys, time, argparse, re
from typing import Any, Dict, List
import requests
import pandas as pd

API_BASE = "https://v3.football.api-sports.io"

def headers() -> Dict[str, str]:
    api_key = os.getenv("API_FOOTBALL_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST", "").strip()
    if not api_key:
        print("ERROR: missing API_FOOTBALL_KEY", file=sys.stderr)
        sys.exit(1)
    return ({"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": rapidapi_host}
            if rapidapi_host else {"x-apisports-key": api_key})

def get_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(f"{API_BASE}/{path.lstrip('/')}", headers=headers(), params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def list_rounds(league: int, season: int) -> List[str]:
    d = get_json("/fixtures/rounds", {"league": league, "season": season})
    rounds = d.get("response", []) or []
    return [rounds] if isinstance(rounds, str) else rounds

def fixtures_for_round(league: int, season: int, round_name: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    page = 1
    while True:
        d = get_json("/fixtures", {"league": league, "season": season, "round": round_name, "page": page})
        out.extend(d.get("response", []) or [])
        paging = d.get("paging", {}) or {}
        if paging.get("current", 1) >= paging.get("total", 1):
            break
        page += 1
        time.sleep(0.15)
    return out

def players_for_fixture(fixture_id: int) -> List[Dict[str, Any]]:
    d = get_json("/fixtures/players", {"fixture": fixture_id})
    return d.get("response", []) or []

def round_number(round_name: str) -> str:
    """Extract '1' from 'Regular Season - 1' etc. Fallback to a safe slug."""
    m = re.search(r"(\d+)$", round_name or "")
    return m.group(1) if m else re.sub(r"[^A-Za-z0-9]+", "", (round_name or "round"))

def flatten_players(fixture_id: int, payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for team in payload:
        t = team.get("team", {}) or {}
        for p in team.get("players", []) or []:
            player = p.get("player", {}) or {}
            stats = (p.get("statistics") or [{}])[0]
            games = stats.get("games", {}) or {}
            shots = stats.get("shots", {}) or {}
            goals = stats.get("goals", {}) or {}
            cards = stats.get("cards", {}) or {}
            pen   = stats.get("penalty", {}) or {}
            rows.append({
                "fixture_id": fixture_id,
                "team_id": t.get("id"),
                "team_name": t.get("name"),
                "player_id": player.get("id"),
                "player_name": player.get("name"),
                "position": games.get("position"),
                "minutes": games.get("minutes"),
                "rating": games.get("rating"),
                "shots_total": shots.get("total"),
                "shots_on": shots.get("on"),
                "goals": goals.get("total"),
                "assists": goals.get("assists"),
                "conceded": goals.get("conceded"),
                "yellow": cards.get("yellow"),
                "red": cards.get("red"),
                "penalties_won": pen.get("won"),
                "penalties_scored": pen.get("scored"),
                "penalties_missed": pen.get("missed"),
            })
    return rows

def df_for_round(league: int, season: int, round_name: str) -> pd.DataFrame:
    fixtures = fixtures_for_round(league, season, round_name)
    rows: List[Dict[str, Any]] = []
    for f in fixtures:
        fx_id = (f.get("fixture") or {}).get("id")
        if not fx_id:
            continue
        rows.extend(flatten_players(fx_id, players_for_fixture(fx_id)))
        time.sleep(0.1)
    df = pd.DataFrame(rows)
    if not df.empty:
        df.insert(0, "round", round_name)
        df.insert(0, "season", season)
    return df

def main():
    ap = argparse.ArgumentParser("Pandas player stats → CSV files per round")
    ap.add_argument("--league", type=int, required=True)
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--round", type=str, default="all", help="'all' or exact (e.g., 'Regular Season - 1')")
    ap.add_argument("--outdir", type=str, default="raw-data", help="relative folder under current dir")
    args = ap.parse_args()

    # Ensure output dir exists
    os.makedirs(args.outdir, exist_ok=True)

    if args.round.lower() == "all":
        rounds = list_rounds(args.league, args.season)
    else:
        rounds = [args.round]

    if not rounds:
        print("No rounds returned by API. Check league/season.", file=sys.stderr)
        sys.exit(1)

    for r in rounds:
        df = df_for_round(args.league, args.season, r)
        rn = round_number(r)
        out_path = os.path.join(args.outdir, f"playerScores{args.season}round{rn}.csv")
        df.to_csv(out_path, index=False)
        print(f"Wrote {out_path} (rows={len(df)})")

if __name__ == "__main__":
    main()
