#!/usr/bin/env python3
"""
Pandas-first player ingestion for API-Football.

Endpoints:
  - /fixtures?league={id}&season={yyyy}&round={Round Name}
  - /fixtures/players?fixture={fixture_id}

Usage examples:
  python pandas_players_by_round.py --league 39 --season 2023 --round "Regular Season - 1"
  python pandas_players_by_round.py --league 39 --season 2023 --round all
"""

import os
import sys
import time
import argparse
from typing import Any, Dict, List

import requests
import pandas as pd

API_BASE = "https://v3.football.api-sports.io"

def headers() -> Dict[str, str]:
    api_key = os.getenv("API_FOOTBALL_KEY")
    rapidapi_host = os.getenv("RAPIDAPI_HOST", "").strip()
    if not api_key:
        print("ERROR: missing API_FOOTBALL_KEY in environment.", file=sys.stderr)
        sys.exit(1)
    return ({"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": rapidapi_host}
            if rapidapi_host else {"x-apisports-key": api_key})

def get_json(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(f"{API_BASE}/{path.lstrip('/')}", headers=headers(), params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def list_rounds(league_id: int, season: int) -> List[str]:
    d = get_json("/fixtures/rounds", {"league": league_id, "season": season})
    rounds = d.get("response", []) or []
    return [rounds] if isinstance(rounds, str) else rounds

def fixtures_for_round(league_id: int, season: int, round_name: str) -> List[Dict[str, Any]]:
    fixtures: List[Dict[str, Any]] = []
    page = 1
    while True:
        d = get_json("/fixtures", {"league": league_id, "season": season, "round": round_name, "page": page})
        fixtures.extend(d.get("response", []) or [])
        paging = d.get("paging", {}) or {}
        if paging.get("current", 1) >= paging.get("total", 1):
            break
        page += 1
        time.sleep(0.2)
    return fixtures

def players_for_fixture(fixture_id: int) -> List[Dict[str, Any]]:
    d = get_json("/fixtures/players", {"fixture": fixture_id})
    return d.get("response", []) or []

def flatten_players(fixture_id: int, payload: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for team in payload:
        team_id = team.get("team", {}).get("id")
        team_name = team.get("team", {}).get("name")
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
                "team_id": team_id,
                "team_name": team_name,
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
    all_rows: List[Dict[str, Any]] = []
    for f in fixtures:
        fx_id = (f.get("fixture") or {}).get("id")
        if not fx_id:
            continue
        payload = players_for_fixture(fx_id)
        all_rows.extend(flatten_players(fx_id, payload))
        time.sleep(0.15)
    return pd.DataFrame(all_rows)

def main():
    p = argparse.ArgumentParser(description="Build a pandas DataFrame of player stats for a league/season/round.")
    p.add_argument("--league", type=int, required=True, help="League ID (e.g., 39 = EPL)")
    p.add_argument("--season", type=int, required=True, help="Season year (e.g., 2023)")
    p.add_argument("--round", type=str, required=False, default="all",
                  help="'all' or exact string like 'Regular Season - 1'")
    p.add_argument("--out", type=str, default="", help="Optional CSV output path")
    args = p.parse_args()

    # Determine rounds to run
    if args.round.lower() == "all":
        rounds = list_rounds(args.league, args.season)
        if not rounds:
            print("No rounds returned by API. Check league/season.", file=sys.stderr)
            sys.exit(1)
    else:
        rounds = [args.round]

    # Build one DF per round, then concat
    dfs = []
    for r in rounds:
        df = df_for_round(args.league, args.season, r)
        # tack round name into the DataFrame for context
        if not df.empty:
            df.insert(0, "round", r)
        dfs.append(df)

    final = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    print("DataFrame shape:", final.shape)
    print(final.head(20))

    out_path = args.out or f"players_{args.league}_{args.season}_{args.round.replace(' ', '_').replace('-', '')}.csv"
    final.to_csv(out_path, index=False)
    print(f"\nSaved CSV -> {out_path}")

if __name__ == "__main__":
    main()
