import os, json
from .api_football import get, paged_get

def peek(label, obj, limit=2):
    try:
        sample = obj[:limit]
    except Exception:
        sample = obj
    print(f"\n=== {label} (showing up to {limit}) ===")
    print(json.dumps(sample, indent=2, default=str))

def main():
    league = int(os.getenv("LEAGUE_ID", "39"))    # EPL default
    season = int(os.getenv("SEASON", "2024"))
    round_limit = int(os.getenv("ROUND_LIMIT", "2"))
    specific_round = os.getenv("ROUND_NAME")       # optional override
    print(f"LEAGUE_ID={league} SEASON={season} ROUND_LIMIT={round_limit} ROUND_NAME={specific_round}")

    # 1) Get rounds
    r = get("fixtures/rounds", {"league": league, "season": season})
    print("\n# ROUNDS: meta")
    print(json.dumps({k: r.get(k) for k in ("errors", "results", "paging")}, indent=2))
    rounds = r.get("response", []) or []
    print(f"Total rounds returned: {len(rounds)}")
    peek("Rounds", rounds)

    # 2) Choose rounds to fetch
    targets = [specific_round] if specific_round else rounds[:round_limit]
    print(f"Target rounds: {targets}")

    # 3) Fetch fixtures for each round
    total_fx = 0
    for rnd in targets:
        fx_list = list(paged_get("fixtures", {"league": league, "season": season, "round": rnd}))
        print(f"\n# FIXTURES for round='{rnd}': count={len(fx_list)}")
        peek("Fixtures sample", [
            {
                "fixture_id": fx["fixture"]["id"],
                "date": fx["fixture"]["date"],
                "status": fx["fixture"]["status"]["short"],
                "home": fx["teams"]["home"]["name"],
                "away": fx["teams"]["away"]["name"],
                "goals": fx.get("goals"),
                "score_ht": fx.get("score", {}).get("halftime"),
            } for fx in fx_list
        ])
        total_fx += len(fx_list)

    print(f"\nTOTAL fixtures across chosen rounds: {total_fx}")
    if total_fx == 0:
        print("\nTIPs if zero:\n"
              "- Check LEAGUE_ID/SEASON are correct for your plan.\n"
              "- Try setting ROUND_NAME exactly (e.g. 'Regular Season - 1').\n"
              "- Free plans may limit historical data.\n")

if __name__ == "__main__":
    main()
