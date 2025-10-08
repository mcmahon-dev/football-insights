import os, json
from .api_football import get

def dump_meta(name, data):
    print(f"\n## META: {name}")
    meta = {k: data.get(k) for k in ("errors", "results", "paging")}
    print(json.dumps(meta, indent=2))

def main():
    league = int(os.getenv("LEAGUE_ID", "39"))
    season = int(os.getenv("SEASON", "2024"))
    round_name = os.getenv("ROUND_NAME") or "Regular Season - 1"

    print(f"LEAGUE_ID={league} SEASON={season} ROUND_NAME='{round_name}'")

    # 1) Confirm the round exists for this season (optional)
    rounds = get("fixtures/rounds", {"league": league, "season": season})
    dump_meta("fixtures/rounds", rounds)
    print("First 3 rounds:", (rounds.get("response") or [])[:3])

    # 2) Fetch fixtures for exactly that round — single call (no paging)
    fx = get("fixtures", {"league": league, "season": season, "round": round_name})
    dump_meta("fixtures", fx)

    resp = fx.get("response") or []
    print(f"fixtures.response length = {len(resp)}")

    if resp:
        sample = resp[0]
        # print a minimal, schema-agnostic peek to avoid KeyErrors
        skinny = {
            "fixture_id": sample.get("fixture", {}).get("id"),
            "date": sample.get("fixture", {}).get("date"),
            "status": sample.get("fixture", {}).get("status", {}).get("short"),
            "home": sample.get("teams", {}).get("home", {}).get("name"),
            "away": sample.get("teams", {}).get("away", {}).get("name"),
            "goals": sample.get("goals"),
        }
        print("\nFirst fixture (skinny):")
        print(json.dumps(skinny, indent=2))
    else:
        print("\nNo fixtures returned for that round. Tips:")
        print("- Make sure ROUND_NAME matches exactly one value from fixtures/rounds.")
        print("- Double-check season (EPL 2023 = 2023/24).")
        print("- Try another round (e.g., 'Regular Season - 20').")
        print("- Some plans restrict older seasons.")
