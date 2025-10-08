import os
from .api_football import paged_get
from ..storage.supabase_client import upsert

# Minimal normalization helper
def pick(d, path, default=None):
    cur = d
    for p in path.split("."):
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur

def main():
    league = int(os.getenv("LEAGUE_ID", "39"))     # EPL by default
    season = int(os.getenv("SEASON", "2023"))

    # 1) Get recent fixtures for the season (limit keeps first run small)
    fixtures = []
    for f in paged_get("fixtures", {"league": league, "season": season}):
        fixtures.append(f)
        if len(fixtures) >= 10:     # keep the first manual run light
            break

    # 2) For each fixture, fetch events and normalize
    rows = []
    for fx in fixtures:
        fid = pick(fx, "fixture.id")
        for e in paged_get("fixtures/events", {"fixture": fid}):
            rows.append({
                "event_id": pick(e, "id"),
                "fixture_id": pick(e, "fixture.id"),
                "team_id": pick(e, "team.id"),
                "player_id": pick(e, "player.id"),
                "player_name": pick(e, "player.name"),
                "type": pick(e, "type"),
                "detail": pick(e, "detail"),
                "minute": pick(e, "time.elapsed", 0),
            })

    # 3) Write to Supabase
    upsert("fpl_events", rows, pk="event_id")
    print(f"Upserted {len(rows)} event rows into Supabase.")

if __name__ == "__main__":
    main()
