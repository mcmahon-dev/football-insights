# etl_round.py
import os
import json
import math
import urllib.parse
from datetime import datetime, timezone
from typing import List, Dict

import requests
import pandas as pd
import numpy as np
from pathlib import Path

# ========= Env =========
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "api-football-raw")

SEASON = os.getenv("SEASON", "2023")
ROUND = os.getenv("ROUND", "Regular Season - 1")

# Build the storage prefix used by the ingestion step
PREFIX = f"{SEASON}_{ROUND.replace(' ', '_').replace('-', '_')}"
PLAYERS_PREFIX = f"{PREFIX}/players_by_fixture"

# ========= Small helpers =========
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _sb_list(prefix_path: str) -> List[Dict]:
    """
    List objects under a prefix in Supabase Storage (REST).
    Returns a list of dicts with at least 'name'.
    """
    url = f"{SUPABASE_URL}/storage/v1/object/list/{urllib.parse.quote(SUPABASE_BUCKET)}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
    }
    payload = {"prefix": prefix_path, "limit": 1000}
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data or []

def _sb_get(object_key: str) -> bytes:
    """
    Download a single object from Supabase Storage (REST).
    """
    url = f"{SUPABASE_URL}/storage/v1/object/{urllib.parse.quote(SUPABASE_BUCKET)}/{urllib.parse.quote(object_key)}"
    headers = {"Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}"}
    r = requests.get(url, headers=headers, timeout=120)
    r.raise_for_status()
    return r.content

def json_safe(obj):
    """Recursively convert pandas/NumPy/float edge cases to JSON-safe Python types."""
    if obj is pd.NA:
        return None
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        f = float(obj)
        return f if math.isfinite(f) else None
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, pd.Timestamp):
        return obj.to_pydatetime().isoformat()
    if isinstance(obj, list):
        return [json_safe(x) for x in obj]
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    return obj

def upsert_rows(table: str, rows: List[Dict], conflict: str = "fixture_id,player_id", chunk_size: int = 500):
    """
    Minimal, dependency-light upsert into Supabase Postgres using PostgREST.
    """
    if not rows:
        print("Nothing to upsert.")
        return
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }
    params = {"on_conflict": conflict}

    print(f"🔼 Upserting {len(rows)} rows into '{table}' (chunk_size={chunk_size})")
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i:i+chunk_size]
        chunk = [json_safe(rec) for rec in chunk]  # sanitize NaN/Inf/<NA> etc.
        r = requests.post(url, headers=headers, params=params, json=chunk, timeout=120)
        if r.status_code not in (200, 201, 204):
            raise RuntimeError(f"Upsert failed [{r.status_code}]: {r.text[:400]}")
    print("✅ Upsert complete.")

# ========= Transform =========
def parse_players_json(blob: bytes) -> List[Dict]:
    """
    Parse one players_<fixture_id>.json file to rows.
    """
    j = json.loads(blob.decode("utf-8"))
    response = j.get("response", []) or []
    out = []
    for team_block in response:
        team = team_block.get("team", {}) or {}
        team_id = team.get("id")
        team_name = team.get("name")
        players = team_block.get("players", []) or []
        for p in players:
            player = p.get("player", {}) or {}
            player_id = player.get("id")
            player_name = player.get("name")
            stats_list = p.get("statistics", []) or []
            stats = stats_list[0] if stats_list else {}
            games = (stats.get("games") or {})
            goals = (stats.get("goals") or {})

            rating_val = games.get("rating")
            try:
                rating_val = float(rating_val) if rating_val not in (None, "", "NaN") else None
                if rating_val is not None and not math.isfinite(rating_val):
                    rating_val = None
            except Exception:
                rating_val = None

            row = {
                "season": int(SEASON),
                "round_text": ROUND,
                "team_id": team_id,
                "team_name": team_name,
                "player_id": player_id,
                "player_name": player_name,
                "position": games.get("position"),
                "minutes": games.get("minutes"),
                "rating": rating_val,
                "goals": goals.get("total"),
                "assists": goals.get("assists"),
                "fetched_datetime": now_iso(),
                # fixture_id will be attached by caller
            }
            out.append(row)
    return out

def main():
    print(f"🔎 Reading Storage prefix: {PLAYERS_PREFIX}")

    # 1) fixtures.json (optional)
    fixtures_json = None
    try:
        fixtures_json = json.loads(_sb_get(f"{PREFIX}/fixtures.json").decode("utf-8"))
        if isinstance(fixtures_json, dict) and "results" in fixtures_json:
            print(f"📈 fixtures.json indicates {fixtures_json['results']} fixture results")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            print("fixtures.json not found (continuing).")
        else:
            raise

    # 2) List per-fixture files
    listing = _sb_list(PLAYERS_PREFIX)
    fixture_files = [item["name"] for item in listing if item.get("name", "").endswith(".json")]
    print(f"🧾 Found {len(fixture_files)} players_*.json")

    rows_all: List[Dict] = []
    for fname in fixture_files:
        # Extract fixture_id from "players_<fixture_id>.json"
        try:
            fixture_id = int(fname.split("_", 1)[1].split(".json")[0])
        except Exception:
            print(f"⚠️ Skipping unexpected file name: {fname}")
            continue

        blob = _sb_get(f"{PLAYERS_PREFIX}/{fname}")
        rows = parse_players_json(blob)
        for r in rows:
            r["fixture_id"] = fixture_id
        rows_all.extend(rows)

    print(f"🧮 Total rows parsed: {len(rows_all)}")

    if not rows_all:
        print("⚠️ No rows parsed; nothing to upsert.")
        return

    # 3) Pandas normalization (light)
    df = pd.DataFrame(rows_all)

    # Normalize numerics (keep nullable Int64, sanitize later in json_safe)
    for col in ("minutes", "goals", "assists", "team_id", "player_id", "fixture_id"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "rating" in df.columns:
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")  # float with NaN allowed

    # Optional: enforce PK by dropping rows with null player_id
    if "player_id" in df.columns:
        before = len(df)
        df = df[df["player_id"].notna()]
        dropped = before - len(df)
        if dropped:
            print(f"⚠️ Dropped {dropped} rows with null player_id to satisfy PK")

    print("🧪 Sample:")
    print(df.head(5))

    # 4) Upsert
    upsert_rows(
        table="player_round_data",
        rows=df.to_dict(orient="records"),
        conflict="fixture_id,player_id",
        chunk_size=500,
    )

if __name__ == "__main__":
    Path("raw-data").mkdir(exist_ok=True)  # optional local cache dir
    main()
