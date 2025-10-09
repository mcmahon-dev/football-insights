import os, sys, json, time
from pathlib import Path
from datetime import datetime, timezone
import requests

# ===== Env/config =====
API_KEY  = os.getenv("API_FOOTBALL_KEY")
LEAGUE_ID = int(os.getenv("LEAGUE_ID", 39))
SEASON    = int(os.getenv("SEASON", 2023))
ROUND     = os.getenv("ROUND", "Regular Season - 1")
MAX_ATTEMPTS_PER_FIXTURE = int(os.getenv("MAX_ATTEMPTS", 3))
MIN_INTERVAL_SECONDS = float(os.getenv("MIN_INTERVAL_SECONDS", 6.5))  # 10 req/min -> ~6s; keep 6.5s safe

if not API_KEY:
    print("❌ Missing API_FOOTBALL_KEY")
    sys.exit(1)

BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

# ===== Stable output (resume-friendly) =====
ROOT = Path(__file__).resolve().parent
RUN_DIR = ROOT / "raw-data" / "api-football" / f"{SEASON}_{ROUND.replace(' ', '_').replace('-', '_')}"
FIX_DIR = RUN_DIR / "players_by_fixture"
FIX_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST = RUN_DIR / "manifest.jsonl"   # append-only log

def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def load_done_map():
    """Return {fixture_id: last_status_entry} from manifest.jsonl, if present."""
    done = {}
    if MANIFEST.exists():
        with MANIFEST.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    done[rec["fixture_id"]] = rec
                except Exception:
                    continue
    return done

def append_manifest(**rec):
    with MANIFEST.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def get_json(path, params=None):
    url = f"{BASE}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {})
    # Basic debug (keep light—GA logs are limited)
    print(f"GET {r.url} [{r.status_code}]")
    try:
        j = r.json()
    except Exception:
        return r, None
    return r, j

def sleep_for_rate_limit(last_ts):
    if last_ts is None:
        return time.time()
    elapsed = time.time() - last_ts
    if elapsed < MIN_INTERVAL_SECONDS:
        time.sleep(MIN_INTERVAL_SECONDS - elapsed)
    return time.time()

# ===== 0) Status (optional but helpful) =====
r, status = get_json("/status")
if r.status_code != 200:
    print(f"⚠️ status check failed: {r.status_code} {r.text[:200]}")
else:
    (RUN_DIR / "status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")

# ===== 1) Fixtures for round (write once) =====
fixtures_path = RUN_DIR / "fixtures.json"
if fixtures_path.exists():
    fixtures = json.loads(fixtures_path.read_text(encoding="utf-8")).get("response", [])
else:
    r, fjson = get_json("/fixtures", params={"league": LEAGUE_ID, "season": SEASON, "round": ROUND})
    if r.status_code != 200 or not isinstance(fjson, dict):
        print(f"❌ fixtures fetch failed: {r.status_code} {r.text[:200]}")
        sys.exit(1)
    fixtures_path.write_text(json.dumps(fjson, indent=2), encoding="utf-8")
    fixtures = fjson.get("response", [])

print(f"📈 Fixtures found: {len(fixtures)} for {SEASON} / '{ROUND}'")

if not fixtures:
    # Persist valid rounds for debugging if round string is off
    r, rounds = get_json("/fixtures/rounds", params={"league": LEAGUE_ID, "season": SEASON})
    (RUN_DIR / "valid_rounds.json").write_text(json.dumps(rounds, indent=2), encoding="utf-8")
    print("⚠️ No fixtures for that round. See valid_rounds.json")
    sys.exit(0)

# ===== 2) Resume map =====
done_map = load_done_map()
print(f"🔁 Already have manifest entries for {len(done_map)} fixtures")

# ===== 3) Iterate fixtures with strict rate-limit, write per fixture, log manifest =====
last_request_ts = None
ok_count, skip_count, err_count = 0, 0, 0

for fx in fixtures:
    fixture_id = fx["fixture"]["id"]
    out_path = FIX_DIR / f"players_{fixture_id}.json"

    # Skip if previously successful OR file already exists
    prev = done_map.get(fixture_id)
    if (prev and prev.get("status") == "ok") or out_path.exists():
        skip_count += 1
        continue

    attempts = (prev or {}).get("attempts", 0)

    while attempts < MAX_ATTEMPTS_PER_FIXTURE:
        attempts += 1
        last_request_ts = sleep_for_rate_limit(last_request_ts)

        r, j = get_json("/fixtures/players", params={"fixture": fixture_id})

        if r.status_code == 429:
            # Honor Retry-After, fallback to exponential wait
            retry_after = r.headers.get("Retry-After")
            wait = int(retry_after) if (retry_after and retry_after.isdigit()) else min(60, int(2 ** attempts * 2))
            print(f"⏳ 429 rate limited. Waiting {wait}s…")
            time.sleep(wait)
            continue

        if r.status_code != 200 or not isinstance(j, dict):
            msg = f"http {r.status_code}: {r.text[:200]}"
            append_manifest(
                season=SEASON, round=ROUND, fixture_id=fixture_id,
                status="error", attempts=attempts, message=msg, updated_at=now()
            )
            print(f"❌ Fixture {fixture_id} attempt {attempts} failed: {msg}")
            # Continue loop to retry, or break if max attempts exceeded
        else:
            # Success; write immediately
            out_path.write_text(json.dumps(j, indent=2), encoding="utf-8")
            append_manifest(
                season=SEASON, round=ROUND, fixture_id=fixture_id,
                status="ok", attempts=attempts, message=f"saved:{out_path.name}", updated_at=now()
            )
            ok_count += 1
            print(f"✅ Saved {out_path.name}")
            break

    # Exceeded attempts?
    if attempts >= MAX_ATTEMPTS_PER_FIXTURE and (not out_path.exists()):
        err_count += 1
        print(f"⚠️ Gave up on fixture {fixture_id} after {attempts} attempts")

print(f"🏁 Done. ok={ok_count} | skipped={skip_count} | errors={err_count}")
print(f"🧾 Manifest: {MANIFEST}")
print(f"📂 Output:   {FIX_DIR}")
