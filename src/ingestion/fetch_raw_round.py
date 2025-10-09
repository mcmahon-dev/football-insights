import os, sys, json, time
from pathlib import Path
from datetime import datetime, timezone
import urllib.parse
import requests

# ===== Env/config =====
API_KEY  = os.getenv("API_FOOTBALL_KEY")
LEAGUE_ID = int(os.getenv("LEAGUE_ID", 39))
SEASON    = int(os.getenv("SEASON", 2023))
ROUND     = os.getenv("ROUND", "Regular Season - 1")

# rate limit / retry
MAX_ATTEMPTS_PER_FIXTURE = int(os.getenv("MAX_ATTEMPTS", 3))
MIN_INTERVAL_SECONDS = float(os.getenv("MIN_INTERVAL_SECONDS", 6.5))  # 10 req/min -> ~6.5s safe

# Supabase Storage (RENAMED KEY)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE")  # <<< renamed
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET", "api-football-raw")

if not API_KEY:
    print("❌ Missing API_FOOTBALL_KEY")
    sys.exit(1)
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
    print("❌ Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE")
    sys.exit(1)

# ===== Constants/paths =====
BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": API_KEY}

ROOT = Path(__file__).resolve().parent
RUN_DIR = ROOT / "raw-data" / "api-football" / f"{SEASON}_{ROUND.replace(' ', '_').replace('-', '_')}"
FIX_DIR = RUN_DIR / "players_by_fixture"
FIX_DIR.mkdir(parents=True, exist_ok=True)
MANIFEST = RUN_DIR / "manifest.jsonl"

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------- Supabase Storage helpers (REST) ----------
def _sb_obj_url(bucket: str, object_key: str) -> str:
    return f"{SUPABASE_URL}/storage/v1/object/{urllib.parse.quote(bucket)}/{urllib.parse.quote(object_key)}"

def sb_upload_json(bucket: str, object_key: str, obj: dict, upsert: bool = True) -> None:
    url = _sb_obj_url(bucket, object_key)
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": "application/json",
        "x-upsert": "true" if upsert else "false",
    }
    data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    resp = requests.post(url, headers=headers, data=data)
    if resp.status_code not in (200, 201):
        if resp.status_code in (400, 409):
            resp = requests.put(url, headers=headers, data=data)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"Storage upload failed ({resp.status_code}): {resp.text[:300]}")

def sb_upload_bytes(bucket: str, object_key: str, content: bytes, content_type="application/json", upsert: bool = True) -> None:
    url = _sb_obj_url(bucket, object_key)
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "Content-Type": content_type,
        "x-upsert": "true" if upsert else "false",
    }
    resp = requests.post(url, headers=headers, data=content)
    if resp.status_code not in (200, 201):
        if resp.status_code in (400, 409):
            resp = requests.put(url, headers=headers, data=content)
        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(f"Storage upload failed ({resp.status_code}): {resp.text[:300]}")

# ---------- Local manifest helpers ----------
def load_done_map():
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
    line = json.dumps(rec, ensure_ascii=False) + "\n"
    MANIFEST.open("a", encoding="utf-8").write(line)
    # also sync manifest to Storage (append-like by re-uploading whole file)
    try:
        sb_upload_bytes(
            SUPABASE_BUCKET,
            f"{SEASON}_{ROUND.replace(' ', '_').replace('-', '_')}/manifest.jsonl",
            MANIFEST.read_bytes(),
            content_type="text/plain; charset=utf-8",
            upsert=True,
        )
    except Exception as e:
        print(f"⚠️ Failed to upload manifest: {e}")

# ---------- HTTP helpers ----------
def get_json(path, params=None):
    url = f"{BASE}{path}"
    r = requests.get(url, headers=HEADERS, params=params or {})
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

# ===== 0) Status (optional) =====
r, status = get_json("/status")
if r.status_code == 200 and isinstance(status, dict):
    (RUN_DIR / "status.json").write_text(json.dumps(status, indent=2), encoding="utf-8")
    try:
        sb_upload_json(SUPABASE_BUCKET, f"{SEASON}_{ROUND.replace(' ', '_').replace('-', '_')}/status.json", status)
    except Exception as e:
        print(f"⚠️ Failed to upload status.json: {e}")
else:
    print(f"⚠️ status check failed: {r.status_code} {getattr(r, 'text', '')[:200]}")

# ===== 1) Fixtures for round =====
fixtures_path = RUN_DIR / "fixtures.json"
if fixtures_path.exists():
    fixtures = json.loads(fixtures_path.read_text(encoding="utf-8")).get("response", [])
else:
    r, fjson = get_json("/fixtures", params={"league": LEAGUE_ID, "season": SEASON, "round": ROUND})
    if r.status_code != 200 or not isinstance(fjson, dict):
        print(f"❌ fixtures fetch failed: {r.status_code} {getattr(r, 'text', '')[:200]}")
        sys.exit(1)
    fixtures_path.write_text(json.dumps(fjson, indent=2), encoding="utf-8")
    try:
        sb_upload_json(SUPABASE_BUCKET, f"{SEASON}_{ROUND.replace(' ', '_').replace('-', '_')}/fixtures.json", fjson)
    except Exception as e:
        print(f"⚠️ Failed to upload fixtures.json: {e}")
    fixtures = fjson.get("response", [])

print(f"📈 Fixtures found: {len(fixtures)} for {SEASON} / '{ROUND}'")
if not fixtures:
    r, rounds = get_json("/fixtures/rounds", params={"league": LEAGUE_ID, "season": SEASON})
    (RUN_DIR / "valid_rounds.json").write_text(json.dumps(rounds, indent=2), encoding="utf-8")
    try:
        sb_upload_json(SUPABASE_BUCKET, f"{SEASON}_{ROUND.replace(' ', '_').replace('-', '_')}/valid_rounds.json", rounds)
    except Exception as e:
        print(f"⚠️ Failed to upload valid_rounds.json: {e}")
    sys.exit(0)

# ===== 2) Resume map =====
done_map = load_done_map()
print(f"🔁 Already have manifest entries for {len(done_map)} fixtures")

# ===== 3) Iterate fixtures with strict rate-limit =====
last_request_ts = None
ok_count, skip_count, err_count = 0, 0, 0

for fx in fixtures:
    fixture_id = fx["fixture"]["id"]
    out_path = FIX_DIR / f"players_{fixture_id}.json"
    storage_key = f"{SEASON}_{ROUND.replace(' ', '_').replace('-', '_')}/players_by_fixture/players_{fixture_id}.json"

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
            retry_after = r.headers.get("Retry-After")
            wait = int(retry_after) if (retry_after and retry_after.isdigit()) else min(60, int(2 ** attempts * 2))
            print(f"⏳ 429 rate limited. Waiting {wait}s…")
            time.sleep(wait)
            continue

        if r.status_code != 200 or not isinstance(j, dict):
            msg = f"http {r.status_code}: {getattr(r, 'text', '')[:200]}"
            append_manifest(
                season=SEASON, round=ROUND, fixture_id=fixture_id,
                status="error", attempts=attempts, message=msg, updated_at=now_iso()
            )
            print(f"❌ Fixture {fixture_id} attempt {attempts} failed: {msg}")
        else:
            # Write locally (optional) and upload immediately to Supabase
            out_path.write_text(json.dumps(j, indent=2), encoding="utf-8")
            try:
                sb_upload_json(SUPABASE_BUCKET, storage_key, j)
            except Exception as e:
                print(f"⚠️ Failed to upload {storage_key}: {e}")
            append_manifest(
                season=SEASON, round=ROUND, fixture_id=fixture_id,
                status="ok", attempts=attempts, message=f"saved:{out_path.name}", updated_at=now_iso()
            )
            ok_count += 1
            print(f"✅ Saved {out_path.name} and uploaded to storage: {storage_key}")
            break

    if attempts >= MAX_ATTEMPTS_PER_FIXTURE and (not out_path.exists()):
        err_count += 1
        print(f"⚠️ Gave up on fixture {fixture_id} after {attempts} attempts")

print(f"🏁 Done. ok={ok_count} | skipped={skip_count} | errors={err_count}")
print(f"🧾 Manifest: {MANIFEST}")
print(f"📦 Storage prefix: {SEASON}_{ROUND.replace(' ', '_').replace('-', '_')}/")
