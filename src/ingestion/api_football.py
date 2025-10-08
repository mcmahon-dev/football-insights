import os, time, requests

BASE = "https://v3.football.api-sports.io"

def _headers():
    return {"x-apisports-key": os.environ["API_FOOTBALL_KEY"]}

def get(path, params):
    url = f"{BASE}/{path}"
    r = requests.get(url, headers=_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def paged_get(path, params, data_key="response"):
    page = 1
    while True:
        payload = {**params, "page": page}
        data = get(path, payload)
        items = data.get(data_key, [])
        if not items:
            break
        for it in items:
            yield it
        time.sleep(0.25)
        page += 1
