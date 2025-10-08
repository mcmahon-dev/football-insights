import os
from supabase import create_client, Client

def supa() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_ROLE"]
    return create_client(url, key)

def upsert(table, rows, pk="event_id"):
    if not rows:
        return
    client = supa()
    client.table(table).upsert(rows, on_conflict=pk).execute()
