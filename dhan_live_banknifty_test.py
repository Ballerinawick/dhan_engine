# dhan_live_banknifty_test.py

import os
import json
import base64

# ---- asyncio monkey patch for Python 3.13 ----
import asyncio as _asyncio

# simple global holder for one event loop
_LOOP_HOLDER = {}

def _compat_get_event_loop():
    """
    Backwards-compatible get_event_loop for libraries
    that still call asyncio.get_event_loop() on Python 3.13.
    We keep a single loop in _LOOP_HOLDER.
    """
    loop = _LOOP_HOLDER.get("loop")
    if loop is None or loop.is_closed():
        loop = _asyncio.new_event_loop()
        _LOOP_HOLDER["loop"] = loop
    return loop

# If asyncio does NOT have get_event_loop, add our own
if not hasattr(_asyncio, "get_event_loop"):
    _asyncio.get_event_loop = _compat_get_event_loop


from dotenv import load_dotenv
from dhanhq import dhanhq, marketfeed


# --------- helpers ---------
def extract_client_id_from_token(token: str) -> str:
    """
    Decode Dhan JWT and fetch dhanClientId from payload
    without extra libs.
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid JWT format")

    payload_b64 = parts[1]
    padding = "=" * (-len(payload_b64) % 4)
    payload_b64 += padding

    payload_json = base64.urlsafe_b64decode(payload_b64.encode("utf-8")).decode("utf-8")
    payload = json.loads(payload_json)

    client_id = payload.get("dhanClientId") or payload.get("dhanClientID")
    if not client_id:
        raise RuntimeError("dhanClientId not found in token payload")

    return str(client_id)


# --------- load creds ---------
load_dotenv()

ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise RuntimeError("DHAN_ACCESS_TOKEN missing in .env")

CLIENT_ID = os.getenv("DHAN_CLIENT_ID")
if not CLIENT_ID:
    CLIENT_ID = extract_client_id_from_token(ACCESS_TOKEN)

print(f"Detected CLIENT_ID: {CLIENT_ID}")

# --------- create dhan client (sanity check) ---------
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

segment_constants = [name for name in dir(dhan) if name.isupper()]
print("Available exchange segments:", segment_constants)

# BANKNIFTY is an index -> use INDEX -> 'IDX_I'
EXCHANGE_SEGMENT = dhan.INDEX   # 'IDX_I'

# From your api-scrip-master.csv (row with BANKNIFTY index)
BANKNIFTY_SECURITY_ID = "25"

# For marketfeed, instruments = list of (exchange_segment, security_id)
instruments = [(EXCHANGE_SEGMENT, BANKNIFTY_SECURITY_ID)]
subscription_code = marketfeed.Ticker   # 15


def main():
    print("Client ID        :", CLIENT_ID)
    print("Exchange segment :", EXCHANGE_SEGMENT)
    print("BANKNIFTY secId  :", BANKNIFTY_SECURITY_ID)
    print("Subscription code:", subscription_code)

    feed = marketfeed.DhanFeed(
        CLIENT_ID,
        ACCESS_TOKEN,
        instruments,
        subscription_code,
    )

    # This blocks and runs websocket loop
    feed.run_forever()


if __name__ == "__main__":
    main()
