# dhan_live_banknifty_rest.py

import json
import time
import requests

from config import DHAN_ACCESS_TOKEN, DHAN_BASE_URL, DHAN_CLIENT_ID

# BANKNIFTY index from your api-scrip-master.csv
BANKNIFTY_SECURITY_ID = 25
EXCHANGE_SEGMENT = "IDX_I"      # this worked for historical API

URL = f"{DHAN_BASE_URL}/v2/marketfeed/quote"

HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "access-token": DHAN_ACCESS_TOKEN,
    "client-id": DHAN_CLIENT_ID,
}


def poll_banknifty():
    print("Polling BANKNIFTY via /v2/marketfeed/quote")
    print(f"Segment: {EXCHANGE_SEGMENT}, securityId: {BANKNIFTY_SECURITY_ID}")
    print(f"Client-id header: {DHAN_CLIENT_ID}")
    print("Ctrl+C to stop\n")

    payload = {EXCHANGE_SEGMENT: [BANKNIFTY_SECURITY_ID]}

    while True:
        print(f"Requesting: {URL}")
        print("Headers:", HEADERS)
        print("Payload:")
        print(json.dumps(payload, indent=2))

        resp = requests.post(URL, headers=HEADERS, data=json.dumps(payload))
        print("Status code:", resp.status_code)

        try:
            data = resp.json()
        except Exception:
            print("Non-JSON response:", resp.text)
            time.sleep(2)
            continue

        print("Body:", data)

        # ---- SAFE LTP PARSE FOR CURRENT STRUCTURE ----
        ltp = None
        data_root = data.get("data", {})
        seg_block = data_root.get(EXCHANGE_SEGMENT, {})

        # seg_block is like {"25": {...}}
        if isinstance(seg_block, dict):
            sec_block = seg_block.get(str(BANKNIFTY_SECURITY_ID))
            if sec_block:
                # Dhan names this 'last_price'
                ltp = sec_block.get("last_price")
                ohlc = sec_block.get("ohlc", {})
            else:
                sec_block = None
        else:
            sec_block = None

        if ltp is not None:
            o = ohlc.get("open")
            h = ohlc.get("high")
            l = ohlc.get("low")
            c = ohlc.get("close")
            print(
                f"➡ BANKNIFTY LTP: {ltp:.2f} | "
                f"OHLC O:{o} H:{h} L:{l} C:{c}"
            )
        else:
            print("Could not find LTP in response, full JSON printed above")

        print()
        time.sleep(2)


if __name__ == "__main__":
    try:
        poll_banknifty()
    except KeyboardInterrupt:
        print("\nStopped by user.")
