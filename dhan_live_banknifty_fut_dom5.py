# dhan_live_banknifty_fut_dom5.py

import os
import time
import json
import requests
from dotenv import load_dotenv

load_dotenv()

ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CLIENT_ID    = os.getenv("DHAN_CLIENT_ID")
BASE_URL     = os.getenv("DHAN_BASE_URL", "https://api.dhan.co").rstrip("/")

if not ACCESS_TOKEN or not CLIENT_ID:
    raise RuntimeError("Missing DHAN_ACCESS_TOKEN or DHAN_CLIENT_ID in .env")

# ---------- CONFIG ----------
EXCHANGE_SEGMENT = "NSE_FNO"        # F&O segment
BANKNIFTY_FUT_SEC_ID = "49508"      # BANKNIFTY-Dec2025-FUT
POLL_INTERVAL_SEC = 1.0
# -----------------------------

def poll_banknifty_fut():
    url = f"{BASE_URL}/v2/marketfeed/quote"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "access-token": ACCESS_TOKEN,
        "client-id": CLIENT_ID,
    }

    payload = {
        EXCHANGE_SEGMENT: [int(BANKNIFTY_FUT_SEC_ID)]
    }

    print(f"Polling BANKNIFTY FUT via /v2/marketfeed/quote")
    print(f"Segment: {EXCHANGE_SEGMENT}, securityId: {BANKNIFTY_FUT_SEC_ID}")
    print(f"Client-id header: {CLIENT_ID}")
    print("Ctrl+C to stop\n")

    while True:
        try:
            print(f"Requesting: {url}")
            print("Payload:")
            print(json.dumps(payload, indent=2))

            resp = requests.post(url, headers=headers, json=payload, timeout=5)
            print("Status code:", resp.status_code)

            try:
                data = resp.json()
            except Exception:
                print("Could not decode JSON, raw text:\n", resp.text)
                time.sleep(POLL_INTERVAL_SEC)
                continue

            print("Body:", data)

            if resp.status_code != 200 or data.get("status") != "success":
                print("⚠️ API did not return success, skipping\n")
                time.sleep(POLL_INTERVAL_SEC)
                continue

            # data -> { EXCHANGE_SEGMENT : { securityId : { ... } } }
            inner_seg = data.get("data", {}).get(EXCHANGE_SEGMENT, {})
            fut_info = inner_seg.get(str(int(BANKNIFTY_FUT_SEC_ID)))

            if not fut_info:
                print("⚠️ No data for this securityId, check segment/securityId\n")
                time.sleep(POLL_INTERVAL_SEC)
                continue

            ltp = fut_info.get("last_price", 0.0)
            ohlc = fut_info.get("ohlc", {}) or {}
            depth = fut_info.get("depth", {}) or {}

            buy_levels = depth.get("buy", []) or []
            sell_levels = depth.get("sell", []) or []

            best_bid = buy_levels[0]["price"] if buy_levels else 0.0
            best_ask = sell_levels[0]["price"] if sell_levels else 0.0
            spread = (best_ask - best_bid) if best_bid and best_ask else 0.0

            buy_qty = sum(l.get("quantity", 0) for l in buy_levels[:5])
            sell_qty = sum(l.get("quantity", 0) for l in sell_levels[:5])
            if buy_qty + sell_qty > 0:
                dom5_imb = (buy_qty - sell_qty) / (buy_qty + sell_qty)
            else:
                dom5_imb = 0.0

            print(
                f"→ FUT LTP: {ltp:.2f} | "
                f"Bid:{best_bid:.2f} Ask:{best_ask:.2f} Spread:{spread:.2f} | "
                f"DOM5 Imb:{dom5_imb:.3f} | "
                f"OHLC O:{ohlc.get('open',0)} H:{ohlc.get('high',0)} "
                f"L:{ohlc.get('low',0)} C:{ohlc.get('close',0)}\n"
            )

            time.sleep(POLL_INTERVAL_SEC)

        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as e:
            print("⚠️ Error during poll:", e)
            time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    poll_banknifty_fut()
