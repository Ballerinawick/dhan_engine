# dhan_fut_dom5_logger.py
import time
import json
import csv
from pathlib import Path
from datetime import datetime

import requests

from config import DHAN_ACCESS_TOKEN, DHAN_BASE_URL, DHAN_CLIENT_ID

# ====== CONFIG ======
EXCHANGE_SEGMENT = "NSE_FNO"       # Futures & options segment
FUT_SECURITY_ID  = "49508"         # BANKNIFTY-Dec2025-FUT from api-scrip-master.csv
CSV_PATH         = Path("banknifty_fut_dom5.csv")
POLL_INTERVAL    = 1.0             # seconds between requests

# ====== CSV SETUP ======
def init_csv(path: Path):
    file_exists = path.exists()
    f = path.open("a", newline="")
    writer = csv.writer(f)

    if not file_exists or path.stat().st_size == 0:
        writer.writerow(
            [
                "ts_iso",           # timestamp (ISO string)
                "ts_epoch",         # timestamp (epoch seconds)
                "ltp",
                "bid1_price",
                "ask1_price",
                "spread",
                "buy_qty_1_5",
                "sell_qty_1_5",
                "dom5_imbalance",
                "ohlc_open",
                "ohlc_high",
                "ohlc_low",
                "ohlc_close",
            ]
        )
    return f, writer

csv_file, csv_writer = init_csv(CSV_PATH)

# ====== HTTP HELPERS ======
def make_headers():
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "access-token": DHAN_ACCESS_TOKEN,
        "client-id": DHAN_CLIENT_ID,
    }


def make_payload():
    # Dhan expects: {"NSE_FNO": [49508]}
    return {EXCHANGE_SEGMENT: [int(FUT_SECURITY_ID)]}


# ====== MAIN LOOP ======
def poll_fut_dom5():
    print(f"Logging BANKNIFTY FUT (secId={FUT_SECURITY_ID}) DOM5 to {CSV_PATH}")
    print(f"Endpoint: {DHAN_BASE_URL}/v2/marketfeed/quote")
    print("Press Ctrl+C to stop\n")

    url = f"{DHAN_BASE_URL}/v2/marketfeed/quote"

    try:
        while True:
            payload = make_payload()
            headers = make_headers()

            try:
                resp = requests.post(url, headers=headers, json=payload, timeout=2)
            except Exception as e:
                print("⚠️  Request error:", e)
                time.sleep(POLL_INTERVAL)
                continue

            print("\nRequesting:", url)
            print("Payload:")
            print(json.dumps(payload, indent=2))
            print("Status code:", resp.status_code)

            try:
                body = resp.json()
            except json.JSONDecodeError:
                print("⚠️  Response is not JSON, raw text:")
                print(resp.text[:500])
                time.sleep(POLL_INTERVAL)
                continue

            print("Body:", body)

            if resp.status_code != 200 or body.get("status") != "success":
                print("⚠️  Non-success status, skipping this tick.")
                time.sleep(POLL_INTERVAL)
                continue

            # Navigate to the fut record: data -> NSE_FNO -> "49508"
            try:
                inner = body["data"][EXCHANGE_SEGMENT][FUT_SECURITY_ID]
            except KeyError as e:
                print("⚠️  Key missing in response:", e)
                time.sleep(POLL_INTERVAL)
                continue

            # ---- core fields ----
            ltp = float(inner.get("last_price", 0.0))
            ohlc = inner.get("ohlc", {}) or {}
            o = float(ohlc.get("open", 0.0))
            h = float(ohlc.get("high", 0.0))
            l = float(ohlc.get("low", 0.0))
            c = float(ohlc.get("close", 0.0))

            depth = inner.get("depth", {}) or {}
            buy_levels = depth.get("buy", []) or []
            sell_levels = depth.get("sell", []) or []

            # best bid/ask
            best_bid_price = float(buy_levels[0]["price"]) if buy_levels and buy_levels[0]["price"] else 0.0
            best_ask_price = float(sell_levels[0]["price"]) if sell_levels and sell_levels[0]["price"] else 0.0

            spread = best_ask_price - best_bid_price if best_bid_price and best_ask_price else 0.0

            # DOM5 quantities
            buy_qty_1_5 = sum(float(level.get("quantity", 0.0)) for level in buy_levels[:5])
            sell_qty_1_5 = sum(float(level.get("quantity", 0.0)) for level in sell_levels[:5])

            denom = buy_qty_1_5 + sell_qty_1_5
            dom_imb = (buy_qty_1_5 - sell_qty_1_5) / denom if denom > 0 else 0.0

            now = datetime.utcnow()
            ts_iso = now.isoformat()
            ts_epoch = now.timestamp()

            # ---- print compact line ----
            print(
                f"→ FUT LTP:{ltp:8.2f} | Bid:{best_bid_price:8.2f} "
                f"Ask:{best_ask_price:8.2f} Spread:{spread:6.2f} "
                f"| DOM5 Imb:{dom_imb: .3f} | "
                f"OHLC O:{o:.1f} H:{h:.1f} L:{l:.1f} C:{c:.1f}"
            )

            # ---- write to CSV ----
            csv_writer.writerow(
                [
                    ts_iso,
                    f"{ts_epoch:.3f}",
                    f"{ltp:.2f}",
                    f"{best_bid_price:.2f}",
                    f"{best_ask_price:.2f}",
                    f"{spread:.2f}",
                    f"{buy_qty_1_5:.2f}",
                    f"{sell_qty_1_5:.2f}",
                    f"{dom_imb:.6f}",
                    f"{o:.2f}",
                    f"{h:.2f}",
                    f"{l:.2f}",
                    f"{c:.2f}",
                ]
            )
            csv_file.flush()

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        print("\nStopped by user.")
    finally:
        csv_file.close()


if __name__ == "__main__":
    poll_fut_dom5()
