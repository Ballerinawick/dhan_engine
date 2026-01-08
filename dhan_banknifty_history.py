# dhan_banknifty_history.py

import os
import json
import requests
from datetime import date, timedelta

from config import DHAN_ACCESS_TOKEN, DHAN_BASE_URL

HISTORICAL_URL = f"{DHAN_BASE_URL}/charts/historical"

def main():
    # last 10 trading days for safety
    to_date = date(2024, 10, 10)        # you can adjust this
    from_date = to_date - timedelta(days=10)

    payload = {
        "symbol": "BANKNIFTY",
        "exchangeSegment": "IDX_I",   # index segment (from your CSV)
        "instrument": "INDEX",
        "expiryCode": 0,
        "fromDate": from_date.strftime("%Y-%m-%d"),
        "toDate": to_date.strftime("%Y-%m-%d"),
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "access-token": DHAN_ACCESS_TOKEN,
    }

    print(f"Requesting: {HISTORICAL_URL}")
    print("Payload:", json.dumps(payload, indent=2))

    resp = requests.post(HISTORICAL_URL, headers=headers, json=payload)
    print("Status code:", resp.status_code)
    print("Body:", resp.text)

    if resp.status_code == 200:
        data = resp.json()
        print("\nParsed:")
        print("opens :", data.get("open"))
        print("highs :", data.get("high"))
        print("lows  :", data.get("low"))
        print("closes:", data.get("close"))
        print("vol   :", data.get("volume"))
    else:
        print("❌ BANKNIFTY historical failed")

if __name__ == "__main__":
    main()
