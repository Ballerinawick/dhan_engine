# dhan_history_test.py

import requests
import json

# 👉 better: move this to config.py or ENV later
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY0Njk5NTMzLCJpYXQiOjE3NjQ2MTMxMzMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA5MjI2MDg3In0.FCnlWatq8W1Tvr_on5qU1N9rTVsAjiYihiibPRlbXz5rHY0NoLvrR5ejLvTBP2_kj03xV0kjzw_Pd7QZrT2h_w"

BASE_URL = "https://api.dhan.co"


def test_historical_daily():
    """
    Simple test: fetch daily candles for TCS between two dates.
    Once this works, we will switch to NIFTY / BANKNIFTY, etc.
    """

    url = f"{BASE_URL}/charts/historical"

    payload = {
        "symbol": "TCS",            # equity example from docs
        "exchangeSegment": "NSE_EQ",
        "instrument": "EQUITY",
        "expiryCode": 0,            # 0 for non-derivatives
        "fromDate": "2024-11-25",   # use any past date range with trading days
        "toDate":   "2024-11-29"
    }

    headers = {
        "Content-Type": "application/json",
        "access-token": ACCESS_TOKEN,
    }

    print("Requesting:", url)
    print("Payload:", json.dumps(payload, indent=2))

    resp = requests.post(url, headers=headers, json=payload)

    print("Status code:", resp.status_code)
    print("Body:", resp.text)


if __name__ == "__main__":
    test_historical_daily()
