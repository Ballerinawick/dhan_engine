import os
import json
import requests

BASE_URL = "https://api.dhan.co"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY0Njk5NTMzLCJpYXQiOjE3NjQ2MTMxMzMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA5MjI2MDg3In0.FCnlWatq8W1Tvr_on5qU1N9rTVsAjiYihiibPRlbXz5rHY0NoLvrR5ejLvTBP2_kj03xV0kjzw_Pd7QZrT2h_w"  # <- paste your token

def get_intraday_banknifty():
    url = f"{BASE_URL}/charts/intraday"

    payload = {
        "securityId": "25",       # from SEM_SMST_SECURITY_ID
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX"
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

    if resp.status_code == 200:
        data = resp.json()
        print("\nFirst 5 candles (open, high, low, close):")
        for i in range(min(5, len(data.get("open", [])))):
            print(
                i,
                data["open"][i],
                data["high"][i],
                data["low"][i],
                data["close"][i],
            )

if __name__ == "__main__":
    get_intraday_banknifty()
