# dhan_live_crude_test.py

import os
import json
from dotenv import load_dotenv

from dhanhq import dhanhq, marketfeed

# 1) Load credentials
load_dotenv()

ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise RuntimeError("DHAN_ACCESS_TOKEN missing in .env")

# --- derive CLIENT_ID from JWT (as we did earlier) ---
def extract_client_id_from_token(token: str) -> str:
    """
    Your Dhan token payload contains dhanClientId.
    We already saw it printed as 1109226087, but we
    derive it generically here so the script is reusable.
    """
    try:
        import base64
        header_b64, payload_b64, sig = token.split(".")
        # fix padding
        pad = "=" * (-len(payload_b64) % 4)
        payload_json = base64.urlsafe_b64decode(payload_b64 + pad).decode("utf-8")
        data = json.loads(payload_json)
        return str(data.get("dhanClientId") or data.get("dhanClientID") or data.get("clientId"))
    except Exception as e:
        print("⚠️ Could not decode client id from token, fallback to hard-coded:", e)
        # fallback to the value you already saw in logs (1109226087)
        return "1109226087"


CLIENT_ID = extract_client_id_from_token(ACCESS_TOKEN)
print("Detected CLIENT_ID from token:", CLIENT_ID)

# 2) Create REST client (sanity check)
dhan = dhanhq(CLIENT_ID, ACCESS_TOKEN)

# Inspect available segments so we don't guess
segment_constants = [name for name in dir(dhan) if name.isupper()]
print("Available exchange segments on dhan object:")
print(segment_constants)

# For commodities, MCX is exposed as 'MCX'
SEGMENT_NAME = "MCX"
if not hasattr(dhan, SEGMENT_NAME):
    raise RuntimeError(
        f"Exchange segment '{SEGMENT_NAME}' not found on dhan object. "
        f"Check printed segment_constants and adjust SEGMENT_NAME."
    )

EXCHANGE_SEGMENT = getattr(dhan, SEGMENT_NAME)

# 3) Choose CRUDEOIL FUTCOM securityId from your api-scrip-master.csv
CRUDE_FUT_SECURITY_ID = "462523"  # CRUDEOIL FUT

# For marketfeed, instruments = list of (exchange_segment, security_id)
instruments = [(EXCHANGE_SEGMENT, CRUDE_FUT_SECURITY_ID)]

# Subscription code (Ticker)
subscription_code = marketfeed.Ticker   # this is 15 (you saw in logs)


# 4) Simple synchronous message handler
def on_message(instance, message):
    """
    This is called for every tick from Dhan.
    Keep it super simple for now – just print.
    """
    try:
        text = message.decode("utf-8") if isinstance(message, (bytes, bytearray)) else str(message)
    except Exception as e:
        print("⚠️ decode error:", e)
        text = str(message)

    print("Tick:", text)


def main():
    print("Client:", CLIENT_ID)
    print("Using exchange segment constant:", SEGMENT_NAME, "=", EXCHANGE_SEGMENT)
    print("Security ID:", CRUDE_FUT_SECURITY_ID)
    print("Subscription code:", subscription_code)

    # ❗ IMPORTANT: no on_connect / on_message here
    feed = marketfeed.DhanFeed(
        CLIENT_ID,
        ACCESS_TOKEN,
        instruments,
        subscription_code,
    )

    # Many SDKs use run_forever(callback) style – try this:
    feed.run_forever(on_message)


if __name__ == "__main__":
    main()
