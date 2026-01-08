import requests
from config import DHAN_ACCESS_TOKEN, DHAN_BASE_URL, DHAN_CLIENT_ID

class DhanFutureFeed:
    def __init__(self, security_id):
        self.request_exchange = "NSE_FNO"   # what we SEND
        self.response_exchange = "NSE_EQ"  # what Dhan RETURNS
        self.security_id = str(security_id)
        self.url = f"{DHAN_BASE_URL}/v2/marketfeed/quote"

    def _headers(self):
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "access-token": DHAN_ACCESS_TOKEN,
            "client-id": DHAN_CLIENT_ID,
        }

    def fetch_tick(self):
        payload = { self.request_exchange: [int(self.security_id)] }

        try:
            resp = requests.post(
                self.url,
                headers=self._headers(),
                json=payload,
                timeout=2
            )
        except Exception:
            return None

        if resp.status_code != 200:
            return None

        body = resp.json()
        if body.get("status") != "success":
            return None

        # 🔥 CRITICAL FIX
        return body["data"].get(self.response_exchange, {}).get(self.security_id)
