import requests
from config import DHAN_ACCESS_TOKEN, DHAN_BASE_URL, DHAN_CLIENT_ID

class DhanMarketFeed:
    def __init__(self, exchange, security_id):
        self.exchange = exchange
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
        payload = { self.exchange: [int(self.security_id)] }

        resp = requests.post(
            self.url,
            headers=self._headers(),
            json=payload,
            timeout=2
        )

        if resp.status_code != 200:
            return None

        body = resp.json()
        if body.get("status") != "success":
            return None

        return body["data"][self.exchange][self.security_id]
