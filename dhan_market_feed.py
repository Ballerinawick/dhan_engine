# dhan_market_feed.py
import os
import requests
from dotenv import load_dotenv

load_dotenv()

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID")
DHAN_BASE_URL     = os.getenv("DHAN_BASE_URL", "https://api.dhan.co").rstrip("/")


class DhanMarketFeed:
    def __init__(self, exchange_segment: str, security_id: str):
        self.exchange = exchange_segment
        self.security_id = str(security_id)
        self.url = f"{DHAN_BASE_URL}/v2/marketfeed/quote"

        if not DHAN_ACCESS_TOKEN or not DHAN_CLIENT_ID:
            raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID in .env")

    def _headers(self):
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "access-token": DHAN_ACCESS_TOKEN,
            "client-id": DHAN_CLIENT_ID,
        }

    def fetch_tick(self):
        payload = {self.exchange: [int(self.security_id)]}

        try:
            resp = requests.post(
                self.url,
                headers=self._headers(),
                json=payload,
                timeout=3
            )
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            # IMPORTANT: soft-fail, do not crash engine
            print(f"⚠️ FUT REST error [{self.exchange}:{self.security_id}] -> {e}")
            return None

        body = resp.json()
        if body.get("status") != "success":
            return None

        return body.get("data", {}).get(self.exchange, {}).get(self.security_id)
