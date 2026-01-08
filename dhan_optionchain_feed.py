# dhan_optionchain_feed.py
import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID", "").strip()
DHAN_BASE_URL     = os.getenv("DHAN_BASE_URL", "https://api.dhan.co").rstrip("/")

if not DHAN_ACCESS_TOKEN or not DHAN_CLIENT_ID:
    raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID in .env")


class DhanOptionChainFeed:
    """
    Safe OptionChain client
    - HARD throttling
    - retry on timeout
    - NEVER crashes WS loop
    """

    def __init__(self):
        self._sess = requests.Session()
        self._last_call_ts = 0.0

        # expose for WS
        self.access_token = DHAN_ACCESS_TOKEN
        self.client_id = DHAN_CLIENT_ID

    def _headers(self):
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "access-token": DHAN_ACCESS_TOKEN,
            "client-id": DHAN_CLIENT_ID,
        }

    def _throttle(self, min_gap_sec: float = 6.0):
        now = time.time()
        wait = (self._last_call_ts + min_gap_sec) - now
        if wait > 0:
            time.sleep(wait)
        self._last_call_ts = time.time()

    # -------------------------------------------------
    # EXPIRY LIST (SAFE)
    # -------------------------------------------------
    def expiry_list(self, underlying_scrip: int, underlying_seg: str):
        self._throttle()

        url = f"{DHAN_BASE_URL}/v2/optionchain/expirylist"
        payload = {
            "UnderlyingScrip": int(underlying_scrip),
            "UnderlyingSeg": underlying_seg
        }

        for _ in range(2):  # retry once
            try:
                r = self._sess.post(
                    url,
                    headers=self._headers(),
                    json=payload,
                    timeout=12
                )
                r.raise_for_status()
                body = r.json()
                if body.get("status") == "success":
                    return body.get("data", []) or []
            except Exception:
                time.sleep(1.5)

        return []

    # -------------------------------------------------
    # OPTION CHAIN (SAFE)
    # -------------------------------------------------
    def option_chain(self, underlying_scrip: int, underlying_seg: str, expiry: str):
        self._throttle()

        url = f"{DHAN_BASE_URL}/v2/optionchain"
        payload = {
            "UnderlyingScrip": int(underlying_scrip),
            "UnderlyingSeg": underlying_seg,
            "Expiry": expiry
        }

        for _ in range(2):
            try:
                r = self._sess.post(
                    url,
                    headers=self._headers(),
                    json=payload,
                    timeout=12
                )
                r.raise_for_status()
                body = r.json()
                if body.get("status") == "success":
                    return body.get("data")
            except Exception:
                time.sleep(1.5)

        return None
