# ltp_rest_engine.py
import time
from typing import Dict, List, Optional

import requests


class DhanLtpRestEngine:
    """
    Polls Dhan MarketFeed LTP REST API:
      POST https://api.dhan.co/v2/marketfeed/ltp

    - Official limit: 1 request / second.
    - Up to 1000 instruments per request.
    """

    BASE_URL = "https://api.dhan.co/v2/marketfeed/ltp"

    def __init__(self, access_token: str, client_id: str, timeout_sec: float = 8.0, debug: bool = False):
        self.access_token = (access_token or "").strip()
        self.client_id = (client_id or "").strip()
        self.timeout_sec = timeout_sec
        self.debug = debug

        if not self.access_token or not self.client_id:
            raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID for REST LTP engine")

        self._session = requests.Session()
        self._session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": self.access_token,
            "client-id": self.client_id,
        })

    def fetch_ltp_map(self, segment_to_secids: Dict[str, List[int]]) -> Optional[Dict[int, float]]:
        """
        Returns a mapping {security_id: last_price} for all requested secids.
        If API fails, returns None (caller can retry).
        """
        payload = {}
        for seg, secids in (segment_to_secids or {}).items():
            # ensure ints + unique
            cleaned = []
            seen = set()
            for s in secids:
                try:
                    si = int(s)
                except Exception:
                    continue
                if si not in seen:
                    seen.add(si)
                    cleaned.append(si)
            if cleaned:
                payload[seg] = cleaned

        if not payload:
            return None

        try:
            r = self._session.post(self.BASE_URL, json=payload, timeout=self.timeout_sec)
            if r.status_code != 200:
                if self.debug:
                    print(f"❌ LTP REST HTTP {r.status_code}: {r.text[:200]}")
                return None

            js = r.json()
            if js.get("status") != "success":
                if self.debug:
                    print(f"❌ LTP REST status not success: {js}")
                return None

            data = js.get("data", {})
            out: Dict[int, float] = {}

            for seg, secmap in data.items():
                if not isinstance(secmap, dict):
                    continue
                for secid_str, obj in secmap.items():
                    try:
                        secid = int(secid_str)
                        ltp = float(obj.get("last_price", 0.0))
                        out[secid] = ltp
                    except Exception:
                        continue

            if self.debug:
                requested = {int(secid) for secids in payload.values() for secid in secids}
                received = set(out.keys())
                missing = sorted(requested - received)

                if not out:
                    print(f"LTP REST success but no instruments returned | payload={payload}")
                elif missing:
                    print(f"LTP REST missing secids | secids={missing}")

                if out and all(float(price or 0.0) <= 0.0 for price in out.values()):
                    print(f"LTP REST returned only zero prices | secids={sorted(received)}")

            return out

        except requests.RequestException as e:
            if self.debug:
                print("❌ LTP REST request error:", str(e))
            return None

    def safe_poll_every_1s(self, segment_to_secids: Dict[str, List[int]], max_fail_print: int = 1) -> Dict[int, float]:
        """
        Polls once per second (compliant with rate limit), returns latest LTP map.
        Never raises on network errors; returns {} on failure.
        """
        ltp_map = self.fetch_ltp_map(segment_to_secids)
        if ltp_map is None:
            if max_fail_print > 0 and self.debug:
                print("⚠️ LTP REST failed this cycle (will retry next second)")
            time.sleep(1.0)
            return {}
        if not ltp_map:
            print("⚠️ REST_EMPTY_SKIP")
            time.sleep(1.0)
            return {}

        # Respect 1 req/sec
        time.sleep(1.0)
        return ltp_map
