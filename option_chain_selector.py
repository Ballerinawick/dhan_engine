# option_chain_selector.py
import time
import requests
from typing import Dict, List

class OptionChainSelector:
    """
    Selects BEST CE / PE using Dhan Option Chain API.
    Does NOT place trades.
    Does NOT stream ticks.
    Only selects instruments.
    """

    BASE_URL = "https://api.dhan.co/v2/optionchain"

    def __init__(
        self,
        access_token: str,
        client_id: str,
        instrument_master,
        mode: int = 1,   # 1 = one position, 2 = CE+PE
        max_strikes_each_side: int = 10,
        debug: bool = True,
    ):
        self.access_token = access_token
        self.client_id = client_id
        self.im = instrument_master
        self.mode = mode
        self.max_strikes = max_strikes_each_side
        self.debug = debug

        self._last_call_ts = 0.0
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "access-token": access_token,
            "client-id": client_id,
        })

    # --------------------------------------------------
    def _rate_limit(self):
        now = time.time()
        if now - self._last_call_ts < 3.1:
            time.sleep(3.1 - (now - self._last_call_ts))
        self._last_call_ts = time.time()

    # --------------------------------------------------
    def fetch_chain(self, index: str):
        self._rate_limit()

        fut = self.im.get_nearest_future(index)
        expiry = self.im.get_nearest_option_expiry(index)

        payload = {
            "UnderlyingScrip": int(fut["security_id"]),
            "UnderlyingSeg": "IDX_I",
            "Expiry": expiry.strftime("%Y-%m-%d"),
        }

        r = self._session.post(self.BASE_URL, json=payload, timeout=8)
        r.raise_for_status()
        return r.json()["data"]

    # --------------------------------------------------
    def _score_option(self, opt: dict, side: str) -> float:
        g = opt.get("greeks", {})
        delta = abs(g.get("delta", 0))
        oi = opt.get("oi", 0)
        prev_oi = opt.get("previous_oi", 0)
        vol = opt.get("volume", 0)
        iv = opt.get("implied_volatility", 0)

        oi_change = max(oi - prev_oi, 0)

        score = (
            delta * 3.0 +
            (oi_change / 1e6) * 2.0 +
            (vol / 1e7) * 2.0 +
            iv * 0.5
        )
        return score

    # --------------------------------------------------
    def select_best(self, index: str) -> Dict[str, Dict]:
        data = self.fetch_chain(index)
        oc = data.get("oc", {})

        ce_scores = []
        pe_scores = []

        for strike, node in oc.items():
            if "ce" in node:
                ce_scores.append((self._score_option(node["ce"], "CE"), strike, node["ce"]))
            if "pe" in node:
                pe_scores.append((self._score_option(node["pe"], "PE"), strike, node["pe"]))

        ce_scores.sort(reverse=True)
        pe_scores.sort(reverse=True)

        result = {}

        if self.mode == 1:
            best_ce = ce_scores[0] if ce_scores else None
            best_pe = pe_scores[0] if pe_scores else None

            if best_ce and (not best_pe or best_ce[0] > best_pe[0]):
                result["CE"] = best_ce
            elif best_pe:
                result["PE"] = best_pe

        else:  # mode 2
            if ce_scores:
                result["CE"] = ce_scores[0]
            if pe_scores:
                result["PE"] = pe_scores[0]

        if self.debug:
            print(f"🎯 [{index}] Selected:", result)

        return result
