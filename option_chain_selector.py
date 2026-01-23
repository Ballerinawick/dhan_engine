import time
import requests
from typing import Dict, Tuple


class OptionChainSelector:
    """
    Selects BEST CE / PE using Dhan Option Chain API.

    ✔ DOES NOT affect trading logic
    ✔ ONLY adds explainable reasons + logs
    """

    BASE_URL = "https://api.dhan.co/v2/optionchain"

    DEFAULT_UNDERLYING_MAP = {
        "NIFTY": (13, "IDX_I"),
        "BANKNIFTY": (25, "IDX_I"),
        "FINNIFTY": (0, "IDX_I"),  # CSV preferred
    }

    def __init__(
        self,
        *,
        access_token: str,
        client_id: str,
        instrument_master,
        strike_step_map: Dict[str, int],
        mode: int = 1,
        max_steps_each_side: int = 10,
        min_ltp: float = 1.0,
        debug: bool = True,
        timeout_sec: float = 8.0,
    ):
        self.access_token = access_token.strip()
        self.client_id = client_id.strip()
        self.im = instrument_master
        self.strike_step_map = strike_step_map
        self.mode = mode
        self.max_steps = max_steps_each_side
        self.min_ltp = min_ltp
        self.debug = debug
        self.timeout_sec = timeout_sec

        self._last_call_ts = 0.0

        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "access-token": self.access_token,
            "client-id": self.client_id,
        })

    # --------------------------------------------------
    def _rate_limit(self):
        now = time.time()
        wait = 3.1 - (now - self._last_call_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_call_ts = time.time()

    # --------------------------------------------------
    def _detect_underlying_scrip(self, index: str) -> Tuple[int, str]:
        try:
            df = self.im.df
            cand = df[
                (df["SEM_EXM_EXCH_ID"] == "NSE")
                & (~df["SEM_INSTRUMENT_NAME"].isin(["OPTIDX", "FUTIDX"]))
                & (df["SEM_TRADING_SYMBOL"].str.contains(index))
            ]
            if not cand.empty:
                sid = int(cand.iloc[0]["SEM_SMST_SECURITY_ID"])
                return sid, "IDX_I"
        except Exception:
            pass

        sid, seg = self.DEFAULT_UNDERLYING_MAP.get(index, (0, "IDX_I"))
        if sid <= 0:
            raise RuntimeError(f"UnderlyingScrip missing for {index}")
        return sid, seg

    # --------------------------------------------------
    def fetch_chain(self, index: str) -> dict:
        self._rate_limit()

        expiry = self.im.get_nearest_option_expiry(index)
        expiry_str = expiry.strftime("%Y-%m-%d")

        underlying_scrip, seg = self._detect_underlying_scrip(index)

        payload = {
            "UnderlyingScrip": underlying_scrip,
            "UnderlyingSeg": seg,
            "Expiry": expiry_str,
        }

        if self.debug:
            print(f"⛓️ OPTIONCHAIN REQ | {index} | Underlying:{underlying_scrip} | Exp:{expiry_str}")

        r = self._session.post(self.BASE_URL, json=payload, timeout=self.timeout_sec)
        r.raise_for_status()
        return r.json()["data"]

    # --------------------------------------------------
    def _score_with_reason(self, opt: dict) -> Tuple[float, Dict]:
        ltp = float(opt.get("last_price", 0))
        if ltp < self.min_ltp:
            return -1e9, {}

        g = opt.get("greeks", {}) or {}
        delta = abs(float(g.get("delta", 0)))
        oi = float(opt.get("oi", 0))
        prev_oi = float(opt.get("previous_oi", 0))
        vol = float(opt.get("volume", 0))
        iv = float(opt.get("implied_volatility", 0))

        oi_change = oi - prev_oi
        bid = float(opt.get("top_bid_price", 0))
        ask = float(opt.get("top_ask_price", 0))
        spread = ask - bid if bid > 0 and ask > 0 else 0

        score = (
            delta * 3.0 +
            (max(oi_change, 0) / 1e6) * 2.0 +
            (vol / 1e7) * 2.0 +
            iv * 0.5
        )

        if spread > 0:
            score -= min(spread, 10) * 0.05

        reason = {
            "delta_strength": "strong" if delta > 0.45 else "weak",
            "oi_trend": "buildup" if oi_change > 0 else "unwinding",
            "volume_activity": "high" if vol > 1e6 else "normal",
            "iv_bias": "expanding" if iv > 10 else "stable",
            "liquidity": "tight" if spread < 1 else "wide",
            "final_decision": "highest composite momentum score",
        }

        return score, reason

    # --------------------------------------------------
    def select_best(self, index: str) -> Dict[str, Dict]:
        data = self.fetch_chain(index)
        oc = data["oc"]
        underlying_ltp = float(data["last_price"])

        step = self.strike_step_map[index]
        expiry = self.im.get_nearest_option_expiry(index)
        expiry_str = expiry.strftime("%Y-%m-%d")

        atm = int(round(underlying_ltp / step) * step)
        low = atm - self.max_steps * step
        high = atm + self.max_steps * step

        best_ce = None
        best_pe = None

        for strike_str, node in oc.items():
            strike = float(strike_str)
            if not (low <= strike <= high):
                continue

            if "ce" in node:
                s, r = self._score_with_reason(node["ce"])
                if not best_ce or s > best_ce["score"]:
                    best_ce = dict(score=s, strike=strike, reason=r)

            if "pe" in node:
                s, r = self._score_with_reason(node["pe"])
                if not best_pe or s > best_pe["score"]:
                    best_pe = dict(score=s, strike=strike, reason=r)

        out = {}

        def build(side, obj):
            if not obj:
                return None
            secid = self.im.find_option_security_id(index, expiry, obj["strike"], side)
            return {
                "index": index,
                "side": side,
                "strike": obj["strike"],
                "security_id": secid,
                "score": round(obj["score"], 3),
                "expiry": expiry_str,
                "atm": atm,
                "underlying_ltp": underlying_ltp,
                "reason": obj["reason"],
            }

        ce = build("CE", best_ce)
        pe = build("PE", best_pe)

        if self.mode == 1:
            pick = ce if ce and (not pe or ce["score"] >= pe["score"]) else pe
            out["BEST"] = pick
        else:
            if ce:
                out["CE"] = ce
            if pe:
                out["PE"] = pe

        if self.debug:
            for k, v in out.items():
                print(f"🎯 [{index}] {k} SELECTED → {v['side']} {v['strike']} | score={v['score']}")
                print(f"    Reason: {v['reason']}")

        return out