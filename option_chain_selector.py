import time
import requests
from typing import Dict, Optional, Tuple


class OptionChainSelector:
    """
    Selects BEST CE / PE using Dhan Option Chain API.

    ✅ Uses correct UnderlyingScrip (INDEX spot scrip, not FUT)
    ✅ Rate-limit: 1 call per 3 seconds
    ✅ Restricts scoring to strikes near ATM (fast + relevant)
    ✅ Returns SecurityId(s) ready for WS subscribe
       - mode=1: picks ONE best (CE or PE)
       - mode=2: picks best CE and best PE (two instruments)

    NOTE:
      - This class ONLY selects instruments. No trading. No WS.
      - It needs instrument_master (your existing InstrumentMaster instance).
    """

    BASE_URL = "https://api.dhan.co/v2/optionchain"

    # Known common underlying IDs (per Dhan examples seen widely)
    # If CSV detection works, it will override these.
    DEFAULT_UNDERLYING_MAP = {
        "NIFTY": (13, "IDX_I"),
        "BANKNIFTY": (25, "IDX_I"),
        # FINNIFTY: will try CSV detection; if not found, fill manually once
        "FINNIFTY": (0, "IDX_I"),
    }

    def __init__(
        self,
        *,
        access_token: str,
        client_id: str,
        instrument_master,
        strike_step_map: Dict[str, int],
        mode: int = 1,                 # 1=ONE best (CE or PE), 2=best CE + best PE
        max_steps_each_side: int = 10, # e.g. 10 steps each side around ATM
        min_ltp: float = 1.0,          # ignore dead options
        debug: bool = True,
        timeout_sec: float = 8.0,
    ):
        self.access_token = (access_token or "").strip()
        self.client_id = (client_id or "").strip()
        self.im = instrument_master
        self.strike_step_map = strike_step_map

        self.mode = int(mode)
        self.max_steps = int(max_steps_each_side)
        self.min_ltp = float(min_ltp)
        self.debug = bool(debug)
        self.timeout_sec = float(timeout_sec)

        if not self.access_token or not self.client_id:
            raise RuntimeError("Missing DHAN access token / client id for option chain selector")

        self._last_call_ts = 0.0
        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "access-token": self.access_token,
            "client-id": self.client_id,
        })

    # --------------------------------------------------
    # Rate limit: once per 3 sec
    # --------------------------------------------------
    def _rate_limit(self):
        now = time.time()
        wait = 3.1 - (now - self._last_call_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_call_ts = time.time()

    # --------------------------------------------------
    # Try detect underlying scrip id from CSV (best)
    # Fallback to DEFAULT_UNDERLYING_MAP
    # --------------------------------------------------
    def _detect_underlying_scrip(self, index: str) -> Tuple[int, str]:
        idx = str(index).upper().strip()

        # 1) Try CSV detection (if your csv includes index spot rows)
        try:
            df = getattr(self.im, "df", None)
            if df is not None and len(df) > 0:
                # Heuristic search: look for rows that match idx in trading symbol and look like index spot.
                # We keep it safe: if not confident, we fallback.
                cand = df[
                    (df["SEM_EXM_EXCH_ID"].astype(str).str.upper() == "NSE")
                    &
                    (df["SEM_TRADING_SYMBOL"].astype(str).str.upper().str.contains(idx, na=False))
                ].copy()

                # Prefer rows that are NOT OPTIDX/FUTIDX and have no strike/option type.
                if "SEM_INSTRUMENT_NAME" in cand.columns:
                    bad = cand["SEM_INSTRUMENT_NAME"].astype(str).str.upper().isin(["OPTIDX", "FUTIDX"])
                    cand = cand[~bad].copy()

                # If any candidate remains, pick the first with valid security id.
                if not cand.empty:
                    cand = cand.dropna(subset=["SEM_SMST_SECURITY_ID"])
                    if not cand.empty:
                        sid = int(float(cand.iloc[0]["SEM_SMST_SECURITY_ID"]))
                        # UnderlyingSeg for index is IDX_I as per Dhan docs example
                        return sid, "IDX_I"
        except Exception:
            pass

        # 2) Fallback defaults
        sid, seg = self.DEFAULT_UNDERLYING_MAP.get(idx, (0, "IDX_I"))
        if sid <= 0:
            # FINNIFTY likely lands here if CSV detection failed
            raise RuntimeError(
                f"UnderlyingScrip not found for {idx}. "
                f"Update DEFAULT_UNDERLYING_MAP['{idx}'] with correct scrip id OR ensure CSV has index spot row."
            )
        return int(sid), str(seg)

    # --------------------------------------------------
    def fetch_chain(self, index: str) -> dict:
        self._rate_limit()

        idx = str(index).upper().strip()
        expiry = self.im.get_nearest_option_expiry(idx)

        underlying_scrip, underlying_seg = self._detect_underlying_scrip(idx)

        payload = {
            "UnderlyingScrip": int(underlying_scrip),
            "UnderlyingSeg": str(underlying_seg),
            "Expiry": expiry.strftime("%Y-%m-%d"),
        }

        if self.debug:
            print(f"⛓️ OPTIONCHAIN REQ | {idx} | Underlying:{underlying_scrip} {underlying_seg} | Exp:{payload['Expiry']}")

        r = self._session.post(self.BASE_URL, json=payload, timeout=self.timeout_sec)
        if r.status_code != 200:
            raise RuntimeError(f"OptionChain HTTP {r.status_code}: {r.text[:200]}")
        js = r.json()
        data = js.get("data")
        if not data:
            raise RuntimeError(f"OptionChain empty data for {idx}. Resp: {str(js)[:200]}")
        return data

    # --------------------------------------------------
    # Score: simple + robust (no magic)
    # --------------------------------------------------
    def _score_option(self, opt: dict) -> float:
        if not isinstance(opt, dict):
            return -1e9

        ltp = float(opt.get("last_price", 0) or 0)
        if ltp < self.min_ltp:
            return -1e9

        g = opt.get("greeks", {}) or {}
        delta = abs(float(g.get("delta", 0) or 0))

        oi = float(opt.get("oi", 0) or 0)
        prev_oi = float(opt.get("previous_oi", 0) or 0)
        oi_change = max(oi - prev_oi, 0.0)

        vol = float(opt.get("volume", 0) or 0)
        iv = float(opt.get("implied_volatility", 0) or 0)

        # Liquidity proxy: tighter spreads are better (if present)
        bid = float(opt.get("top_bid_price", 0) or 0)
        ask = float(opt.get("top_ask_price", 0) or 0)
        spread = (ask - bid) if (ask > 0 and bid > 0) else 0.0

        # Score weights (kept stable, not overfit)
        score = (
            delta * 3.0 +
            (oi_change / 1e6) * 2.0 +
            (vol / 1e7) * 2.0 +
            (iv * 0.5)
        )

        # Penalize huge spread a bit
        if spread > 0:
            score -= min(spread, 10.0) * 0.05

        return float(score)

    # --------------------------------------------------
    def select_best(self, index: str) -> Dict[str, Dict]:
        """
        Returns:
          mode=1:
            {"BEST": {"side":"CE"/"PE", "security_id":int, "strike":float, "score":float, "expiry":"YYYY-MM-DD"}}
          mode=2:
            {"CE": {...}, "PE": {...}}
        """
        idx = str(index).upper().strip()
        data = self.fetch_chain(idx)

        oc = data.get("oc", {}) or {}
        underlying_ltp = float(data.get("last_price", 0) or 0)

        step = int(self.strike_step_map.get(idx, 50))
        expiry = self.im.get_nearest_option_expiry(idx)
        expiry_str = expiry.strftime("%Y-%m-%d")

        if underlying_ltp <= 0:
            raise RuntimeError(f"OptionChain underlying last_price is invalid for {idx}: {underlying_ltp}")

        # compute ATM strike
        atm = int(round(underlying_ltp / step) * step)

        # restrict to near-ATM strikes (faster + relevant)
        strike_low = atm - (self.max_steps * step)
        strike_high = atm + (self.max_steps * step)

        best_ce = None  # (score, strike, opt_dict)
        best_pe = None

        for strike_str, node in oc.items():
            try:
                strike = float(strike_str)
            except Exception:
                continue

            if strike < strike_low or strike > strike_high:
                continue

            if not isinstance(node, dict):
                continue

            ce = node.get("ce")
            pe = node.get("pe")

            if isinstance(ce, dict):
                s = self._score_option(ce)
                if best_ce is None or s > best_ce[0]:
                    best_ce = (s, strike, ce)

            if isinstance(pe, dict):
                s = self._score_option(pe)
                if best_pe is None or s > best_pe[0]:
                    best_pe = (s, strike, pe)

        out: Dict[str, Dict] = {}

        # Resolve to SECURITY IDs using your InstrumentMaster (derivatives OPTIDX)
        def resolve(side: str, item):
            if not item:
                return None
            score, strike, _ = item
            try:
                secid = self.im.find_option_security_id(idx, expiry, strike, side)
            except Exception as e:
                if self.debug:
                    print(f"⚠️ Resolve failed {idx} {side} strike={strike}: {e}")
                return None
            return {
                "index": idx,
                "side": side,
                "strike": float(strike),
                "security_id": int(secid),
                "score": float(score),
                "expiry": expiry_str,
                "underlying_ltp": float(underlying_ltp),
                "atm": float(atm),
            }

        r_ce = resolve("CE", best_ce)
        r_pe = resolve("PE", best_pe)

        if self.mode == 1:
            # pick ONE best between CE and PE
            pick = None
            if r_ce and r_pe:
                pick = r_ce if r_ce["score"] >= r_pe["score"] else r_pe
            else:
                pick = r_ce or r_pe

            if not pick:
                if self.debug:
                    print(f"🎯 [{idx}] No selectable option found near ATM.")
                return {}

            out["BEST"] = pick

        else:
            # mode 2: allow both
            if r_ce:
                out["CE"] = r_ce
            if r_pe:
                out["PE"] = r_pe

        if self.debug:
            print(f"🎯 [{idx}] Selected: {out}")

        return out