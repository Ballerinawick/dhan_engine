import time
import requests
from typing import Dict, Tuple


PREMIUM_FILTER = {
    "NIFTY": (10, 40),
    "BANKNIFTY": (200, 450),
    "FINNIFTY": (80, 250),
}

DELTA_FILTER = {
    "NIFTY": (0.12, 0.35),
    "BANKNIFTY": (0.10, 0.32),
    "FINNIFTY": (0.10, 0.33),
}

SPREAD_MAX_PCT = {
    "NIFTY": 0.012,
    "BANKNIFTY": 0.010,
    "FINNIFTY": 0.012,
}


class OptionChainSelector:
    """
    Selects BEST CE / PE using Dhan Option Chain API.

    ✔ DOES NOT affect trading logic
    ✔ ONLY selects instruments
    ✔ BANKNIFTY 400 error FIXED (uses INDEX spot scrip)
    ✔ Logs WHY an option was selected
    """

    BASE_URL = "https://api.dhan.co/v2/optionchain"

    def __init__(
        self,
        *,
        access_token: str,
        client_id: str,
        instrument_master,
        strike_step_map: Dict[str, int],
        mode: int = 1,                 # 1 = one best, 2 = CE + PE
        max_steps_each_side: int = 10, # ± strikes around ATM
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
    # Rate limit: 1 call / 3 seconds (Dhan rule)
    # --------------------------------------------------
    def _rate_limit(self):
        now = time.time()
        wait = 3.1 - (now - self._last_call_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_call_ts = time.time()

    # --------------------------------------------------
    # 🔴 FIXED FUNCTION (BANKNIFTY BUG WAS HERE)
    # --------------------------------------------------
    def fetch_chain(self, index: str) -> dict:
        self._rate_limit()
        expiry = self.im.get_nearest_option_expiry(index, prefer_weekly=True)
        expiry_str = expiry.strftime("%Y-%m-%d")

        # ✅ FIX: Always use INDEX (spot) security id
        underlying_scrip = self.im.get_index_security_id(index)
        seg = "IDX_I"

        payload = {
            "UnderlyingScrip": int(underlying_scrip),
            "UnderlyingSeg": seg,
            "Expiry": expiry_str,
        }

        if self.debug:
            print(
                f"⛓️ OPTIONCHAIN REQ | {index} | "
                f"INDEX_SID:{underlying_scrip} | SEG:{seg} | EXP:{expiry_str}"
            )

        r = self._session.post(self.BASE_URL, json=payload, timeout=self.timeout_sec)
        r.raise_for_status()
        return r.json()["data"]

    # --------------------------------------------------
    # Scoring with explanation (unchanged)
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
    # Selection logic (unchanged)
    # --------------------------------------------------
    def select_best(self, index: str) -> Dict[str, Dict]:
        data = self.fetch_chain(index)
        oc = data["oc"]
        underlying_ltp = float(data["last_price"])

        step = self.strike_step_map[index]
        expiry = self.im.get_nearest_option_expiry(index, prefer_weekly=True)
        expiry_str = expiry.strftime("%Y-%m-%d")

        atm = int(round(underlying_ltp / step) * step)
        ce_strikes = None
        pe_strikes = None
        if index == "NIFTY":
            ce_strikes = [
                atm - 2 * step,
                atm - 1 * step,
                atm,
                atm + 1 * step,
                atm + 2 * step,
                atm + 3 * step,
                atm + 4 * step,
                atm + 5 * step,
                atm + 6 * step,
                atm + 7 * step,
            ]
            pe_strikes = [
                atm + 2 * step,
                atm + 1 * step,
                atm,
                atm - 1 * step,
                atm - 2 * step,
                atm - 3 * step,
                atm - 4 * step,
                atm - 5 * step,
                atm - 6 * step,
                atm - 7 * step,
            ]
            ce_strike_set = set(ce_strikes)
            pe_strike_set = set(pe_strikes)
        else:
            ce_low = atm + step
            ce_high = atm + self.max_steps * step
            pe_high = atm - step
            pe_low = atm - self.max_steps * step

        min_prem, max_prem = PREMIUM_FILTER[index]
        min_delta, max_delta = DELTA_FILTER[index]
        max_spread_pct = SPREAD_MAX_PCT[index]

        if self.debug:
            print(
                f"🧭 FILTERS | {index} | ATM={atm} | "
                f"CE_STRIKES={ce_strikes if ce_strikes is not None else f'[{ce_low}..{ce_high}]'} | "
                f"PE_STRIKES={pe_strikes if pe_strikes is not None else f'[{pe_low}..{pe_high}]'} | "
                f"prem={min_prem}-{max_prem} | "
                f"delta={min_delta:.2f}-{max_delta:.2f} | "
                f"spr<={max_spread_pct * 100:.1f}%"
            )

        ce_candidates = []
        pe_candidates = []
        premium_reject = 0
        delta_reject = 0
        spread_reject = 0
        otm_reject = 0

        def _evaluate_leg(leg: dict):
            ltp = float(leg.get("last_price", 0))
            if index == "NIFTY":
                if ltp < self.min_ltp:
                    return None, "premium"
            else:
                if not (min_prem <= ltp <= max_prem):
                    return None, "premium"

            g = leg.get("greeks", {}) or {}
            delta = abs(float(g.get("delta", 0)))
            if not (min_delta <= delta <= max_delta):
                return None, "delta"

            bid = float(leg.get("top_bid_price", 0) or 0)
            ask = float(leg.get("top_ask_price", 0) or 0)
            if bid <= 0 or ask <= 0 or ask < bid:
                return None, "spread"

            mid = (bid + ask) / 2.0
            spread_pct = (ask - bid) / mid if mid > 0 else 1e9
            if spread_pct > max_spread_pct:
                return None, "spread"

            return {
                "ltp": ltp,
                "delta": delta,
                "spread_pct": spread_pct,
            }, None

        for strike_str, node in oc.items():
            strike = float(strike_str)

            if "ce" in node:
                ce_in_range = strike in ce_strike_set if index == "NIFTY" else (ce_low <= strike <= ce_high)
                if not ce_in_range:
                    otm_reject += 1
                else:
                    ce_leg = node["ce"]
                    ce_meta, reject = _evaluate_leg(ce_leg)
                    if reject == "premium":
                        premium_reject += 1
                    elif reject == "delta":
                        delta_reject += 1
                    elif reject == "spread":
                        spread_reject += 1
                    elif ce_meta:
                        ce_candidates.append((strike, ce_leg, ce_meta))

            if "pe" in node:
                pe_in_range = strike in pe_strike_set if index == "NIFTY" else (pe_low <= strike <= pe_high)
                if not pe_in_range:
                    otm_reject += 1
                else:
                    pe_leg = node["pe"]
                    pe_meta, reject = _evaluate_leg(pe_leg)
                    if reject == "premium":
                        premium_reject += 1
                    elif reject == "delta":
                        delta_reject += 1
                    elif reject == "spread":
                        spread_reject += 1
                    elif pe_meta:
                        pe_candidates.append((strike, pe_leg, pe_meta))

        if self.debug:
            print(
                f"📌 CANDIDATES | {index} | CE_ok={len(ce_candidates)} "
                f"PE_ok={len(pe_candidates)} | rej_prem={premium_reject} "
                f"rej_delta={delta_reject} rej_spr={spread_reject} "
                f"rej_otm={otm_reject}"
            )

        if not ce_candidates and not pe_candidates:
            print(
                f"⚠️ NO_VALID_CONTRACT | {index} | "
                f"prem={min_prem}-{max_prem} "
                f"delta={min_delta:.2f}-{max_delta:.2f} "
                f"spr<={max_spread_pct * 100:.1f}% | widen_steps or relax filters"
            )

        if not ce_candidates or not pe_candidates:
            rel_min_prem = max(self.min_ltp, min_prem * 0.6)
            rel_max_prem = max_prem * 1.6
            rel_min_delta = max(0.01, min_delta * 0.6)
            rel_max_delta = min(0.95, max_delta * 1.8)
            rel_max_spread = max_spread_pct * 1.8
            ce_rel_low = atm + step
            ce_rel_high = atm + max(self.max_steps + 8, self.max_steps * 2) * step
            pe_rel_high = atm - step
            pe_rel_low = atm - max(self.max_steps + 8, self.max_steps * 2) * step

            if self.debug:
                print(
                    f"🛟 RELAXED_FILTERS | {index} | "
                    f"prem={rel_min_prem:.1f}-{rel_max_prem:.1f} "
                    f"delta={rel_min_delta:.2f}-{rel_max_delta:.2f} "
                    f"spr<={rel_max_spread * 100:.1f}% "
                    f"CE_OTM=[{ce_rel_low}..{ce_rel_high}] PE_OTM=[{pe_rel_high}..{pe_rel_low}]"
                )

            for strike_str, node in oc.items():
                strike = float(strike_str)

                if not ce_candidates and ("ce" in node) and (ce_rel_low <= strike <= ce_rel_high):
                    ce_leg = node["ce"]
                    ltp = float(ce_leg.get("last_price", 0) or 0)
                    g = ce_leg.get("greeks", {}) or {}
                    delta = abs(float(g.get("delta", 0) or 0))
                    bid = float(ce_leg.get("top_bid_price", 0) or 0)
                    ask = float(ce_leg.get("top_ask_price", 0) or 0)
                    mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0 and ask >= bid) else 0.0
                    spread_pct = ((ask - bid) / mid) if mid > 0 else 1e9
                    if rel_min_prem <= ltp <= rel_max_prem and rel_min_delta <= delta <= rel_max_delta and spread_pct <= rel_max_spread:
                        ce_candidates.append((strike, ce_leg, {"ltp": ltp, "delta": delta, "spread_pct": spread_pct}))

                if not pe_candidates and ("pe" in node) and (pe_rel_low <= strike <= pe_rel_high):
                    pe_leg = node["pe"]
                    ltp = float(pe_leg.get("last_price", 0) or 0)
                    g = pe_leg.get("greeks", {}) or {}
                    delta = abs(float(g.get("delta", 0) or 0))
                    bid = float(pe_leg.get("top_bid_price", 0) or 0)
                    ask = float(pe_leg.get("top_ask_price", 0) or 0)
                    mid = (bid + ask) / 2.0 if (bid > 0 and ask > 0 and ask >= bid) else 0.0
                    spread_pct = ((ask - bid) / mid) if mid > 0 else 1e9
                    if rel_min_prem <= ltp <= rel_max_prem and rel_min_delta <= delta <= rel_max_delta and spread_pct <= rel_max_spread:
                        pe_candidates.append((strike, pe_leg, {"ltp": ltp, "delta": delta, "spread_pct": spread_pct}))

            if self.debug:
                print(f"🛟 RELAXED_RESULT | {index} | CE_ok={len(ce_candidates)} PE_ok={len(pe_candidates)}")

        if (not ce_candidates) or (not pe_candidates):
            def _fallback_leg(side: str):
                best = None
                for strike_str, node in oc.items():
                    strike = float(strike_str)
                    leg = node.get("ce" if side == "CE" else "pe")
                    if not leg:
                        continue
                    if side == "CE" and strike < (atm + step):
                        continue
                    if side == "PE" and strike > (atm - step):
                        continue

                    ltp = float(leg.get("last_price", 0) or 0)
                    if ltp < self.min_ltp:
                        continue

                    bid = float(leg.get("top_bid_price", 0) or 0)
                    ask = float(leg.get("top_ask_price", 0) or 0)
                    if bid <= 0 or ask <= 0 or ask < bid:
                        continue

                    mid = (bid + ask) / 2.0
                    spread_pct = (ask - bid) / mid if mid > 0 else 1e9
                    score = spread_pct + (abs(strike - atm) / max(step, 1)) * 0.02
                    if (best is None) or (score < best[0]):
                        g = leg.get("greeks", {}) or {}
                        delta = abs(float(g.get("delta", 0) or 0))
                        best = (score, strike, leg, {"ltp": ltp, "delta": delta, "spread_pct": spread_pct})
                return None if best is None else best[1:]

            if not ce_candidates:
                ce_fb = _fallback_leg("CE")
                if ce_fb:
                    if self.debug:
                        print(f"🛟 EMERGENCY_PICK | {index} | CE strike={ce_fb[0]}")
                    ce_candidates.append(ce_fb)

            if not pe_candidates:
                pe_fb = _fallback_leg("PE")
                if pe_fb:
                    if self.debug:
                        print(f"🛟 EMERGENCY_PICK | {index} | PE strike={pe_fb[0]}")
                    pe_candidates.append(pe_fb)

        if not ce_candidates and not pe_candidates:
            return None

        best_ce = None
        best_pe = None

        for strike, ce_leg, ce_meta in ce_candidates:
            s, r = self._score_with_reason(ce_leg)
            if not best_ce or s > best_ce["score"]:
                best_ce = dict(score=s, strike=strike, reason=r, **ce_meta)

        for strike, pe_leg, pe_meta in pe_candidates:
            s, r = self._score_with_reason(pe_leg)
            if not best_pe or s > best_pe["score"]:
                best_pe = dict(score=s, strike=strike, reason=r, **pe_meta)

        out = {}

        def build(side, obj):
            if not obj:
                return None
            secid = self.im.find_option_security_id(index, expiry, obj["strike"], side)
            payload = {
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
            if self.debug:
                payload.update({
                    "ltp": round(float(obj["ltp"]), 3),
                    "delta": round(float(obj["delta"]), 4),
                    "spread_pct": round(float(obj["spread_pct"]), 6),
                })
            return payload

        ce = build("CE", best_ce)
        pe = build("PE", best_pe)

        if ce is None and pe is None:
            return None

        if self.mode == 1:
            pick = ce if ce and (not pe or ce["score"] >= pe["score"]) else pe
            if pick:
                out["BEST"] = pick
        else:
            if ce:
                out["CE"] = ce
            if pe:
                out["PE"] = pe

        if self.debug:
            if best_ce:
                print(
                    f"✅ PICK | {index} | CE strike={best_ce['strike']} "
                    f"ltp={best_ce['ltp']:.2f} delta={best_ce['delta']:.4f} "
                    f"spr={best_ce['spread_pct'] * 100:.2f}% "
                    f"score={best_ce['score']:.3f}"
                )
            if best_pe:
                print(
                    f"✅ PICK | {index} | PE strike={best_pe['strike']} "
                    f"ltp={best_pe['ltp']:.2f} delta={best_pe['delta']:.4f} "
                    f"spr={best_pe['spread_pct'] * 100:.2f}% "
                    f"score={best_pe['score']:.3f}"
                )
            for k, v in out.items():
                print(
                    f"🎯 [{index}] {k} → {v['side']} {v['strike']} "
                    f"| score={v['score']}"
                )
                print(f"    Reason: {v['reason']}")

        return out
