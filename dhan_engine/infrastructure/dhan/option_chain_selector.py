import time
import requests
from typing import Dict, Tuple


PREMIUM_FILTER = {
    "NIFTY": (10, 40),
    "BANKNIFTY": (200, 450),
    "FINNIFTY": (80, 250),
}

DELTA_FILTER = {
    "NIFTY": (0.35, 0.65),
    "BANKNIFTY": (0.30, 0.60),
    "FINNIFTY": (0.30, 0.60),
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
        self._last_chain_cache = {}
        self._last_chain_cache_ts = {}
        self._last_fetch_ok = {}
        self._last_fetch_error = {}
        self._last_fetch_retries = {}

        self._session = requests.Session()
        self._session.headers.update({
            "Content-Type": "application/json",
            "access-token": self.access_token,
            "client-id": self.client_id,
        })

    @staticmethod
    def _extract_live_security_id(leg: dict):
        for key in ("security_id", "securityId", "sec_id", "SecurityId"):
            raw = leg.get(key)
            if raw is None:
                continue
            try:
                return int(raw)
            except Exception:
                continue
        return None

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
    def fetch_chain(self, index: str) -> dict | None:
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

        for attempt in range(1, 4):
            try:
                r = self._session.post(self.BASE_URL, json=payload, timeout=10)
                r.raise_for_status()
                body = r.json()
                data = body.get("data")
                if not data or "oc" not in data:
                    raise ValueError(f"Invalid option chain response keys={list(body.keys())}")

                self._last_chain_cache[index] = data
                self._last_chain_cache_ts[index] = time.time()
                self._last_fetch_ok[index] = True
                self._last_fetch_error[index] = None
                self._last_fetch_retries[index] = attempt - 1

                if self.debug:
                    print(f"OPTION_CHAIN_FETCH_OK | {index} | attempt={attempt} | expiry={expiry_str}")
                return data
            except Exception as exc:
                self._last_fetch_ok[index] = False
                self._last_fetch_error[index] = str(exc)
                self._last_fetch_retries[index] = attempt
                print(f"OPTION_CHAIN_FETCH_RETRY | index={index} | attempt={attempt} | error={exc}")
                if attempt < 3:
                    time.sleep([1, 2, 4][attempt - 1])

        cached = self._last_chain_cache.get(index)
        print(f"OPTION_CHAIN_FETCH_FAILED_USING_CACHE | index={index} | cache_available={cached is not None}")
        if cached is not None:
            return cached
        return None

    def get_health(self, index: str) -> dict:
        cache_ts = float(self._last_chain_cache_ts.get(index, 0.0) or 0.0)
        return {
            "last_fetch_ok": bool(self._last_fetch_ok.get(index, False)),
            "last_fetch_error": self._last_fetch_error.get(index),
            "last_fetch_retries": int(self._last_fetch_retries.get(index, 0) or 0),
            "cache_available": index in self._last_chain_cache,
            "cache_age": time.time() - cache_ts if cache_ts else None,
        }

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
    def select_best(self, index: str, underlying_ltp_override: float | None = None) -> Dict[str, Dict]:
        data = self.fetch_chain(index)
        if not data:
            print(f"OPTION_CHAIN_NO_DATA | {index} | select_best skipped safely")
            return None
        if "oc" not in data or not data["oc"]:
            print(f"OPTION_CHAIN_EMPTY_OC | {index} | select_best skipped safely")
            return None
        oc = data["oc"]
        if underlying_ltp_override is not None and float(underlying_ltp_override or 0.0) > 0.0:
            underlying_ltp = float(underlying_ltp_override)
        else:
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
        else:
            ce_strikes = [atm + i * step for i in range(1, self.max_steps + 1)]
            pe_strikes = [atm - i * step for i in range(1, self.max_steps + 1)]

        ce_strike_set = set(ce_strikes)
        pe_strike_set = set(pe_strikes)

        min_prem, max_prem = PREMIUM_FILTER[index]
        min_delta, max_delta = DELTA_FILTER[index]
        max_spread_pct = SPREAD_MAX_PCT[index]

        if self.debug:
            print(
                f"🧭 FILTERS | {index} | ATM={atm} | "
                f"CE_STRIKES={ce_strikes} | "
                f"PE_STRIKES={pe_strikes} | "
                f"spread_max_pct={max_spread_pct * 100:.1f}%"
            )

        ce_candidates = []
        pe_candidates = []
        premium_reject = 0
        delta_reject = 0
        spread_reject = 0
        otm_reject = 0

        wp = 2.0
        wd = 1.5

        def _calculate_penalty(value: float, low: float, high: float) -> float:
            penalty = 0.0
            if value < low and low > 0:
                penalty += (low - value) / low
            if value > high and high > 0:
                penalty += (value - high) / high
            return penalty

        def _evaluate_leg(leg: dict):
            ltp = float(leg.get("last_price", 0))
            if ltp < self.min_ltp:
                return None, "premium"

            g = leg.get("greeks", {}) or {}
            delta = abs(float(g.get("delta", 0)))
            premium_penalty = _calculate_penalty(ltp, min_prem, max_prem)
            # SOFT penalty only (do NOT reject here)
            delta_penalty = _calculate_penalty(delta, min_delta, max_delta)

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
                "premium_penalty": premium_penalty,
                "delta_penalty": delta_penalty,
            }, None

        def _collect_candidates(target_ce_set, target_pe_set):
            local_ce_candidates = []
            local_pe_candidates = []
            local_premium_reject = 0
            local_spread_reject = 0
            local_otm_reject = 0

            for strike_str, node in oc.items():
                strike = float(strike_str)

                if "ce" in node:
                    if strike not in target_ce_set:
                        local_otm_reject += 1
                    else:
                        ce_leg = node["ce"]
                        ce_meta, reject = _evaluate_leg(ce_leg)
                        if reject == "premium":
                            local_premium_reject += 1
                        elif reject == "spread":
                            local_spread_reject += 1
                        elif ce_meta:
                            local_ce_candidates.append((strike, ce_leg, ce_meta))

                if "pe" in node:
                    if strike not in target_pe_set:
                        local_otm_reject += 1
                    else:
                        pe_leg = node["pe"]
                        pe_meta, reject = _evaluate_leg(pe_leg)
                        if reject == "premium":
                            local_premium_reject += 1
                        elif reject == "spread":
                            local_spread_reject += 1
                        elif pe_meta:
                            local_pe_candidates.append((strike, pe_leg, pe_meta))

            return (
                local_ce_candidates,
                local_pe_candidates,
                local_premium_reject,
                local_spread_reject,
                local_otm_reject,
            )

        (
            ce_candidates,
            pe_candidates,
            premium_reject,
            spread_reject,
            otm_reject,
        ) = _collect_candidates(ce_strike_set, pe_strike_set)

        available_strikes = sorted(float(s) for s in oc.keys())
        min_strike = available_strikes[0] if available_strikes else atm
        max_strike = available_strikes[-1] if available_strikes else atm

        if not ce_candidates:
            if self.debug:
                print(f"⚠️ CE_NO_CANDIDATE expanding strikes | {index}")
            ce_expanded = [
                s for s in [ce_strikes[-1] + i * step for i in range(1, 4)]
                if min_strike <= s <= max_strike
            ]
            if ce_expanded:
                ce_expanded_candidates, _, ce_premium_reject, ce_spread_reject, ce_otm_reject = _collect_candidates(
                    set(ce_strikes + ce_expanded),
                    set(),
                )
                ce_candidates = ce_expanded_candidates
                premium_reject += ce_premium_reject
                spread_reject += ce_spread_reject
                otm_reject += ce_otm_reject

        if not pe_candidates:
            if self.debug:
                print(f"⚠️ PE_NO_CANDIDATE expanding strikes | {index}")
            pe_expanded = [
                s for s in [pe_strikes[-1] - i * step for i in range(1, 4)]
                if min_strike <= s <= max_strike
            ]
            if pe_expanded:
                _, pe_expanded_candidates, pe_premium_reject, pe_spread_reject, pe_otm_reject = _collect_candidates(
                    set(),
                    set(pe_strikes + pe_expanded),
                )
                pe_candidates = pe_expanded_candidates
                premium_reject += pe_premium_reject
                spread_reject += pe_spread_reject
                otm_reject += pe_otm_reject

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
                f"spr<={max_spread_pct * 100:.1f}% | widen_steps"
            )

        if not ce_candidates and not pe_candidates:
            return None

        best_ce = None
        best_pe = None

        # -------- CE SELECTION --------

        ce_scored = []

        for strike, ce_leg, ce_meta in ce_candidates:
            base_score, r = self._score_with_reason(ce_leg)

            s = base_score - (ce_meta["premium_penalty"] * wp) - (ce_meta["delta_penalty"] * wd)

            ce_scored.append({
                "score": s,
                "strike": strike,
                "reason": r,
                "live_security_id": self._extract_live_security_id(ce_leg),
                **ce_meta,
            })

        # STRICT FILTER
        strict_ce = [
            x for x in ce_scored
            if min_delta <= x["delta"] <= max_delta
        ]

        # FALLBACK FILTER
        if strict_ce:
            ce_pool = strict_ce
        else:
            ce_pool = [
                x for x in ce_scored
                if 0.25 <= x["delta"] <= 0.75
            ]

        best_ce = max(ce_pool, key=lambda x: x["score"]) if ce_pool else None

        # -------- PE SELECTION --------

        pe_scored = []

        for strike, pe_leg, pe_meta in pe_candidates:
            base_score, r = self._score_with_reason(pe_leg)

            s = base_score - (pe_meta["premium_penalty"] * wp) - (pe_meta["delta_penalty"] * wd)

            pe_scored.append({
                "score": s,
                "strike": strike,
                "reason": r,
                "live_security_id": self._extract_live_security_id(pe_leg),
                **pe_meta,
            })

        # STRICT FILTER
        strict_pe = [
            x for x in pe_scored
            if min_delta <= x["delta"] <= max_delta
        ]

        # FALLBACK FILTER
        if strict_pe:
            pe_pool = strict_pe
        else:
            pe_pool = [
                x for x in pe_scored
                if 0.25 <= x["delta"] <= 0.75
            ]

        best_pe = max(pe_pool, key=lambda x: x["score"]) if pe_pool else None

        out = {}

        def build(side, obj):
            if not obj:
                return None
            secid = int(self.im.find_option_security_id(index, expiry, obj["strike"], side))
            live_secid = obj.get("live_security_id")
            if self.debug and live_secid:
                try:
                    live_secid = int(live_secid)
                    if live_secid != secid:
                        print(
                            f"⚠️ OPTIONCHAIN_SECURITY_ID_MISMATCH | {index} {side} {obj['strike']} "
                            f"| exp={expiry_str} | optionchain={live_secid} | csv={secid} | using CSV"
                        )
                except Exception:
                    pass
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
                    f"PICK | CE strike={best_ce['strike']} "
                    f"ltp={best_ce['ltp']:.2f} delta={best_ce['delta']:.4f} "
                    f"spr%={best_ce['spread_pct'] * 100:.2f}% "
                    f"score={best_ce['score']:.3f}"
                )
            if best_pe:
                print(
                    f"PICK | PE strike={best_pe['strike']} "
                    f"ltp={best_pe['ltp']:.2f} delta={best_pe['delta']:.4f} "
                    f"spr%={best_pe['spread_pct'] * 100:.2f}% "
                    f"score={best_pe['score']:.3f}"
                )
            for k, v in out.items():
                print(
                    f"🎯 [{index}] {k} → {v['side']} {v['strike']} "
                    f"| score={v['score']}"
                )
                print(f"    Reason: {v['reason']}")

        return out
