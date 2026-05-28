from __future__ import annotations

import logging
import os
import threading
import time

logger = logging.getLogger("dhan_engine.application.runtime")


def install_optionchain_premium_fallback() -> None:
    """Use option-chain prices as a controlled fallback when Dhan marketfeed is silent."""
    from dhan_engine.application.runtime import TradingRuntimeCoordinator

    if getattr(TradingRuntimeCoordinator, "_optionchain_premium_fallback_installed", False):
        return

    original_select_and_subscribe = TradingRuntimeCoordinator._select_and_subscribe_option_pairs

    def build_optionchain_tick(secid: int, index: str, side: str, ltp: float, source: str) -> dict:
        return {
            "ltp": float(ltp),
            "secid": int(secid),
            "tag": f"{index}_{side}",
            "ts": time.time(),
            "feature_source": source,
            "bid_price": float(ltp),
            "ask_price": float(ltp),
            "spread": 0.0,
            "spread_pct": 0.0,
            "recovery_score": 0.0,
            "clean_trade_score": 0.0,
            "exhaustion_score": 0.0,
            "flow": 0.0,
            "ofi": 0.0,
            "depth_imbalance_5": 0.0,
            "market_queue_imbalance": 0.0,
        }

    def find_cached_leg_ltp(self, index: str, side: str, secid: int):
        cache = getattr(self.selector, "_last_chain_cache", {}).get(index) or {}
        oc = cache.get("oc") or {}
        expiry = self.master.get_nearest_option_expiry(index, prefer_weekly=True)
        for strike_str, node in oc.items():
            leg = node.get(str(side).lower()) or {}
            if not leg:
                continue
            try:
                csv_secid = int(self.master.find_option_security_id(index, expiry, float(strike_str), side))
            except Exception:
                csv_secid = None
            live_secid = None
            if hasattr(self.selector, "_extract_live_security_id"):
                live_secid = self.selector._extract_live_security_id(leg)
            if secid not in {csv_secid, live_secid}:
                continue
            ltp = float(leg.get("last_price", 0.0) or 0.0)
            if ltp > 0:
                return ltp
        return None

    def apply_optionchain_tick(self, index: str, side: str, secid: int, ltp: float, source: str) -> None:
        pair = self.pairs.get(index)
        if pair is None or ltp <= 0:
            return
        raw = build_optionchain_tick(secid, index, side, ltp, source)
        with self._lock:
            if side == "CE":
                pair.ce_ltp = float(ltp)
                pair.ce_depth = raw
            elif side == "PE":
                pair.pe_ltp = float(ltp)
                pair.pe_depth = raw
            self.option_full_ltp_ts_by_secid[int(secid)] = time.time()
            self.latest_depth_features_by_secid[int(secid)] = dict(raw)
            if self.TRI_WAVE_V2_ONLY_MODE:
                self.tri_wave_recorder.record_tick(index=index, stream=side, secid=int(secid), ltp=float(ltp), features=dict(raw))
            self._process_option_update(index, pair, int(secid), f"{index}_{side}", raw)
        logger.info(
            "OPTIONCHAIN_PREMIUM_FALLBACK_TICK | index=%s | side=%s | secid=%s | ltp=%.2f | source=%s",
            index,
            side,
            int(secid),
            float(ltp),
            source,
        )

    def seed_selected_from_optionchain_cache(self, source: str = "OPTIONCHAIN_SELECTION_SEED") -> int:
        count = 0
        for index, pair in self.pairs.items():
            for side, secid in (("CE", pair.ce_id), ("PE", pair.pe_id)):
                if not secid:
                    continue
                ltp = find_cached_leg_ltp(self, index, side, int(secid))
                if ltp is None:
                    continue
                apply_optionchain_tick(self, index, side, int(secid), float(ltp), source)
                count += 1
        if count:
            logger.warning("OPTIONCHAIN_SELECTION_SEED_DONE | ticks=%s", count)
        return count

    def stale_selected_options(self) -> list[tuple[str, str, int]]:
        now = time.time()
        stale_after = float(getattr(self, "optionchain_premium_stale_after_sec", 20.0))
        out = []
        for index, pair in self.pairs.items():
            for side, secid in (("CE", pair.ce_id), ("PE", pair.pe_id)):
                if not secid:
                    continue
                tick_ts = float(getattr(self, "option_last_tick_ts_by_secid", {}).get(int(secid), 0.0) or 0.0)
                full_ts = float(getattr(self, "option_full_ltp_ts_by_secid", {}).get(int(secid), 0.0) or 0.0)
                latest = max(tick_ts, full_ts)
                if not latest or now - latest >= stale_after:
                    out.append((index, side, int(secid)))
        return out

    def optionchain_fallback_loop(self) -> None:
        logger.warning(
            "OPTIONCHAIN_PREMIUM_FALLBACK_ACTIVE | refresh_sec=%.1f | stale_after=%.1f",
            float(getattr(self, "optionchain_premium_refresh_sec", 30.0)),
            float(getattr(self, "optionchain_premium_stale_after_sec", 20.0)),
        )
        last_refresh_by_index = {}
        while not self._optionchain_premium_fallback_stop.is_set():
            try:
                if not self.market_open():
                    time.sleep(5.0)
                    continue
                stale = stale_selected_options(self)
                if not stale:
                    time.sleep(2.0)
                    continue

                now = time.time()
                refresh_sec = float(getattr(self, "optionchain_premium_refresh_sec", 30.0))
                stale_indexes = sorted({index for index, _, _ in stale})
                for index in stale_indexes:
                    if now - float(last_refresh_by_index.get(index, 0.0) or 0.0) < refresh_sec:
                        continue
                    last_refresh_by_index[index] = now
                    try:
                        self.selector.fetch_chain(index)
                    except Exception:
                        logger.exception("OPTIONCHAIN_PREMIUM_REFRESH_FAILED | index=%s", index)

                applied = 0
                for index, side, secid in stale:
                    ltp = find_cached_leg_ltp(self, index, side, secid)
                    if ltp is None:
                        continue
                    apply_optionchain_tick(self, index, side, secid, float(ltp), "OPTIONCHAIN_PREMIUM_FALLBACK")
                    applied += 1
                if not applied:
                    logger.warning("OPTIONCHAIN_PREMIUM_FALLBACK_NO_PRICE | stale=%s", stale)
            except Exception:
                logger.exception("OPTIONCHAIN_PREMIUM_FALLBACK_ERROR")
            time.sleep(2.0)

    def start_optionchain_premium_fallback(self) -> None:
        if os.getenv("OPTIONCHAIN_PREMIUM_FALLBACK", "1").strip() == "0":
            logger.info("OPTIONCHAIN_PREMIUM_FALLBACK_DISABLED")
            return
        self.optionchain_premium_refresh_sec = float(os.getenv("OPTIONCHAIN_PREMIUM_REFRESH_SEC", "30") or 30)
        self.optionchain_premium_stale_after_sec = float(os.getenv("OPTIONCHAIN_PREMIUM_STALE_AFTER_SEC", "20") or 20)
        if not hasattr(self, "_optionchain_premium_fallback_stop"):
            self._optionchain_premium_fallback_stop = threading.Event()
        thread = getattr(self, "_optionchain_premium_fallback_thread", None)
        if thread is not None and thread.is_alive():
            return
        self._optionchain_premium_fallback_stop.clear()
        thread = threading.Thread(target=optionchain_fallback_loop, args=(self,), name="OptionChainPremiumFallback", daemon=True)
        self._optionchain_premium_fallback_thread = thread
        thread.start()

    def select_and_subscribe_option_pairs(self) -> None:
        previous_rest_enabled = getattr(self, "option_rest_fallback_enabled", True)
        self.option_rest_fallback_enabled = False
        try:
            result = original_select_and_subscribe(self)
        finally:
            self.option_rest_fallback_enabled = previous_rest_enabled
        seed_selected_from_optionchain_cache(self)
        start_optionchain_premium_fallback(self)
        return result

    TradingRuntimeCoordinator._select_and_subscribe_option_pairs = select_and_subscribe_option_pairs
    TradingRuntimeCoordinator._seed_selected_from_optionchain_cache = seed_selected_from_optionchain_cache
    TradingRuntimeCoordinator._start_optionchain_premium_fallback = start_optionchain_premium_fallback
    TradingRuntimeCoordinator._optionchain_premium_fallback_installed = True
