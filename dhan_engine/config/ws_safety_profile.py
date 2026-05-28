from __future__ import annotations

import logging
import os
import time
from typing import Optional

logger = logging.getLogger("dhan_engine.application.runtime")


def install_ws_safety_profile() -> None:
    """Install websocket startup and stale-price guards without changing strategy rules."""
    from dhan_engine.application.runtime import TradingRuntimeCoordinator
    from dhan_engine.infrastructure.dhan.ltp_rest_engine import DhanLtpRestEngine
    from dhan_engine.infrastructure.dhan.marketfeed_ws import DhanLiveMarketFeedWS

    if getattr(TradingRuntimeCoordinator, "_ws_safety_profile_installed", False):
        return

    original_runtime_init = TradingRuntimeCoordinator.__init__
    original_execute_tri_wave_signal = TradingRuntimeCoordinator._execute_tri_wave_signal
    original_ws_init = DhanLiveMarketFeedWS.__init__
    original_ws_on_error = DhanLiveMarketFeedWS._on_error
    original_ws_on_close = DhanLiveMarketFeedWS._on_close
    original_ws_on_message = DhanLiveMarketFeedWS._on_message

    def runtime_init(self, *args, **kwargs):
        original_runtime_init(self, *args, **kwargs)
        settings = self.settings
        self.option_full_ltp_ts_by_secid = getattr(self, "option_full_ltp_ts_by_secid", {})
        self.full_quote_ltp_fresh_sec = float(os.getenv("FULL_QUOTE_LTP_FRESH_SEC", "15") or 15)
        self.trade_price_fresh_sec = float(os.getenv("TRADE_PRICE_FRESH_SEC", "20") or 20)
        self.future_startup_rest_fallback_sec = float(os.getenv("FUTURE_STARTUP_REST_FALLBACK_SEC", "8") or 8)
        self.future_startup_option_chain_fallback_sec = float(
            os.getenv("FUTURE_STARTUP_OPTION_CHAIN_FALLBACK_SEC", "12") or 12
        )
        self.future_startup_fallback_retry_sec = float(os.getenv("FUTURE_STARTUP_FALLBACK_RETRY_SEC", "15") or 15)
        self._last_startup_rest_fallback_ts = 0.0
        self._last_startup_chain_fallback_ts = 0.0
        self.future_ltp_rest = DhanLtpRestEngine(
            access_token=settings.credentials.access_token,
            client_id=settings.credentials.client_id,
            timeout_sec=5.0,
            debug=False,
        )

    def is_full_ltp_fresh(self, secid: int, now: Optional[float] = None) -> bool:
        ts = getattr(self, "option_full_ltp_ts_by_secid", {}).get(int(secid))
        if not ts:
            return False
        return ((now or time.time()) - float(ts)) <= float(getattr(self, "full_quote_ltp_fresh_sec", 15.0))

    def leg_full_age(self, pair, side: str) -> Optional[float]:
        secid = pair.ce_id if side == "CE" else pair.pe_id if side == "PE" else None
        if not secid:
            return None
        ts = getattr(self, "option_full_ltp_ts_by_secid", {}).get(int(secid))
        return time.time() - float(ts) if ts else None

    def leg_depth_age(self, pair, side: str) -> Optional[float]:
        payload = pair.ce_depth if side == "CE" else pair.pe_depth if side == "PE" else None
        if not payload:
            return None
        ts = float(payload.get("ts", 0.0) or 0.0)
        return time.time() - ts if ts else None

    def fresh_leg_ltp(self, pair, side: str, fallback_ltp: float = 0.0) -> tuple[float, str, bool]:
        side = str(side).upper()
        secid = pair.ce_id if side == "CE" else pair.pe_id if side == "PE" else None
        depth_payload = pair.ce_depth if side == "CE" else pair.pe_depth if side == "PE" else None
        ws_ltp = pair.ce_ltp if side == "CE" else pair.pe_ltp if side == "PE" else None
        now = time.time()

        if secid and ws_ltp and ws_ltp > 0 and is_full_ltp_fresh(self, int(secid), now):
            return float(ws_ltp), "FULL_QUOTE", True

        if depth_payload and depth_payload.get("ltp"):
            ts = float(depth_payload.get("ts", 0.0) or 0.0)
            is_fresh = bool(ts and (now - ts) <= float(getattr(self, "trade_price_fresh_sec", 20.0)))
            return float(depth_payload["ltp"]), "DEPTH", is_fresh

        return float(fallback_ltp or 0.0), "FALLBACK", False

    def fresh_leg_ltp_or_block(self, pair, side: str, fallback_ltp: float, action: str) -> Optional[float]:
        ltp, source, fresh = fresh_leg_ltp(self, pair, side, fallback_ltp)
        if fresh and ltp > 0:
            return ltp
        logger.warning(
            "TRI_WAVE_STALE_PRICE_BLOCK | index=%s | side=%s | action=%s | price=%.2f | source=%s | full_age=%s | depth_age=%s",
            pair.index,
            side,
            action,
            ltp,
            source,
            leg_full_age(self, pair, side),
            leg_depth_age(self, pair, side),
        )
        return None

    def seed_missing_underlyings_from_rest(self) -> None:
        missing = [
            int(self.future_secids[index])
            for index in self.settings.indexes
            if not self.pairs[index].underlying_ltp and index in self.future_secids
        ]
        if not missing:
            return
        try:
            prices = self.future_ltp_rest.fetch_ltp_map({self.settings.future_exchange_segment: missing}) or {}
        except Exception:
            logger.exception("FUTURE_REST_STARTUP_LTP_FAILED | secids=%s", missing)
            return

        if not prices:
            logger.warning("FUTURE_REST_STARTUP_LTP_EMPTY | secids=%s | segment=%s", missing, self.settings.future_exchange_segment)
            return

        used = set()
        for secid, ltp in prices.items():
            if not ltp or float(ltp) <= 0:
                continue
            index = self.future_index_by_secid.get(int(secid))
            if not index:
                continue
            self.pairs[index].update_underlying_quote(
                {
                    "ltp": float(ltp),
                    "secid": int(secid),
                    "tag": f"{index}_FUT",
                    "ts": time.time(),
                    "feature_source": "REST_LTP_STARTUP_FALLBACK",
                }
            )
            used.add(index)
            logger.warning(
                "FUTURE_REST_STARTUP_LTP_USED | index=%s | secid=%s | ltp=%.2f | reason=ws_first_tick_missing",
                index,
                int(secid),
                float(ltp),
            )

        missing_after = [index for index in self.settings.indexes if not self.pairs[index].underlying_ltp]
        if missing_after:
            logger.warning("FUTURE_REST_STARTUP_LTP_MISSING | indexes=%s | used=%s", ",".join(missing_after), ",".join(sorted(used)))

    def seed_missing_underlyings_from_option_chain(self) -> None:
        missing_indexes = [index for index in self.settings.indexes if not self.pairs[index].underlying_ltp]
        if not missing_indexes:
            return

        for index in missing_indexes:
            try:
                data = self.selector.fetch_chain(index) or {}
                ltp = float(data.get("last_price", 0.0) or 0.0)
            except Exception:
                logger.exception("OPTIONCHAIN_STARTUP_LTP_FAILED | index=%s", index)
                continue

            if ltp <= 0:
                logger.warning("OPTIONCHAIN_STARTUP_LTP_EMPTY | index=%s", index)
                continue

            secid = int(self.future_secids.get(index, 0) or 0)
            self.pairs[index].update_underlying_quote(
                {
                    "ltp": ltp,
                    "secid": secid,
                    "tag": f"{index}_FUT",
                    "ts": time.time(),
                    "feature_source": "OPTIONCHAIN_STARTUP_FALLBACK",
                }
            )
            logger.warning(
                "OPTIONCHAIN_STARTUP_LTP_USED | index=%s | secid=%s | ltp=%.2f | reason=future_ws_and_rest_missing",
                index,
                secid,
                ltp,
            )

    def retry_future_ws_startup(self) -> None:
        now = time.time()
        last_retry_time = getattr(self, "_last_retry_time", 0)
        if now - last_retry_time < self.ws_retry_delay:
            return
        if now < self.ws_blocked_until:
            logger.info("WS_BLOCK_ACTIVE | retry paused | remaining=%ss", int(self.ws_blocked_until - now))
            return

        self._last_retry_time = time.time()
        time.sleep(self.ws_retry_delay)
        try:
            if self.future_quote_stream is None:
                return
            subscriptions = [(secid, f"{index}_FUT") for index, secid in self.future_secids.items()]
            self.future_quote_stream.subscribe(subscriptions)
            self._future_ws_subscribed = True
            logger.info("WS_RETRY | subscribe_only | delay=%ss", self.ws_retry_delay)
        except Exception as error:
            self._handle_ws_error(error)

    def wait_for_underlyings(self) -> None:
        logger.info("Waiting for future websocket LTP stream")
        wait_started = time.time()
        while not self._all_underlyings_ready():
            self._future_ready.wait(timeout=self.settings.startup_wait_sec)
            if self._all_underlyings_ready():
                break

            now = time.time()
            elapsed = now - wait_started
            fallback_retry_sec = float(getattr(self, "future_startup_fallback_retry_sec", 15.0))

            if elapsed >= float(getattr(self, "future_startup_rest_fallback_sec", 8.0)):
                if now - float(getattr(self, "_last_startup_rest_fallback_ts", 0.0)) >= fallback_retry_sec:
                    self._last_startup_rest_fallback_ts = now
                    seed_missing_underlyings_from_rest(self)
                    if self._all_underlyings_ready():
                        logger.warning(
                            "FUTURE_WS_STARTUP_REST_FALLBACK_READY | indexes=%s",
                            ",".join(index for index in self.settings.indexes if self.pairs[index].underlying_ltp),
                        )
                        break

            if elapsed >= float(getattr(self, "future_startup_option_chain_fallback_sec", 12.0)):
                if now - float(getattr(self, "_last_startup_chain_fallback_ts", 0.0)) >= fallback_retry_sec:
                    self._last_startup_chain_fallback_ts = now
                    seed_missing_underlyings_from_option_chain(self)
                    if self._all_underlyings_ready():
                        logger.warning(
                            "FUTURE_WS_STARTUP_OPTIONCHAIN_FALLBACK_READY | indexes=%s",
                            ",".join(index for index in self.settings.indexes if self.pairs[index].underlying_ltp),
                        )
                        break

            logger.info("FUTURE_WS_STARTUP_RETRY")
            retry_future_ws_startup(self)
        logger.info("Future websocket LTP stream ready")

    def on_option_full_quote(self, secid: int, tag: str, ltp: float, depth) -> None:
        self._handle_ws_connected()
        with self._lock:
            secid_int = int(secid)
            index = self.option_index_by_secid.get(secid_int)
            if index is None:
                index = str(tag).split("_")[0].upper()
            pair = self.pairs.get(index)
            if pair is None:
                return

            pair.update_option_ltp(secid_int, float(ltp))
            self.option_full_ltp_ts_by_secid[secid_int] = time.time()

            features = dict(getattr(depth, "features", None) or {})
            raw_full = dict(getattr(depth, "raw", None) or {})
            self.latest_full_features_by_secid[secid_int] = dict(features)
            self.latest_full_raw_by_secid[secid_int] = dict(raw_full)

            existing = pair.ce_depth if secid_int == pair.ce_id else pair.pe_depth if secid_int == pair.pe_id else None
            merged = {}
            merged.update(features)
            for key, value in (existing or {}).items():
                merged.setdefault(key, value)
            merged["ltp"] = float(ltp)
            merged["secid"] = secid_int
            merged["tag"] = str(tag)
            merged["ts"] = time.time()
            merged["feature_source"] = "FULL_QUOTE_PRIMARY"
            side = "CE" if secid_int == pair.ce_id else "PE" if secid_int == pair.pe_id else None
            if self.TRI_WAVE_V2_ONLY_MODE and side:
                self.tri_wave_recorder.record_tick(index=index, stream=side, secid=secid_int, ltp=float(ltp), features=dict(merged))
            self._process_option_update(index, pair, secid_int, str(tag), merged)

    def on_option_depth(self, secid: int, tag: str, bid, ask) -> None:
        if not self.market_open():
            return

        raw = self.feature_builder.build(secid, bid, ask)
        if not raw:
            return

        secid_int = int(secid)
        now_ts = time.time()
        self.latest_depth_features_by_secid[secid_int] = dict(raw)

        full = self.latest_full_features_by_secid.get(secid_int, {})
        full_fresh = is_full_ltp_fresh(self, secid_int, now_ts)
        merged = dict(raw)
        for key, value in (full or {}).items():
            if key == "ltp" and not full_fresh:
                continue
            if value is not None:
                merged[key] = value

        merged["secid"] = secid_int
        merged["tag"] = str(tag)
        merged["ts"] = now_ts

        index = self.option_index_by_secid.get(secid_int)
        if index is None:
            index = str(tag).split("_")[0].upper()
        pair = self.pairs.get(index)
        if pair is None:
            return

        with self._lock:
            ws_ltp = pair.ce_ltp if secid_int == pair.ce_id else pair.pe_ltp if secid_int == pair.pe_id else None
            if ws_ltp and ws_ltp > 0 and full_fresh:
                merged["ltp"] = float(ws_ltp)
                merged["feature_source"] = "DEPTH_PLUS_FULL"
            else:
                merged["ltp"] = merged.get("ltp", raw.get("ltp", 0))
                merged["feature_source"] = "DEPTH_WITH_STALE_FULL" if full else "DEPTH_ONLY"
            side = "CE" if secid_int == pair.ce_id else "PE" if secid_int == pair.pe_id else None
            if self.TRI_WAVE_V2_ONLY_MODE and side:
                self.tri_wave_recorder.record_tick(
                    index=index,
                    stream=side,
                    secid=secid_int,
                    ltp=float(merged.get("ltp", 0.0) or 0.0),
                    features=dict(merged),
                )
            pair.update_option_depth(secid_int, merged)
            self._process_option_update(index, pair, secid_int, tag, merged)

    def apply_fresh_ltp(pair, side: str, ltp: float) -> None:
        if side == "CE":
            pair.ce_ltp = float(ltp)
        elif side == "PE":
            pair.pe_ltp = float(ltp)

    def execute_tri_wave_signal(self, pair, signal, raw: dict) -> bool:
        action = getattr(signal, "action", None)
        if action in {"BUY_CE", "EXIT_CE"}:
            ltp = fresh_leg_ltp_or_block(self, pair, "CE", raw.get("ltp", 0), str(action))
            if ltp is None:
                return False
            apply_fresh_ltp(pair, "CE", ltp)
        elif action in {"BUY_PE", "EXIT_PE"}:
            ltp = fresh_leg_ltp_or_block(self, pair, "PE", raw.get("ltp", 0), str(action))
            if ltp is None:
                return False
            apply_fresh_ltp(pair, "PE", ltp)
        elif action == "FLIP_TO_CE":
            exit_ltp = fresh_leg_ltp_or_block(self, pair, "PE", raw.get("ltp", 0), str(action))
            entry_ltp = fresh_leg_ltp_or_block(self, pair, "CE", raw.get("ltp", 0), str(action))
            if exit_ltp is None or entry_ltp is None:
                return False
            apply_fresh_ltp(pair, "PE", exit_ltp)
            apply_fresh_ltp(pair, "CE", entry_ltp)
        elif action == "FLIP_TO_PE":
            exit_ltp = fresh_leg_ltp_or_block(self, pair, "CE", raw.get("ltp", 0), str(action))
            entry_ltp = fresh_leg_ltp_or_block(self, pair, "PE", raw.get("ltp", 0), str(action))
            if exit_ltp is None or entry_ltp is None:
                return False
            apply_fresh_ltp(pair, "CE", exit_ltp)
            apply_fresh_ltp(pair, "PE", entry_ltp)

        return original_execute_tri_wave_signal(self, pair, signal, raw)

    def ws_init(self, *args, **kwargs):
        original_ws_init(self, *args, **kwargs)
        self._last_message_ts = 0.0

    def ws_on_error(self, ws, error) -> None:
        original_ws_on_error(self, ws, error)
        self._connected.clear()
        try:
            ws.close()
        except Exception:
            pass

    def ws_on_close(self, ws, code, message) -> None:
        original_ws_on_close(self, ws, code, message)
        last_age = time.time() - self._last_message_ts if self._last_message_ts else -1.0
        print(f"WS_FULLQUOTE_CLOSED | code={code} | message={message} | last_message_age={last_age:.2f}")

    def ws_on_message(self, ws, message) -> None:
        self._last_message_ts = time.time()
        return original_ws_on_message(self, ws, message)

    TradingRuntimeCoordinator.__init__ = runtime_init
    TradingRuntimeCoordinator._wait_for_underlyings = wait_for_underlyings
    TradingRuntimeCoordinator._seed_missing_underlyings_from_rest = seed_missing_underlyings_from_rest
    TradingRuntimeCoordinator._seed_missing_underlyings_from_option_chain = seed_missing_underlyings_from_option_chain
    TradingRuntimeCoordinator._retry_future_ws_startup = retry_future_ws_startup
    TradingRuntimeCoordinator._on_option_full_quote = on_option_full_quote
    TradingRuntimeCoordinator._is_full_ltp_fresh = is_full_ltp_fresh
    TradingRuntimeCoordinator._fresh_leg_ltp = fresh_leg_ltp
    TradingRuntimeCoordinator._fresh_leg_ltp_or_block = fresh_leg_ltp_or_block
    TradingRuntimeCoordinator._leg_full_age = leg_full_age
    TradingRuntimeCoordinator._leg_depth_age = leg_depth_age
    TradingRuntimeCoordinator.on_option_depth = on_option_depth
    TradingRuntimeCoordinator._execute_tri_wave_signal = execute_tri_wave_signal
    TradingRuntimeCoordinator._ws_safety_profile_installed = True

    DhanLiveMarketFeedWS.__init__ = ws_init
    DhanLiveMarketFeedWS._on_error = ws_on_error
    DhanLiveMarketFeedWS._on_close = ws_on_close
    DhanLiveMarketFeedWS._on_message = ws_on_message
