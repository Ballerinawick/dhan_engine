import logging
import os
import threading
import time
from typing import Dict, List

from dhan_engine.domain.market.full_data_feature_extractor import derive_full_data_features
from dhan_engine.infrastructure.dhan.marketfeed_ws import DhanLiveMarketFeedWS, QuoteDepth


logger = logging.getLogger(__name__)


class FastDhanLiveMarketFeedWS(DhanLiveMarketFeedWS):
    """Fullquote client that keeps websocket ping/pong work lightweight."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._callback_thread = None
        self._message_condition = threading.Condition()
        self._pending_messages: List[bytes] = []
        self._last_message_backlog_log_ts = 0.0
        self._max_pending_messages = int(os.getenv("FULLQUOTE_MAX_PENDING_MESSAGES", "500") or 500)

    def close(self) -> None:
        self._stop.set()
        with self._message_condition:
            self._message_condition.notify_all()
        with self._callback_condition:
            self._callback_condition.notify_all()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    def _ensure_worker(self) -> None:
        if not (self._worker_thread and self._worker_thread.is_alive()):
            self._worker_thread = threading.Thread(
                target=self._message_worker,
                name="DhanMarketFeedMessageWorker",
                daemon=True,
            )
            self._worker_thread.start()
        if not (self._callback_thread and self._callback_thread.is_alive()):
            self._callback_thread = threading.Thread(
                target=self._callback_worker,
                name="DhanMarketFeedCallbackWorker",
                daemon=True,
            )
            self._callback_thread.start()

    def _on_message(self, ws, message) -> None:
        try:
            self._last_message_ts = time.time()
            if not isinstance(message, (bytes, bytearray)):
                if self.debug:
                    print("FULLQUOTE_NON_BINARY_MESSAGE")
                return
            self._enqueue_message(bytes(message))
        except Exception as exc:
            print("FULLQUOTE_WS_MESSAGE_ERROR:", exc)

    def _enqueue_message(self, message: bytes) -> None:
        with self._message_condition:
            self._pending_messages.append(message)
            pending_count = len(self._pending_messages)
            if pending_count > self._max_pending_messages:
                overflow = pending_count - self._max_pending_messages
                del self._pending_messages[:overflow]
                pending_count = len(self._pending_messages)
            self._message_condition.notify()

        now = time.time()
        if pending_count > 100 and now - self._last_message_backlog_log_ts >= 10:
            self._last_message_backlog_log_ts = now
            print(f"FULLQUOTE_MESSAGE_BACKLOG | pending={pending_count}")

    def _message_worker(self) -> None:
        while not self._stop.is_set():
            with self._message_condition:
                if not self._pending_messages:
                    self._message_condition.wait(timeout=1.0)
                if not self._pending_messages:
                    continue
                message = self._pending_messages.pop(0)
            self._process_message(message)

    def _process_message(self, message: bytes) -> None:
        try:
            parsed = self.process_data(message)

            if parsed and self.debug:
                print("FULLQUOTE_PARSED_DATA:", parsed)

            if parsed and parsed.get("type") == "Full Data":
                secid = int(parsed.get("security_id"))
                tag = self._tags.get(secid, str(secid))
                self._last_full_data_ts_by_secid[secid] = time.time()
                previous = self._previous_features.get(secid)
                features = derive_full_data_features(parsed, previous)
                self._previous_features[secid] = features
                ltp = float(features.get("ltp", 0.0) or 0.0)

                depth = parsed.get("depth") or []
                bid_price = [float(item.get("bid_price", 0.0)) for item in depth]
                bid_qty = [int(item.get("bid_quantity", 0)) for item in depth]
                ask_price = [float(item.get("ask_price", 0.0)) for item in depth]
                ask_qty = [int(item.get("ask_quantity", 0)) for item in depth]

                now = time.time()
                if self._feature_log_interval_sec > 0 and now - self._last_feature_log_ts.get(secid, 0) >= self._feature_log_interval_sec:
                    self._last_feature_log_ts[secid] = now
                    logger.info(
                        "FULL_DATA_FEATURES | secid=%s | tag=%s | ltp=%.2f | spread_pct=%.4f | depth_imbalance_5=%.2f | top_depth_imbalance=%.2f | market_queue_imbalance=%.2f | volume_change=%s | oi_change=%s | recovery_score=%.2f | exhaustion_score=%.2f | clean_trade_score=%.2f",
                        secid,
                        tag,
                        ltp,
                        features.get("spread_pct", 0.0),
                        features.get("depth_imbalance_5", 0.0),
                        features.get("top_depth_imbalance", 0.0),
                        features.get("market_queue_imbalance", 0.0),
                        features.get("volume_change_tick", 0),
                        features.get("oi_change_tick", 0),
                        features.get("recovery_score", 0.0),
                        features.get("exhaustion_score", 0.0),
                        features.get("clean_trade_score", 0.0),
                    )

                if self.on_full:
                    self._enqueue_callback(
                        (
                            secid,
                            tag,
                            ltp,
                            QuoteDepth(
                                bid_price=bid_price,
                                bid_qty=bid_qty,
                                ask_price=ask_price,
                                ask_qty=ask_qty,
                                ts=time.time(),
                                raw=parsed,
                                features=features,
                            ),
                        )
                    )
        except Exception as exc:
            print("FULLQUOTE_WS_MESSAGE_ERROR:", exc)
            import traceback

            traceback.print_exc()
