import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import websocket

from dhan_engine.domain.market.full_data_feature_extractor import derive_full_data_features

try:
    from dhanhq.marketfeed import DhanFeed
except Exception:  # pragma: no cover - optional dependency path
    DhanFeed = None


REQ_FULL = 21
logger = logging.getLogger(__name__)


@dataclass
class QuoteDepth:
    bid_price: List[float]
    bid_qty: List[int]
    ask_price: List[float]
    ask_qty: List[int]
    ts: float
    raw: Optional[dict] = None
    features: Optional[dict] = None


class DhanLiveMarketFeedWS:
    """
    Stable market-feed websocket client for full quote subscriptions.

    The websocket thread must stay light. Dhan closes the connection when
    ping/pong is not handled quickly, so strategy callbacks run on a worker.
    """

    def __init__(
        self,
        token: str,
        client_id: str,
        auth_type: int = 2,
        on_full: Optional[Callable[[int, str, float, QuoteDepth], None]] = None,
        debug: bool = False,
    ):
        self.token = token
        self.client_id = client_id
        self.auth_type = auth_type
        self.on_full = on_full
        self.debug = debug

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._worker_thread: Optional[threading.Thread] = None
        self._callback_thread: Optional[threading.Thread] = None
        self._watchdog_thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._connected = threading.Event()
        self._subs: List[Dict[str, str]] = []
        self._sub_keys = set()
        self._tags: Dict[int, str] = {}
        self._last_subscribe_ts_by_secid: Dict[int, float] = {}
        self._last_full_data_ts_by_secid: Dict[int, float] = {}
        self._last_connected_ts = 0.0
        self._reconnect_attempt = 0
        self._last_manual_reconnect_ts = 0.0
        self._blocked_until_ts = 0.0
        self._lock = threading.Lock()
        self._callback_condition = threading.Condition()
        self._pending_callbacks: Dict[int, tuple] = {}
        self._message_condition = threading.Condition()
        self._pending_messages: List[bytes] = []
        self._last_coalesce_log_ts = 0.0
        self._last_message_backlog_log_ts = 0.0
        self._previous_features: Dict[int, dict] = {}
        self._last_feature_log_ts: Dict[int, float] = {}
        self._feature_log_interval_sec = float(os.getenv("FULLQUOTE_FEATURE_LOG_SEC", "0") or 0)
        self._last_message_ts = 0.0
        self._max_pending_messages = int(os.getenv("FULLQUOTE_MAX_PENDING_MESSAGES", "500") or 500)
        self._stale_reconnect_sec = float(os.getenv("FULLQUOTE_STALE_RECONNECT_SEC", "45") or 45)
        self._subscription_stale_reconnect_sec = float(os.getenv("FULLQUOTE_SUBSCRIPTION_STALE_RECONNECT_SEC", "0") or 0)
        self._subscription_stale_min_count = int(os.getenv("FULLQUOTE_SUBSCRIPTION_STALE_MIN_COUNT", "2") or 2)
        self._manual_reconnect_min_sec = float(os.getenv("FULLQUOTE_MANUAL_RECONNECT_MIN_SEC", "120") or 120)
        self._rate_limit_backoff_sec = float(os.getenv("FULLQUOTE_429_BACKOFF_SEC", "600") or 600)
        self._last_subscription_stale_log_ts = 0.0
        self._ping_interval = float(os.getenv("FULLQUOTE_WS_PING_INTERVAL", "15") or 15)
        self._ping_timeout = float(os.getenv("FULLQUOTE_WS_PING_TIMEOUT", "8") or 8)
        self._feed_parser = (
            DhanFeed(
                client_id=self.client_id,
                access_token=self.token,
                instruments=[],
                version="v2",
            )
            if DhanFeed is not None
            else None
        )

    def connect(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._ensure_worker()
        self._ensure_watchdog()
        self._thread = threading.Thread(target=self._run_loop, name="DhanMarketFeedWS", daemon=True)
        self._thread.start()

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

    def subscribe_full(self, instruments: List[Dict[str, str]]) -> None:
        new_subscriptions: List[Dict[str, str]] = []
        with self._lock:
            for item in instruments:
                key = (str(item["ExchangeSegment"]), str(item["SecurityId"]))
                if key not in self._sub_keys:
                    subscription = dict(item)
                    self._subs.append(subscription)
                    self._sub_keys.add(key)
                    new_subscriptions.append(subscription)
                secid = int(item["SecurityId"])
                self._tags[secid] = item.get("tag", item["SecurityId"])

        if not new_subscriptions:
            if self.debug:
                print("WS_FULLQUOTE_SUBSCRIBE_SKIPPED_DUPLICATE")
            return

        if self._connected.is_set():
            self._send_subscribe(new_subscriptions)

    def reconnect(self, reason: str = "manual") -> None:
        now = time.time()
        if now < self._blocked_until_ts:
            remaining = self._blocked_until_ts - now
            print(f"FULLQUOTE_FORCE_RECONNECT_SKIPPED | reason={reason} | blocked_for={remaining:.0f}s")
            return
        if now - self._last_manual_reconnect_ts < self._manual_reconnect_min_sec:
            remaining = self._manual_reconnect_min_sec - (now - self._last_manual_reconnect_ts)
            print(f"FULLQUOTE_FORCE_RECONNECT_SKIPPED | reason={reason} | cooldown={remaining:.0f}s")
            return
        self._last_manual_reconnect_ts = now
        print(f"FULLQUOTE_FORCE_RECONNECT | reason={reason}")
        self._connected.clear()
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

    def _ensure_watchdog(self) -> None:
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            return
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            name="DhanMarketFeedWatchdog",
            daemon=True,
        )
        self._watchdog_thread.start()

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            blocked_for = self._blocked_until_ts - time.time()
            if blocked_for > 0:
                wait = min(blocked_for, 30.0)
                print(f"FULLQUOTE_429_BACKOFF_WAIT | sec={wait:.0f}")
                time.sleep(wait)
                continue

            self._connected.clear()

            url = (
                f"wss://api-feed.dhan.co"
                f"?version=2"
                f"&token={self.token}"
                f"&clientId={self.client_id}"
                f"&authType={self.auth_type}"
            )

            if self.debug:
                print("WS_FULLQUOTE_CONNECT", url)

            self._ws = websocket.WebSocketApp(
                url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            try:
                self._ws.run_forever(
                    ping_interval=self._ping_interval,
                    ping_timeout=self._ping_timeout,
                )
            except Exception as exc:
                print(f"FULLQUOTE_WS_EXCEPTION | error={exc}")

            if self._stop.is_set():
                break

            self._reconnect_attempt += 1
            blocked_for = self._blocked_until_ts - time.time()
            wait = min(30, 2 ** min(self._reconnect_attempt, 4))
            if blocked_for > 0:
                wait = min(max(wait, blocked_for), 30)
            print(f"FULLQUOTE_WS_RECONNECT_WAIT | sec={wait}")
            time.sleep(wait)

    def _send_subscribe(self, subscriptions: Optional[List[Dict[str, str]]] = None) -> None:
        if not self._ws:
            return

        with self._lock:
            subscriptions = list(subscriptions if subscriptions is not None else self._subs)
            now = time.time()
            for item in subscriptions:
                self._last_subscribe_ts_by_secid[int(item["SecurityId"])] = now

        if not subscriptions:
            return

        payload = {
            "RequestCode": REQ_FULL,
            "InstrumentCount": len(subscriptions),
            "InstrumentList": [
                {
                    "ExchangeSegment": item["ExchangeSegment"],
                    "SecurityId": item["SecurityId"],
                }
                for item in subscriptions
            ],
        }

        if self.debug:
            print("WS_FULLQUOTE_SUB", payload)

        try:
            print("WS_FULLQUOTE_SUBSCRIBE | count=%s" % payload["InstrumentCount"])
            self._ws.send(json.dumps(payload))
        except Exception as exc:
            print(f"FULLQUOTE_WS_SUBSCRIBE_ERROR | error={exc}")

    def _on_open(self, ws) -> None:
        self._connected.set()
        self._last_connected_ts = time.time()
        self._reconnect_attempt = 0
        print("WS_FULLQUOTE_CONNECTED")
        self._send_subscribe()

    def _on_error(self, ws, error) -> None:
        print(f"FULLQUOTE_WS_ERROR | error={error}")
        self._mark_rate_limited_if_needed(error)
        self._connected.clear()
        try:
            ws.close()
        except Exception:
            pass

    def _on_close(self, ws, code, message) -> None:
        self._connected.clear()
        self._mark_rate_limited_if_needed(message)
        last_age = time.time() - self._last_message_ts if self._last_message_ts else -1.0
        print(f"WS_FULLQUOTE_CLOSED | code={code} | message={message} | last_message_age={last_age:.2f}")

    def _mark_rate_limited_if_needed(self, error) -> None:
        text = str(error or "")
        if "429" not in text and "Too many requests" not in text and "client id is blocked" not in text:
            return
        blocked_until = time.time() + self._rate_limit_backoff_sec
        if blocked_until <= self._blocked_until_ts:
            return
        self._blocked_until_ts = blocked_until
        self._connected.clear()
        print(
            "FULLQUOTE_429_BACKOFF_ACTIVE | "
            f"backoff_sec={self._rate_limit_backoff_sec:.0f} | error={text[:180]}"
        )

    def process_data(self, data: bytes):
        if self._feed_parser is None:
            raise RuntimeError("dhanhq is not installed. Run: pip install dhanhq")
        return self._feed_parser.process_data(data)

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
            parsed = self.process_data(bytes(message))

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

    def _enqueue_callback(self, item: tuple) -> None:
        secid = int(item[0])
        with self._callback_condition:
            self._pending_callbacks[secid] = item
            pending_count = len(self._pending_callbacks)
            self._callback_condition.notify()
        now = time.time()
        if pending_count > 25 and now - self._last_coalesce_log_ts >= 10:
            self._last_coalesce_log_ts = now
            print(f"FULLQUOTE_CALLBACK_COALESCED | pending={pending_count}")

    def _callback_worker(self) -> None:
        while not self._stop.is_set():
            with self._callback_condition:
                if not self._pending_callbacks:
                    self._callback_condition.wait(timeout=1.0)
                if not self._pending_callbacks:
                    continue
                _, item = self._pending_callbacks.popitem()
            secid, tag, ltp, depth = item
            try:
                if self.on_full:
                    self.on_full(secid, tag, ltp, depth)
            except Exception:
                logger.exception("FULLQUOTE_CALLBACK_ERROR | secid=%s | tag=%s", secid, tag)

    def _watchdog_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(5.0)
            if not self._connected.is_set():
                continue
            last_age = time.time() - self._last_message_ts if self._last_message_ts else 0.0
            if self._stale_reconnect_sec > 0 and last_age > self._stale_reconnect_sec:
                print(f"FULLQUOTE_STALE_RECONNECT | last_message_age={last_age:.2f}")
                try:
                    if self._ws:
                        self._ws.close()
                except Exception:
                    pass
                continue
            self._check_subscription_staleness()

    def _check_subscription_staleness(self) -> None:
        if self._subscription_stale_reconnect_sec <= 0:
            return
        now = time.time()
        with self._lock:
            subscriptions = list(self._subs)
            last_subscribe = dict(self._last_subscribe_ts_by_secid)
            last_full = dict(self._last_full_data_ts_by_secid)
            tags = dict(self._tags)
        if not subscriptions:
            return

        stale = []
        for item in subscriptions:
            secid = int(item["SecurityId"])
            subscribed_age = now - float(last_subscribe.get(secid, self._last_connected_ts or now))
            if subscribed_age < self._subscription_stale_reconnect_sec:
                continue
            full_ts = float(last_full.get(secid, 0.0) or 0.0)
            full_age = now - full_ts if full_ts else subscribed_age
            if full_age >= self._subscription_stale_reconnect_sec:
                stale.append((secid, tags.get(secid, str(secid)), full_age))

        if len(stale) < min(self._subscription_stale_min_count, len(subscriptions)):
            return
        if now - self._last_subscription_stale_log_ts < self._subscription_stale_reconnect_sec:
            return
        self._last_subscription_stale_log_ts = now
        stale_preview = ",".join(f"{tag}:{age:.0f}s" for _, tag, age in stale[:8])
        last_message_age = now - self._last_message_ts if self._last_message_ts else None
        if last_message_age is not None and last_message_age < self._subscription_stale_reconnect_sec:
            print(
                "FULLQUOTE_SUBSCRIPTION_STALE_OBSERVED | "
                f"stale={len(stale)}/{len(subscriptions)} | socket_live_age={last_message_age:.1f}s | "
                f"threshold={self._subscription_stale_reconnect_sec:.0f}s | {stale_preview}"
            )
            return
        print(
            "FULLQUOTE_SUBSCRIPTION_STALE_RECONNECT | "
            f"stale={len(stale)}/{len(subscriptions)} | threshold={self._subscription_stale_reconnect_sec:.0f}s | {stale_preview}"
        )
        self.reconnect("subscription_stale")
