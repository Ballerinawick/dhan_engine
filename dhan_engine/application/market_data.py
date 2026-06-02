import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from dhan_engine.infrastructure.dhan.async_depth_adapter import DhanAsyncDepthAdapter
from dhan_engine.infrastructure.dhan.ltp_rest_engine import DhanLtpRestEngine
from dhan_engine.infrastructure.dhan.marketfeed_ws import DhanLiveMarketFeedWS


logger = logging.getLogger(__name__)


@dataclass
class LtpChannel:
    name: str
    callback: Callable[[Dict[int, float]], None]
    segment_to_secids: Dict[str, Set[int]] = field(default_factory=dict)


class RestLtpStreamer:
    """Single rate-limited REST LTP transport with logical channels."""

    def __init__(self, client: DhanLtpRestEngine, poll_interval_sec: float = 1.05):
        self.client = client
        self.poll_interval_sec = max(float(poll_interval_sec), 1.0)
        self._channels: Dict[str, LtpChannel] = {}
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def register_channel(self, name: str, callback: Callable[[Dict[int, float]], None]) -> None:
        with self._lock:
            self._channels[name] = LtpChannel(name=name, callback=callback)

    def update_subscription(self, name: str, segment_to_secids: Dict[str, Iterable[int]]) -> None:
        with self._lock:
            if name not in self._channels:
                raise KeyError(f"Channel not registered: {name}")
            normalized: Dict[str, Set[int]] = {}
            for segment, secids in (segment_to_secids or {}).items():
                cleaned = {int(secid) for secid in secids}
                if cleaned:
                    normalized[segment] = cleaned
            self._channels[name].segment_to_secids = normalized

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="RestLtpStreamer", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            payload, channel_membership = self._snapshot_subscriptions()
            if payload:
                prices = self.client.fetch_ltp_map(payload) or {}
                if prices:
                    self._dispatch(prices, channel_membership)
            time.sleep(self.poll_interval_sec)

    def _snapshot_subscriptions(self) -> Tuple[Dict[str, List[int]], Dict[str, Set[int]]]:
        payload: Dict[str, Set[int]] = {}
        membership: Dict[str, Set[int]] = {}
        with self._lock:
            channels = list(self._channels.values())

        for channel in channels:
            member_set: Set[int] = set()
            for segment, secids in channel.segment_to_secids.items():
                payload.setdefault(segment, set()).update(secids)
                member_set.update(secids)
            membership[channel.name] = member_set

        normalized_payload = {segment: sorted(secids) for segment, secids in payload.items() if secids}
        return normalized_payload, membership

    def _dispatch(self, prices: Dict[int, float], channel_membership: Dict[str, Set[int]]) -> None:
        with self._lock:
            channels = list(self._channels.values())

        for channel in channels:
            members = channel_membership.get(channel.name, set())
            if not members:
                continue
            channel_prices = {secid: price for secid, price in prices.items() if secid in members}
            if not channel_prices:
                continue
            try:
                channel.callback(channel_prices)
            except Exception:
                logger.exception("LTP channel callback failed | channel=%s", channel.name)


class OptionDepthStream:
    """Dedicated option depth stream transport."""

    def __init__(
        self,
        *,
        client_id: str,
        token: str,
        exchange_segment: str,
        on_depth: Callable[[int, str, object, object], None],
    ):
        self._adapter = DhanAsyncDepthAdapter(
            client_id=client_id,
            token=token,
            exchange_segment=exchange_segment,
            on_depth=on_depth,
        )

    def start(self) -> None:
        self._adapter.start()

    def subscribe(self, subscriptions: Iterable[Tuple[int, str]]) -> None:
        instruments = [
            (self._adapter.exchange_segment, str(secid), tag)
            for secid, tag in subscriptions
        ]
        self._adapter.subscribe(instruments)


class FutureQuoteStream:
    """Fullquote stream transport.

    FUT subscriptions use one websocket. Option premium subscriptions are
    sharded by symbol so a busy premium stream cannot stall every profile.
    """

    def __init__(
        self,
        *,
        client_id: str,
        token: str,
        exchange_segment: str,
        on_quote: Callable[[int, str, float, object], None],
        debug: bool = False,
    ):
        self.exchange_segment = exchange_segment
        self._token = token
        self._client_id = client_id
        self._on_quote = on_quote
        self._debug = debug
        self._started = False
        self._client: Optional[DhanLiveMarketFeedWS] = None
        self._clients: List[DhanLiveMarketFeedWS] = []
        self._option_shard_count = max(1, min(int(os.getenv("OPTION_QUOTE_WS_SHARDS", "3") or 3), 4))

    def start(self) -> None:
        self._started = True
        if self._client is not None:
            self._client.connect()
        for client in self._clients:
            client.connect()

    def subscribe(self, subscriptions: Iterable[Tuple[int, str]]) -> None:
        subscription_list = list(subscriptions)
        instruments = [
            {
                "ExchangeSegment": self.exchange_segment,
                "SecurityId": str(secid),
                "tag": tag,
            }
            for secid, tag in subscription_list
        ]
        if not instruments:
            return

        is_option_stream = any(not str(tag).upper().endswith("_FUT") for _, tag in subscription_list)
        if is_option_stream and self._option_shard_count > 1:
            self._ensure_shards()
            by_shard: Dict[int, List[Dict[str, str]]] = {idx: [] for idx in range(self._option_shard_count)}
            for item in instruments:
                shard_idx = self._shard_for_tag(str(item.get("tag", "")))
                by_shard[shard_idx].append(item)
            for shard_idx, shard_instruments in by_shard.items():
                if shard_instruments:
                    self._clients[shard_idx].subscribe_full(shard_instruments)
            return

        self._ensure_single_client()
        self._client.subscribe_full(instruments)

    def _make_client(self) -> DhanLiveMarketFeedWS:
        client = DhanLiveMarketFeedWS(
            token=self._token,
            client_id=self._client_id,
            on_full=self._on_quote,
            debug=self._debug,
        )
        if self._started:
            client.connect()
        return client

    def _ensure_single_client(self) -> None:
        if self._client is None:
            self._client = self._make_client()

    def _ensure_shards(self) -> None:
        while len(self._clients) < self._option_shard_count:
            self._clients.append(self._make_client())

    def _shard_for_tag(self, tag: str) -> int:
        symbol = tag.split("_", 1)[0].upper().strip() or tag
        return sum(ord(ch) for ch in symbol) % self._option_shard_count
