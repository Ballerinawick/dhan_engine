import asyncio
import threading
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, Optional, Tuple

from dhan_engine.domain.market.full_depth_microstructure import BookSnapshot
from dhan_engine.infrastructure.dhan.full_depth import FullDepth


@dataclass
class _Pair:
    bid: Optional[dict] = None
    ask: Optional[dict] = None


class FullDepth200Adapter:
    """One Dhan websocket per instrument, as required by the 200-depth API."""

    def __init__(self, client_id: str, token: str,
                 on_book: Callable[[str, BookSnapshot], None]):
        self.client_id, self.token, self.on_book = str(client_id), str(token), on_book
        self._feeds: Dict[str, FullDepth] = {}
        self._threads = []

    def subscribe(self, instruments: Iterable[Tuple[str, int, str]]) -> None:
        items = list(instruments)
        if len(items) > 5:
            raise ValueError("Dhan permits at most five websocket connections")
        for segment, secid, tag in items:
            if tag in self._feeds:
                continue
            feed = FullDepth(self.client_id, self.token, levels=200)
            self._feeds[tag] = feed
            thread = threading.Thread(
                target=lambda f=feed, s=segment, i=int(secid), t=tag: asyncio.run(self._run(f, s, i, t)),
                name=f"Depth200-{tag}", daemon=True,
            )
            self._threads.append(thread)
            thread.start()

    async def _run(self, feed: FullDepth, segment: str, secid: int, tag: str) -> None:
        pair = _Pair()
        await feed.subscribe_async([(segment, str(secid))])
        async for packet in feed.get_instrument_data():
            if not isinstance(packet, dict):
                continue
            if packet.get("msg_code") == 41:
                pair.bid = packet
            elif packet.get("msg_code") == 51:
                pair.ask = packet
            if not pair.bid or not pair.ask:
                continue
            received_ts = max(float(pair.bid.get("received_ts", 0)), float(pair.ask.get("received_ts", 0)))
            received_mono = max(float(pair.bid.get("received_mono", 0)), float(pair.ask.get("received_mono", 0)))
            rows = lambda p: [(x["price"], x["qty"], x["orders"]) for x in p["levels"]]
            snapshot = BookSnapshot.build(secid, tag, rows(pair.bid), rows(pair.ask), received_ts, received_mono)
            self.on_book(tag, snapshot)

    def close(self) -> None:
        for feed in self._feeds.values():
            try:
                asyncio.run(feed.disconnect())
            except Exception:
                pass
