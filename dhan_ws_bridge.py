# dhan_ws_bridge.py
import threading
import time
from typing import Dict, Any, List, Tuple, Optional

from dhanhq import DhanContext, MarketFeed


class DhanWSBridge:
    """
    Thin WebSocket bridge:
    - Subscribes to security_ids
    - Stores latest tick packet per security_id in memory
    - Provides get_latest(security_id) like your old fetch_tick()
    """

    def __init__(self, client_id: str, access_token: str, version: str = "v2"):
        self.ctx = DhanContext(client_id, access_token)
        self.version = version

        self._lock = threading.Lock()
        self._latest: Dict[str, Dict[str, Any]] = {}   # security_id -> latest packet

        self._ws: Optional[MarketFeed] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

        self._subscribed: List[Tuple[int, str, int]] = []

    def connect(self, instruments: List[Tuple[int, str, int]]):
        """
        instruments format:
          (exchange_segment, "security_id", subscription_type)
        Example:
          (MarketFeed.NSE_FNO, "49543", MarketFeed.Full)
        """
        self._subscribed = instruments
        self._ws = MarketFeed(self.ctx, instruments, self.version)

        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        """
        Official pattern is:
          while True:
            data.run_forever()
            response = data.get_data()
        We'll keep draining and saving latest packets.
        """
        assert self._ws is not None

        while self._running:
            try:
                self._ws.run_forever()
                packet = self._ws.get_data()
                # packet format differs by mode, but typically contains security_id
                if not packet:
                    continue

                # Try common keys
                sec_id = None
                if isinstance(packet, dict):
                    sec_id = (
                        packet.get("security_id")
                        or packet.get("securityId")
                        or packet.get("SecurityId")
                        or packet.get("sec_id")
                    )

                if sec_id is None:
                    # Sometimes packet may be nested under "Data"
                    data = packet.get("Data") if isinstance(packet, dict) else None
                    if isinstance(data, dict):
                        sec_id = data.get("security_id") or data.get("SecurityId")

                if sec_id is None:
                    continue

                sec_id = str(sec_id)

                with self._lock:
                    self._latest[sec_id] = packet

            except Exception:
                # Avoid crash loop; reconnect is handled by user restart for now
                time.sleep(0.5)

    def get_latest(self, security_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._latest.get(str(security_id))

    def subscribe(self, instruments: List[Tuple[int, str, int]]):
        if not self._ws:
            return
        self._ws.subscribe_symbols(instruments)

    def unsubscribe(self, instruments: List[Tuple[int, str, int]]):
        if not self._ws:
            return
        self._ws.unsubscribe_symbols(instruments)

    def disconnect(self):
        self._running = False
        if self._ws:
            try:
                self._ws.disconnect()
            except Exception:
                pass
