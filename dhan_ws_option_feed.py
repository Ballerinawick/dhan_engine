# dhan_ws_option_feed.py
import threading
import time
import asyncio
from typing import Dict, Any, List, Tuple, Optional

from dhanhq.marketfeed import DhanFeed

# Dhan constants
NSE_FNO = 2
Ticker = 15
Quote = 17
Depth = 19
Full = 21


class DhanOptionWS:
    """
    WebSocket wrapper for CE / PE only
    - Uses DhanFeed official SDK
    - Uses version="v2" (token/clientId in URL) to avoid HTTP 400
    - Creates & owns event loop inside this WS thread
    - Closes disconnect coroutine correctly inside the same loop
    """

    def __init__(self, client_id: str, access_token: str):
        self.client_id = str(client_id)
        self.access_token = str(access_token)

        self._lock = threading.Lock()
        self._latest: Dict[str, Dict[str, Any]] = {}

        self._thread: Optional[threading.Thread] = None
        self._stop_flag = False
        self._feed: Optional[DhanFeed] = None

        self._active_instruments: List[Tuple[int, int, int]] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _on_ticks(self, msg: Any):
        if not msg:
            return
        ticks = msg if isinstance(msg, list) else [msg]

        with self._lock:
            for t in ticks:
                sid = str(
                    t.get("security_id")
                    or t.get("securityId")
                    or t.get("sec_id")
                    or ""
                )
                if sid:
                    self._latest[sid] = t

    def _runner(self):
        # ✅ event loop MUST be created inside the thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop

        while not self._stop_flag:
            try:
                self._feed = DhanFeed(
                    client_id=self.client_id,
                    access_token=self.access_token,
                    instruments=self._active_instruments,
                    version="v2",   # ✅ IMPORTANT: v2 avoids HTTP 400
                )
                self._feed.on_ticks = self._on_ticks

                # blocks here until disconnected / error
                self._feed.run_forever()

            except Exception as e:
                if self._stop_flag:
                    break
                print(f"❌ WS ERROR: {type(e).__name__}: {e}")
                time.sleep(1.5)

        # cleanup loop
        try:
            loop.stop()
        except Exception:
            pass

    def start(self, instruments: List[Tuple[int, int, int]]):
        self._active_instruments = instruments[:]
        self._stop_flag = False

        self._thread = threading.Thread(
            target=self._runner,
            name="DhanOptionWS",
            daemon=True,
        )
        self._thread.start()

    def restart_with(self, instruments: List[Tuple[int, int, int]]):
        self.stop()
        time.sleep(0.4)
        self.start(instruments)

    def stop(self):
        self._stop_flag = True

        # ✅ Proper async disconnect inside the WS thread loop
        try:
            if self._loop and self._feed and hasattr(self._feed, "disconnect"):
                async def _do_disconnect():
                    try:
                        await self._feed.disconnect()
                    except Exception:
                        pass

                self._loop.call_soon_threadsafe(lambda: asyncio.create_task(_do_disconnect()))
        except Exception:
            pass

    def get_latest(self, security_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._latest.get(str(security_id))
