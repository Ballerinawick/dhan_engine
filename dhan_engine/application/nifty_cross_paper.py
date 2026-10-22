"""Opt-in NIFTY-only cross-book market adapter; no order execution imports."""

from __future__ import annotations

import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

import requests

from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
from dhan_engine.infrastructure.dhan.ltp_rest_engine import DhanLtpRestEngine
from dhan_engine.simulations.nifty_cross_book import Close, Pair, centered_strikes, ist
from dhan_engine.simulations.nifty_cross_session import CrossSession
from dhan_engine.simulations.nifty_cross_book import Book


BASE_URL = "https://api.dhan.co/v2"
logger = logging.getLogger(__name__)


class NiftyCrossPaper:
    def __init__(self, *, client_id: str, access_token: str, master_csv: str, state_path: str):
        self.master = InstrumentMaster(master_csv, debug=False)
        self.ltp = DhanLtpRestEngine(access_token, client_id)
        self.http = requests.Session()
        self.http.headers.update({"access-token": access_token, "client-id": client_id,
                                  "Content-Type": "application/json", "Accept": "application/json"})
        self.state_path = Path(state_path)
        self.session: CrossSession | None = None
        self.future_id: int | None = None
        self._chain_at = 0.0
        self._last_chart_at = 0.0
        self._index_price: float | None = None

    def _post(self, endpoint: str, payload: dict) -> dict:
        response = self.http.post(BASE_URL + endpoint, json=payload, timeout=12)
        response.raise_for_status()
        body = response.json()
        if body.get("status") == "failure":
            raise RuntimeError(f"Dhan {endpoint} returned failure: {body.get('remarks')}")
        return body

    def _chart(self, security_id: int, instrument: str, now: datetime) -> list[Close]:
        wait = .2 - (time.monotonic() - self._last_chart_at)
        if wait > 0:
            time.sleep(wait)
        self._last_chart_at = time.monotonic()
        today = ist(now).date().isoformat()
        body = self._post("/charts/intraday", {
            "securityId": str(security_id), "exchangeSegment": "NSE_FNO",
            "instrument": instrument, "interval": "1", "oi": False,
            "fromDate": f"{today} 09:15:00", "toDate": f"{today} 15:30:00",
        })
        timestamps, prices = body.get("timestamp", []), body.get("close", [])
        return [Close(datetime.fromtimestamp(int(ts), tz=ist(now).tzinfo), float(price))
                for ts, price in zip(timestamps, prices)]

    def refresh_contracts(self, now: datetime, *, force: bool = False) -> None:
        now = ist(now)
        if not force and self.session and time.monotonic() - self._chain_at < 120:
            return
        future = self.master.get_nearest_future("NIFTY")
        future_id = int(future["security_id"])
        expiries = self._post("/optionchain/expirylist", {"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I"})["data"]
        expiry = min(date for date in expiries if date >= now.date().isoformat())
        chain = self._post("/optionchain", {"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I", "Expiry": expiry})["data"]
        future_bars = self._chart(future_id, "FUTIDX", now)
        finished = [bar for bar in future_bars if bar.t < now.replace(second=0, microsecond=0)]
        index_price = float(chain.get("last_price", 0) or 0)
        anchor = finished[-1].price if finished else index_price
        if anchor <= 0:
            raise ValueError("No finished future close or index price for strike anchor")
        oc = chain["oc"]
        strikes = centered_strikes(anchor, [float(key) for key, value in oc.items()
                                            if value.get("ce") and value.get("pe")])
        pairs = []
        for strike in strikes:
            node = next(value for key, value in oc.items() if float(key) == strike)
            pairs.append(Pair(strike, int(node["ce"]["security_id"]), int(node["pe"]["security_id"])))
        if self.session is None or self.session.expiry != expiry:
            self.session = CrossSession(self.state_path, expiry, pairs)
            self.session.load(now)
        else:
            self.session.pairs = pairs
        self.future_id, self._index_price = future_id, index_price
        self.session.book.minutes[future_id] = future_bars[-400:]
        for pair in pairs:
            for security_id in (pair.call_id, pair.put_id):
                history = self._chart(security_id, "OPTIDX", now)
                if history:
                    self.session.book.minutes[security_id] = history[-400:]
        self._chain_at = time.monotonic()

    def step(self, now: datetime) -> None:
        now = ist(now)
        if now.weekday() >= 5 or not (9, 15) <= (now.hour, now.minute) <= (15, 30):
            return
        new_day = self.session is not None and self.session.book.day != now.date().isoformat()
        if new_day:
            self.session.book = Book(day=now.date().isoformat(), expiry=self.session.expiry)
        self.refresh_contracts(now, force=new_day)
        assert self.session is not None and self.future_id is not None
        ids = {self.future_id, *(sid for pair in self.session.pairs
                                 for sid in (pair.call_id, pair.put_id))}
        if self.session.book.open:
            ids.update((self.session.book.open.call_id, self.session.book.open.put_id))
        segments = {"NSE_FNO": sorted(ids)}
        prices = self.ltp.fetch_ltp_map(segments) or {}
        for security_id, price in prices.items():
            self.session.tick(security_id, price, now)
        future = prices.get(self.future_id)
        if future is not None:
            self.session.step(future, now)
        self.session.save_if_due()

    def run(self) -> None:
        try:
            while True:
                now = datetime.now().astimezone()
                try:
                    self.step(now)
                except (requests.RequestException, RuntimeError, KeyError, ValueError):
                    logger.exception("NIFTY_CROSS_PAPER_DATA_ERROR | retrying without orders")
                    time.sleep(3.2)
                time.sleep(max(0, 1.0 - now.microsecond / 1_000_000))
        finally:
            if self.session:
                self.session.save_if_due(force=True)

