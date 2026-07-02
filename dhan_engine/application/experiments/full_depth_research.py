from __future__ import annotations

import logging
import os
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict

from dhan_engine.domain.market.full_depth_microstructure import (
    CrossInstrumentDepthEngine, InstrumentBookAnalyzer, TradeObservation,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DepthResearchSettings:
    client_id: str
    access_token: str
    csv_file: str = "api-scrip-master.csv"
    index: str = "NIFTY"
    strike_step: int = 50
    horizon_sec: float = 5.0
    round_trip_fee: float = 40.0
    slippage_points: float = 0.5
    min_confidence: float = 0.60
    stale_after_sec: float = 2.0
    log_interval_sec: float = 1.0

    @classmethod
    def from_env(cls):
        client_id, token = os.getenv("DHAN_CLIENT_ID", "").strip(), os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if not client_id or not token:
            raise RuntimeError("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN")
        index = os.getenv("DEPTH_RESEARCH_INDEX", "NIFTY").strip().upper() or "NIFTY"
        return cls(client_id, token, os.getenv("CSV_FILE", "api-scrip-master.csv"), index,
                   int(os.getenv("DEPTH_RESEARCH_STRIKE_STEP", "100" if index == "BANKNIFTY" else "50")),
                   float(os.getenv("DEPTH_RESEARCH_HORIZON_SEC", "5")),
                   float(os.getenv("DEPTH_RESEARCH_ROUND_TRIP_FEE", "40")),
                   float(os.getenv("DEPTH_RESEARCH_SLIPPAGE_POINTS", ".5")),
                   float(os.getenv("DEPTH_RESEARCH_MIN_CONFIDENCE", ".60")),
                   float(os.getenv("DEPTH_RESEARCH_STALE_SEC", "2")),
                   float(os.getenv("DEPTH_RESEARCH_LOG_INTERVAL_SEC", "1")))


class FullDepthResearchRuntime:
    def __init__(self, settings, master, selector, depth_adapter, quote_stream):
        self.settings, self.master, self.selector = settings, master, selector
        self.depth_adapter, self.quote_stream = depth_adapter, quote_stream
        self.analyzers = {x: InstrumentBookAnalyzer() for x in ("FUT", "CE", "PE")}
        self.trades: Dict[str, TradeObservation] = {}
        self.engine = None
        self.last_log = 0.0
        self.first = set()

    @staticmethod
    def _value(obj: Any, *names, default=0):
        for name in names:
            if isinstance(obj, dict) and name in obj:
                return obj[name]
            if hasattr(obj, name):
                return getattr(obj, name)
        return default

    def on_quote(self, secid, tag, ltp, payload):
        leg = str(tag).split("_", 1)[0].upper()
        self.trades[leg] = TradeObservation(float(ltp), int(self._value(payload, "ltq", "LTQ", default=0) or 0),
                                             str(self._value(payload, "ltt", "LTT", default="")))

    def on_book(self, leg, snapshot):
        state = self.analyzers[leg].update(snapshot, self.trades.get(leg))
        if leg not in self.first:
            self.first.add(leg)
            logger.info("FULL_DEPTH_FIRST_SNAPSHOT | leg=%s | secid=%s | bids=%s | asks=%s | receive_ts=%.6f",
                        leg, snapshot.security_id, len(snapshot.bids), len(snapshot.asks), snapshot.received_ts)
        decision = self.engine.update(leg, state, snapshot.received_mono)
        now = time.monotonic()
        if now - self.last_log >= self.settings.log_interval_sec:
            self.last_log = now
            logger.info("FULL_DEPTH_MICRO_STATE | leg=%s | %s", leg, asdict(state))
            logger.info("FULL_DEPTH_CROSS_SIGNAL | %s", asdict(decision))

    def run(self):
        future = self.master.get_nearest_future(self.settings.index)
        pair = self.selector.select_atm_pair(self.settings.index)
        if not pair:
            raise RuntimeError("ATM CE/PE selection failed")
        self.engine = CrossInstrumentDepthEngine(pair["lot_size"], self.settings.round_trip_fee,
            self.settings.slippage_points, self.settings.horizon_sec, self.settings.min_confidence,
            self.settings.stale_after_sec)
        instruments = [("NSE_FNO", int(future["security_id"]), "FUT"),
                       ("NSE_FNO", pair["ce_secid"], "CE"), ("NSE_FNO", pair["pe_secid"], "PE")]
        self.quote_stream.start()
        self.quote_stream.subscribe([(secid, f"{leg}_DEPTH") for _, secid, leg in instruments])
        self.depth_adapter.subscribe(instruments)
        logger.info("FULL_DEPTH_RESEARCH_ACTIVE | index=%s | future=%s | strike=%.0f | connections=4 | paper=true | orders=false",
                    self.settings.index, future["symbol"], pair["strike"])
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("FULL_DEPTH_RESEARCH_STOPPED")
        finally:
            self.depth_adapter.close()
            self.quote_stream.close()


def build_full_depth_research_runtime(settings: DepthResearchSettings):
    from dhan_engine.application.market_data import FutureQuoteStream
    from dhan_engine.infrastructure.dhan.full_depth_200_adapter import FullDepth200Adapter
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster
    from dhan_engine.infrastructure.dhan.option_chain_selector import OptionChainSelector
    master = InstrumentMaster(settings.csv_file, debug=False)
    selector = OptionChainSelector(
        access_token=settings.access_token,
        client_id=settings.client_id,
        instrument_master=master,
        strike_step_map={settings.index: settings.strike_step},
        mode=2,
        debug=False,
    )
    runtime = None
    depth = FullDepth200Adapter(settings.client_id, settings.access_token,
                                lambda leg, book: runtime.on_book(leg, book))
    quotes = FutureQuoteStream(client_id=settings.client_id, token=settings.access_token,
                               exchange_segment="NSE_FNO",
                               on_quote=lambda secid, tag, ltp, payload: runtime.on_quote(secid, tag, ltp, payload),
                               shard_count=1)
    runtime = FullDepthResearchRuntime(settings, master, selector, depth, quotes)
    return runtime
