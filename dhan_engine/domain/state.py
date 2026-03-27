from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class PairRuntimeState:
    index: str
    ce_id: Optional[int] = None
    pe_id: Optional[int] = None
    future_id: Optional[int] = None
    ce_depth: Optional[dict] = None
    pe_depth: Optional[dict] = None
    ce_ltp: Optional[float] = None
    pe_ltp: Optional[float] = None
    underlying_ltp: Optional[float] = None
    underlying_quote: Optional[dict] = None
    ready_logged: bool = False
    last_snapshot: Optional[dict] = None
    last_turn_signal: Optional[dict] = None

    def update_underlying(self, ltp: float) -> None:
        if ltp and ltp > 0:
            self.underlying_ltp = float(ltp)

    def update_underlying_quote(self, payload: dict) -> None:
        if not payload:
            return
        self.underlying_quote = dict(payload)
        ltp = payload.get("ltp")
        if ltp and float(ltp) > 0:
            self.underlying_ltp = float(ltp)

    def update_option_depth(self, secid: int, payload: dict) -> None:
        if secid == self.ce_id:
            self.ce_depth = dict(payload)
        elif secid == self.pe_id:
            self.pe_depth = dict(payload)

    def update_option_ltp(self, secid: int, ltp: float) -> None:
        if not ltp or ltp <= 0:
            return
        if secid == self.ce_id:
            self.ce_ltp = float(ltp)
        elif secid == self.pe_id:
            self.pe_ltp = float(ltp)

    def is_ready(self) -> bool:
        return bool(self.ce_depth and self.pe_depth and self.underlying_ltp)

    def build_market_inputs(self) -> Tuple[Optional[dict], Optional[dict]]:
        return self._merge_leg(self.ce_depth, self.ce_ltp), self._merge_leg(self.pe_depth, self.pe_ltp)

    def route_for_signal(self, signal: str, fallback_tag: str, fallback_ltp: float) -> Tuple[Optional[int], str, float]:
        signal_text = str(signal or "").upper()
        if "BULLISH" in signal_text and self.ce_id:
            return self.ce_id, f"{self.index}_CE", self._best_leg_ltp(self.ce_depth, self.ce_ltp, fallback_ltp)
        if "BEARISH" in signal_text and self.pe_id:
            return self.pe_id, f"{self.index}_PE", self._best_leg_ltp(self.pe_depth, self.pe_ltp, fallback_ltp)
        return None, fallback_tag, float(fallback_ltp)

    @staticmethod
    def _best_leg_ltp(depth_payload: Optional[dict], premium_ltp: Optional[float], fallback_ltp: float) -> float:
        if premium_ltp and premium_ltp > 0:
            return float(premium_ltp)
        if depth_payload and depth_payload.get("ltp"):
            return float(depth_payload["ltp"])
        return float(fallback_ltp)

    @staticmethod
    def _merge_leg(depth_payload: Optional[dict], premium_ltp: Optional[float]) -> Optional[dict]:
        if not depth_payload:
            return None
        payload = dict(depth_payload)
        if premium_ltp and premium_ltp > 0:
            payload["ltp"] = float(premium_ltp)
        return payload


@dataclass(frozen=True)
class ChannelUpdate:
    name: str
    prices: Dict[int, float]
