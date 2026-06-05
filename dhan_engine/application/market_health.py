from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class PairFreshness:
    ce_age: Optional[float]
    pe_age: Optional[float]
    max_age_sec: float

    @property
    def is_fresh(self) -> bool:
        if self.ce_age is None or self.pe_age is None:
            return False
        return self.ce_age <= self.max_age_sec and self.pe_age <= self.max_age_sec


def age_from_ts(ts: float, now_ts: float) -> Optional[float]:
    ts = float(ts or 0.0)
    return now_ts - ts if ts > 0 else None


def format_age(value: Optional[float]) -> str:
    return "missing" if value is None else f"{value:.1f}s"

