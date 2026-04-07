import os
from dataclasses import dataclass, field
from datetime import time as dtime
from typing import Dict, Tuple

import pytz
from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class BrokerCredentials:
    client_id: str
    access_token: str


@dataclass(frozen=True)
class RuntimeSettings:
    credentials: BrokerCredentials
    csv_file: str = "api-scrip-master.csv"
    indexes: Tuple[str, ...] = ("NIFTY",)
    strike_step: Dict[str, int] = field(
        default_factory=lambda: {
            "NIFTY": 50,
            "BANKNIFTY": 100,
            "FINNIFTY": 50,
        }
    )
    option_exchange_segment: str = "NSE_FNO"
    future_exchange_segment: str = "NSE_FNO"
    full_quote_request_code: int = 21
    full_quote_segment: int = 2
    full_quote_log_sec: int = 10
    heartbeat_sec: float = 30.0
    selector_mode: int = 2
    selector_steps_each_side: int = 10
    capital: float = 100000.0
    ltp_poll_sec: float = 1.05
    startup_wait_sec: float = 1.0
    option_premium_stream_enabled: bool = False
    full_quote_sampler_enabled: bool = True
    future_quote_stream_debug: bool = False
    timezone_name: str = "Asia/Kolkata"
    market_start: dtime = dtime(9, 10)
    market_end: dtime = dtime(15, 35)
    master_url: str = "https://images.dhan.co/api-data/api-scrip-master.csv"

    @property
    def timezone(self):
        return pytz.timezone("Asia/Kolkata")


def _csv_file() -> str:
    return os.getenv("CSV_FILE", "api-scrip-master.csv").strip() or "api-scrip-master.csv"


def _indexes() -> Tuple[str, ...]:
    raw = os.getenv("INDEXES", "NIFTY")
    indexes = tuple(part.strip().upper() for part in raw.split(",") if part.strip())
    return indexes or ("NIFTY",)


def load_settings() -> RuntimeSettings:
    access_token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
    client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
    if not access_token or not client_id:
        raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID")

    return RuntimeSettings(
        credentials=BrokerCredentials(client_id=client_id, access_token=access_token),
        csv_file=_csv_file(),
        indexes=_indexes(),
        full_quote_sampler_enabled=os.getenv("ENABLE_FULL_QUOTE_SAMPLER", "1").strip() != "0",
        option_premium_stream_enabled=os.getenv("ENABLE_OPTION_PREMIUM_STREAM", "0").strip() != "0",
    )
