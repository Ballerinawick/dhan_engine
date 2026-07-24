from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass

from dhan_engine.analytics.deeplob_recorder import DepthRecorderSettings, ParquetDepthRecorder

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DeepLobRecorderRuntimeSettings:
    client_id: str
    access_token: str
    csv_file: str
    indexes: tuple[str, ...]
    recorder: DepthRecorderSettings

    @classmethod
    def from_env(cls) -> "DeepLobRecorderRuntimeSettings":
        client_id = os.getenv("DHAN_CLIENT_ID", "").strip()
        token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
        if not client_id or not token:
            raise RuntimeError("Missing DHAN_CLIENT_ID / DHAN_ACCESS_TOKEN")
        raw = os.getenv("DEEPLOB_INDEXES", "NIFTY")
        indexes = tuple(dict.fromkeys(x.strip().upper() for x in raw.split(",") if x.strip()))
        if indexes != ("NIFTY",):
            raise ValueError("The DeepLOB recorder is intentionally restricted to DEEPLOB_INDEXES=NIFTY")
        return cls(
            client_id=client_id,
            access_token=token,
            csv_file=os.getenv("CSV_FILE", "api-scrip-master.csv").strip(),
            indexes=indexes,
            recorder=DepthRecorderSettings.from_env(),
        )


class DeepLobRecorderRuntime:
    def __init__(self, settings, master, depth_adapter, recorder):
        self.settings = settings
        self.master = master
        self.depth_adapter = depth_adapter
        self.recorder = recorder

    def run(self) -> None:
        instruments = []
        for index in self.settings.indexes:
            future = self.master.get_nearest_future(index)
            secid = int(future["security_id"])
            tag = f"{index}_FUT"
            instruments.append(("NSE_FNO", secid, tag))
            logger.info(
                "DEEPLOB_INSTRUMENT_SELECTED | index=%s | symbol=%s | secid=%s",
                index,
                future["symbol"],
                secid,
            )
        self.recorder.start()
        self.depth_adapter.subscribe(instruments)
        logger.info(
            "DEEPLOB_CAPTURE_ACTIVE | indexes=%s | instruments=%s | depth=200 | "
            "connections=%s | orders=false",
            ",".join(self.settings.indexes),
            len(instruments),
            len(instruments),
        )
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("DEEPLOB_CAPTURE_STOPPED")
        finally:
            self.depth_adapter.close()
            self.recorder.close()


def build_deeplob_recorder_runtime(settings: DeepLobRecorderRuntimeSettings):
    from dhan_engine.infrastructure.dhan.full_depth_200_adapter import FullDepth200Adapter
    from dhan_engine.infrastructure.dhan.instrument_master import InstrumentMaster

    master = InstrumentMaster(settings.csv_file, debug=False)
    runtime = None
    recorder = ParquetDepthRecorder(settings.recorder)
    adapter = FullDepth200Adapter(
        settings.client_id,
        settings.access_token,
        lambda tag, book: runtime.recorder.record(tag, book),
    )
    runtime = DeepLobRecorderRuntime(settings, master, adapter, recorder)
    return runtime
