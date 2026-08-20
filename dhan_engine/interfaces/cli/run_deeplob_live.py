import logging
import os

from dotenv import load_dotenv

from dhan_engine.application.deeplob.live_runtime import (
    DeepLobLiveSettings,
    build_deeplob_live_runtime,
)
from dhan_engine.infrastructure.dhan.master_csv import refresh_master_csv

MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    csv_path = os.getenv("CSV_FILE", "api-scrip-master.csv").strip()
    refresh_master_csv(MASTER_URL, csv_path)
    settings = DeepLobLiveSettings.from_env()
    build_deeplob_live_runtime(settings).run()


if __name__ == "__main__":
    main()

