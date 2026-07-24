import logging

from dhan_engine.application.deeplob.recorder_runtime import (
    DeepLobRecorderRuntimeSettings,
    build_deeplob_recorder_runtime,
)
from dhan_engine.infrastructure.dhan.master_csv import refresh_master_csv

MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    settings = DeepLobRecorderRuntimeSettings.from_env()
    refresh_master_csv(MASTER_URL, settings.csv_file)
    build_deeplob_recorder_runtime(settings).run()


if __name__ == "__main__":
    main()
