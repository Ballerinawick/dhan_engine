import logging
import os

from dotenv import load_dotenv

from dhan_engine.application.deeplob.inference_runtime import (
    DeepLobInferenceSettings,
    build_deeplob_inference_runtime,
)
from dhan_engine.infrastructure.dhan.master_csv import refresh_master_csv

MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    settings = DeepLobInferenceSettings.from_env()
    refresh_master_csv(MASTER_URL, settings.csv_file)
    build_deeplob_inference_runtime(settings).run()


if __name__ == "__main__":
    main()
