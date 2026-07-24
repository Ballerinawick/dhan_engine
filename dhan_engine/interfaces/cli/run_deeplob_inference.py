import logging
import os

import requests
from dotenv import load_dotenv

from dhan_engine.application.deeplob.inference_runtime import (
    DeepLobInferenceSettings,
    build_deeplob_inference_runtime,
)

MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    settings = DeepLobInferenceSettings.from_env()
    if not os.path.exists(settings.csv_file) or os.path.getsize(settings.csv_file) <= 1024:
        response = requests.get(MASTER_URL, timeout=30)
        response.raise_for_status()
        with open(settings.csv_file, "wb") as handle:
            handle.write(response.content)
    build_deeplob_inference_runtime(settings).run()


if __name__ == "__main__":
    main()
