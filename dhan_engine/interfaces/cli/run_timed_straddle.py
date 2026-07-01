import logging
import os

import requests

from dhan_engine.application.experiments.timed_straddle import (
    TimedStraddleSettings,
    build_timed_straddle_runtime,
)


MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    settings = TimedStraddleSettings.from_env()
    if not os.path.exists(settings.csv_file) or os.path.getsize(settings.csv_file) <= 1024:
        response = requests.get(MASTER_URL, timeout=30)
        response.raise_for_status()
        with open(settings.csv_file, "wb") as handle:
            handle.write(response.content)
    build_timed_straddle_runtime(settings).run()


if __name__ == "__main__":
    main()
