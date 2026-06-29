import logging
import os

import requests

from dhan_engine.application.commodities.paper_runtime import (
    CommodityPaperSettings,
    build_commodity_paper_runtime,
)


MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _ensure_master(path: str) -> None:
    if os.path.exists(path) and os.path.getsize(path) > 1024:
        return
    response = requests.get(MASTER_URL, timeout=30)
    response.raise_for_status()
    with open(path, "wb") as handle:
        handle.write(response.content)


def main() -> None:
    configure_logging()
    settings = CommodityPaperSettings.from_env()
    _ensure_master(settings.csv_file)
    build_commodity_paper_runtime(settings).run()


if __name__ == "__main__":
    main()
