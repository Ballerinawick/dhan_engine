import logging
import os

from dotenv import load_dotenv

from dhan_engine.application.stocks.option_paper_runtime import (
    StockOptionPaperSettings,
    build_stock_option_paper_runtime,
)
from dhan_engine.infrastructure.dhan.master_csv import refresh_master_csv


MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def main() -> None:
    load_dotenv()
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    csv_path = os.getenv(
        "CSV_FILE", "/var/lib/dhan-engine-stock-options/api-scrip-master.csv"
    ).strip()
    refresh_master_csv(MASTER_URL, csv_path)
    settings = StockOptionPaperSettings.from_env()
    build_stock_option_paper_runtime(settings).run()


if __name__ == "__main__":
    main()
