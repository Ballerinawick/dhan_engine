"""Run the isolated NIFTY cross paper book explicitly."""

import os

from dhan_engine.application.nifty_cross_paper import NiftyCrossPaper
from dhan_engine.infrastructure.dhan.master_csv import refresh_master_csv


MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def main() -> None:
    master_csv = os.getenv("DHAN_MASTER_CSV", os.getenv("CSV_FILE", "data/api-scrip-master.csv"))
    refresh_master_csv(MASTER_URL, master_csv)
    runner = NiftyCrossPaper(
        client_id=os.environ["DHAN_CLIENT_ID"],
        access_token=os.environ["DHAN_ACCESS_TOKEN"],
        master_csv=master_csv,
        state_path=os.getenv("NIFTY_CROSS_STATE_PATH", "/var/lib/dhan-engine/nifty-cross-paper.json"),
    )
    runner.run()


if __name__ == "__main__":
    main()

