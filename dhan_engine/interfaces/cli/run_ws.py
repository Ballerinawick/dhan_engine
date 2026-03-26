import logging

from dhan_engine.application.runtime import build_runtime
from dhan_engine.config.settings import load_settings


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def main() -> None:
    configure_logging()
    settings = load_settings()
    runtime = build_runtime(settings)
    runtime.run()


if __name__ == "__main__":
    main()

