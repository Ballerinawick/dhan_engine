from __future__ import annotations

import logging
import os
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


def refresh_master_csv(
    url: str,
    destination: str,
    *,
    timeout_sec: float = 30.0,
    minimum_bytes: int = 1024,
) -> bool:
    """Atomically refresh the Dhan instrument master.

    A valid existing file is retained when the network refresh fails. This
    prevents a partial download from replacing the last usable master.
    """
    target = Path(destination)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")

    try:
        response = requests.get(url, timeout=timeout_sec)
        response.raise_for_status()
        payload = response.content
        if len(payload) <= minimum_bytes:
            raise RuntimeError(
                f"Instrument master download is unexpectedly small: {len(payload)} bytes"
            )

        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        logger.info(
            "MASTER_CSV_REFRESHED | path=%s | size_kb=%.2f",
            target.resolve(),
            len(payload) / 1024.0,
        )
        return True
    except Exception:
        temporary.unlink(missing_ok=True)
        if target.exists() and target.stat().st_size > minimum_bytes:
            logger.warning(
                "MASTER_CSV_REFRESH_FAILED_USING_EXISTING | path=%s | size_kb=%.2f",
                target.resolve(),
                target.stat().st_size / 1024.0,
                exc_info=True,
            )
            return False
        raise
