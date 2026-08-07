import unittest
from unittest.mock import Mock, patch

from dhan_engine.interfaces.cli import run_deeplob_live


class RunDeepLobLiveTest(unittest.TestCase):
    def test_refreshes_configured_master_before_starting_runtime(self):
        settings = object()
        runtime = Mock()

        with (
            patch.dict("os.environ", {"CSV_FILE": "/tmp/instruments.csv"}, clear=False),
            patch.object(run_deeplob_live, "load_dotenv"),
            patch.object(run_deeplob_live, "refresh_master_csv") as refresh,
            patch.object(
                run_deeplob_live.DeepLobLiveSettings,
                "from_env",
                return_value=settings,
            ),
            patch.object(
                run_deeplob_live,
                "build_deeplob_live_runtime",
                return_value=runtime,
            ),
        ):
            run_deeplob_live.main()

        refresh.assert_called_once_with(
            run_deeplob_live.MASTER_URL,
            "/tmp/instruments.csv",
        )
        runtime.run.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
