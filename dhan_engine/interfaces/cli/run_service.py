import os


def main() -> None:
    service = os.getenv("DHAN_SERVICE", "index").strip().lower()
    aliases = {
        "": "index",
        "nifty": "index",
        "options": "index",
        "option": "index",
        "index": "index",
        "stock": "stock",
        "stocks": "stock",
        "stock-depth-paper": "stock_depth",
        "stock_depth_paper": "stock_depth",
        "stock-future-depth-paper": "stock_depth",
        "commodity": "commodity",
        "commodities": "commodity",
        "mcx": "commodity",
        "timed-straddle": "timed_straddle",
        "timed_straddle": "timed_straddle",
        "straddle-experiment": "timed_straddle",
        "depth-research": "depth_research",
        "full-depth": "depth_research",
        "deeplob-recorder": "deeplob_recorder",
        "deeplob_recorder": "deeplob_recorder",
        "deeplob-inference": "deeplob_inference",
        "deeplob_inference": "deeplob_inference",
        "deeplob-live": "deeplob_live",
        "deeplob_live": "deeplob_live",
        "deeplob-paper": "deeplob_live",
        "deeplob_paper": "deeplob_live",
    }
    selected = aliases.get(service)
    if selected is None:
        valid = ", ".join(sorted(k for k in aliases if k))
        raise RuntimeError(f"Unknown DHAN_SERVICE={service!r}. Use one of: {valid}")

    if selected == "stock":
        from dhan_engine.interfaces.cli.run_stock_paper import main as run_stock

        run_stock()
        return
    if selected == "stock_depth":
        from dhan_engine.interfaces.cli.run_stock_depth_paper import (
            main as run_stock_depth,
        )

        run_stock_depth()
        return
    if selected == "commodity":
        from dhan_engine.interfaces.cli.run_commodity_paper import main as run_commodity

        run_commodity()
        return
    if selected == "timed_straddle":
        from dhan_engine.interfaces.cli.run_timed_straddle import main as run_timed_straddle

        run_timed_straddle()
        return
    if selected == "depth_research":
        from dhan_engine.interfaces.cli.run_full_depth_research import main as run_depth_research

        run_depth_research()
        return
    if selected == "deeplob_recorder":
        from dhan_engine.interfaces.cli.run_deeplob_recorder import main as run_deeplob_recorder

        run_deeplob_recorder()
        return
    if selected == "deeplob_inference":
        from dhan_engine.interfaces.cli.run_deeplob_inference import main as run_deeplob_inference

        run_deeplob_inference()
        return
    if selected == "deeplob_live":
        from dhan_engine.interfaces.cli.run_deeplob_live import main as run_deeplob_live

        run_deeplob_live()
        return

    from dhan_engine.interfaces.cli.run_ws import main as run_index

    run_index()


if __name__ == "__main__":
    main()


