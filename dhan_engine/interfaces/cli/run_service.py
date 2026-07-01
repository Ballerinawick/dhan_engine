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
        "commodity": "commodity",
        "commodities": "commodity",
        "mcx": "commodity",
        "timed-straddle": "timed_straddle",
        "timed_straddle": "timed_straddle",
        "straddle-experiment": "timed_straddle",
    }
    selected = aliases.get(service)
    if selected is None:
        valid = ", ".join(sorted(k for k in aliases if k))
        raise RuntimeError(f"Unknown DHAN_SERVICE={service!r}. Use one of: {valid}")

    if selected == "stock":
        from dhan_engine.interfaces.cli.run_stock_paper import main as run_stock

        run_stock()
        return
    if selected == "commodity":
        from dhan_engine.interfaces.cli.run_commodity_paper import main as run_commodity

        run_commodity()
        return
    if selected == "timed_straddle":
        from dhan_engine.interfaces.cli.run_timed_straddle import main as run_timed_straddle

        run_timed_straddle()
        return

    from dhan_engine.interfaces.cli.run_ws import main as run_index

    run_index()


if __name__ == "__main__":
    main()
