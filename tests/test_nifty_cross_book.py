from datetime import datetime, timedelta
import os
import sys
from types import ModuleType
from unittest.mock import patch
from zoneinfo import ZoneInfo

from dhan_engine.simulations.nifty_cross_book import (
    Book, Close, Pair, Trade, apply_tick, centered_strikes, reduce_atm,
)
from dhan_engine.simulations.nifty_cross_session import CrossSession


BASE = datetime(2026, 10, 19, 9, 15, tzinfo=ZoneInfo("Asia/Kolkata"))
PAIR = Pair(22600, 101, 102)


def run_series(calls, puts, *, now_minute=None):
    book = Book(day=BASE.date().isoformat())
    book.minutes[101] = [Close(BASE + timedelta(minutes=i), price) for i, price in enumerate(calls)]
    book.minutes[102] = [Close(BASE + timedelta(minutes=i), price) for i, price in enumerate(puts)]
    minute = len(calls) if now_minute is None else now_minute
    reduce_atm(book, PAIR, BASE + timedelta(minutes=minute))
    return book


def test_initial_put_stays_open():
    book = run_series([10, 6, 5.4, 5.6], [6, 10, 6.4, 6.2])
    assert book.open is not None and book.open.side == "PE"
    assert not book.closed


def test_put_turn_then_later_call():
    book = run_series([10, 6, 4, 2, 5, 12], [6, 10, 12, 14, 10, 6])
    assert book.closed[0].side == "PE"
    assert book.closed[0].exit == 14
    assert book.closed[0].reason == "turned"
    assert book.open is not None and book.open.side == "CE"


def test_flip_does_not_reverse_on_second_pass():
    book = run_series([150, 167, 160, 142], [184, 166, 172, 198])
    assert book.closed[0].side == "CE"
    assert book.closed[0].exit == 160
    assert book.closed[0].reason == "flipped"
    assert book.open is None
    reduce_atm(book, PAIR, BASE + timedelta(minutes=4, seconds=1))
    assert book.open is None


def test_spike_is_not_a_trade():
    book = run_series([10, 10, 10, 10], [8, 80, 8, 8])
    assert book.open is None and not book.closed


def test_cutoff_flat_and_tick_only_marks():
    book = Book(day=BASE.date().isoformat())
    book.open = Trade("prior", "CE", 101, 102, BASE, 100)
    apply_tick(book, 101, 110, BASE + timedelta(minutes=2, seconds=4))
    assert book.open.mark == 110 and not book.closed
    book.minutes[101] = [Close(BASE + timedelta(minutes=373), 110)]
    book.minutes[102] = [Close(BASE + timedelta(minutes=373), 90)]
    reduce_atm(book, PAIR, BASE.replace(hour=15, minute=29))
    assert book.closed[0].reason == "flat"
    assert book.closed[0].pnl == 590


def test_strike_window():
    assert centered_strikes(22597.40, [22500, 22550, 22600, 22650, 22700, 22750]) == [
        22500, 22550, 22600, 22650, 22700]


def test_entry_cutoff_and_exact_touch():
    later = BASE.replace(hour=15, minute=22)
    book = Book(day=BASE.date().isoformat(), minutes={
        101: [Close(later - timedelta(minutes=2), 10), Close(later - timedelta(minutes=1), 6)],
        102: [Close(later - timedelta(minutes=2), 6), Close(later - timedelta(minutes=1), 10)],
    })
    reduce_atm(book, PAIR, later)
    assert book.open is None
    assert len(book.seen) == 1
    touch = run_series([10, 8, 10], [6, 8, 6])
    assert touch.open is None and not touch.seen


def test_session_save_load_and_new_day(tmp_path):
    path = tmp_path / "cross.json"
    session = CrossSession(path, "2026-10-20", [PAIR], save_delay=0)
    session.tick(101, 10, BASE)
    session.step(22600, BASE + timedelta(seconds=1))
    assert path.exists()
    restored = CrossSession(path, "2026-10-20", [PAIR])
    restored.load(BASE + timedelta(minutes=1))
    assert restored.book.minutes[101][0].price == 10
    restored.step(22600, BASE + timedelta(days=1))
    assert restored.book.minutes == {}


def test_service_selects_only_cross_runner():
    from dhan_engine.interfaces.cli.run_service import main

    module = ModuleType("dhan_engine.interfaces.cli.run_nifty_cross_paper")
    calls = []
    module.main = lambda: calls.append("cross")
    with patch.dict(os.environ, {"DHAN_SERVICE": "nifty-cross-paper"}), patch.dict(sys.modules, {
        module.__name__: module,
    }):
        main()
    assert calls == ["cross"]

