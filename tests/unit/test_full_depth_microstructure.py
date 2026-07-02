import struct

from dhan_engine.domain.market.full_depth_microstructure import (
    BookSnapshot, CrossInstrumentDepthEngine, InstrumentBookAnalyzer, MicrostructureState, TradeObservation,
)


def book(name="FUT", mono=1.0, bid_qty=100, ask_qty=50):
    bids = [(100-i, bid_qty if i == 0 else 10, 5) for i in range(20)]
    asks = [(101+i, ask_qty if i == 0 else 10, 5) for i in range(20)]
    return BookSnapshot.build(1, name, bids, asks, 1, mono)


def test_snapshot_is_ordered_and_first_tick_only_warms_up():
    snap = BookSnapshot.build(1, "CE", [(99, 2, 1), (100, 3, 1)], [(102, 2, 1), (101, 3, 1)], 1, 1)
    state = InstrumentBookAnalyzer().update(snap)
    assert [x.price for x in snap.bids] == [100, 99]
    assert [x.price for x in snap.asks] == [101, 102]
    assert not state.ready


def test_depletion_refill_and_trade_inference_are_causal():
    analyzer = InstrumentBookAnalyzer()
    analyzer.update(book(mono=1, bid_qty=100, ask_qty=100))
    trade = TradeObservation(101, 7, received_mono=2)
    state = analyzer.update(book(mono=2, bid_qty=140, ask_qty=40), trade)
    assert state.ready and state.bid_refill == 40 and state.ask_depletion == 60
    assert state.inferred_buy_qty == 7 and state.pressure > 0
    duplicate = analyzer.update(book(mono=3, bid_qty=140, ask_qty=40), trade)
    assert duplicate.inferred_buy_qty == 0


def test_three_leg_confirmation_still_rejects_move_below_cost():
    engine = CrossInstrumentDepthEngine(25, 80, slippage_points=1, min_confidence=.4)
    def state(name, pressure):
        return MicrostructureState(name=name, ready=True, mid=100, microprice=100.1, spread=1,
                                   pressure=pressure, persistence=1, received_mono=10)
    engine.update("FUT", state("FUT", .9), 10)
    engine.update("CE", state("CE", .9), 10)
    result = engine.update("PE", state("PE", -.9), 10)
    assert result.direction == "UP"
    assert result.reason == "EXPECTED_MOVE_BELOW_COST"
    assert result.action == "NO_TRADE"


def test_cross_leg_conflict_blocks_entry():
    engine = CrossInstrumentDepthEngine(25, 10, min_confidence=.1)
    base = dict(ready=True, mid=100, microprice=102, spread=.1, velocity_points_sec=2, received_mono=10)
    engine.update("FUT", MicrostructureState(name="FUT", pressure=.9, **base), 10)
    engine.update("CE", MicrostructureState(name="CE", pressure=-.9, **base), 10)
    result = engine.update("PE", MicrostructureState(name="PE", pressure=-.9, **base), 10)
    assert result.reason == "CROSS_LEG_CONFLICT"


def test_stale_leg_blocks_entry():
    engine = CrossInstrumentDepthEngine(25, 10, stale_after_sec=2)
    for leg in ("FUT", "CE"):
        engine.update(leg, MicrostructureState(name=leg, ready=True, pressure=.8, received_mono=1), 1)
    result = engine.update("PE", MicrostructureState(name="PE", ready=True, pressure=-.8, received_mono=10), 10)
    assert result.reason == "STALE_BOOK"
