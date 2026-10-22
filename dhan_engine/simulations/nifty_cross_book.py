"""Isolated, ideal-price NIFTY cross paper book. Never routes broker orders."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo


IST = ZoneInfo("Asia/Kolkata")
START = time(9, 15)
END = time(15, 30)
ENTRY_END = time(15, 20)
FLAT = time(15, 29)


@dataclass(frozen=True)
class Close:
    t: datetime
    price: float


@dataclass(frozen=True)
class Pair:
    strike: float
    call_id: int
    put_id: int


@dataclass(frozen=True)
class Hit:
    id: str
    t: datetime
    side: str
    entry: float
    call_id: int
    put_id: int


@dataclass
class Trade:
    hit_id: str
    side: str
    call_id: int
    put_id: int
    entry_t: datetime
    entry: float
    high_lead: float = 0.0
    high_quote: float = 0.0
    exit_t: datetime | None = None
    exit: float | None = None
    reason: str | None = None
    pnl: float | None = None
    mark: float | None = None


@dataclass
class Book:
    day: str = ""
    expiry: str = ""
    minutes: dict[int, list[Close]] = field(default_factory=dict)
    open: Trade | None = None
    closed: list[Trade] = field(default_factory=list)
    seen: list[str] = field(default_factory=list)


def ist(t: datetime) -> datetime:
    if t.tzinfo is None:
        raise ValueError("Market timestamps must include a timezone")
    return t.astimezone(IST)


def centered_strikes(anchor: float, available: list[float]) -> list[float]:
    strikes = sorted(set(available))
    if len(strikes) < 5:
        raise ValueError("Five strikes are required")
    atm = min(range(len(strikes)), key=lambda i: (abs(strikes[i] - anchor), strikes[i]))
    if atm < 2 or atm + 2 >= len(strikes):
        raise ValueError("Two strikes on either side of ATM are required")
    return strikes[atm - 2:atm + 3]


def atm_pair(future: float, pairs: list[Pair]) -> Pair:
    return min(pairs, key=lambda pair: (abs(pair.strike - future), pair.strike))


def samples_of(points: list[Close], now: datetime) -> list[Close]:
    current = ist(now).replace(second=0, microsecond=0)
    valid = [p for p in points if p.price > 0 and ist(p.t).date() == current.date()
             and START <= ist(p.t).time() <= END and ist(p.t) < current]
    valid.sort(key=lambda p: p.t)
    clean = []
    for i, point in enumerate(valid):
        if 0 < i < len(valid) - 1:
            previous, following = valid[i - 1].price, valid[i + 1].price
            if abs(point.price - previous) / previous > .40 and abs(following - previous) / previous < .15:
                continue
        clean.append(point)
    return [p for p in clean if ist(p.t) >= current - timedelta(hours=7)][-400:]


def pair_path(call: list[Close], put: list[Close]) -> list[tuple[datetime, float, float]]:
    """Linearly interpolate both series at their union of observed minute times."""
    if len(call) < 2 or len(put) < 2:
        return []
    times = sorted({p.t for p in call} | {p.t for p in put})

    def at(points: list[Close], t: datetime) -> float | None:
        for left, right in zip(points, points[1:]):
            if left.t <= t <= right.t and right.t - left.t <= timedelta(minutes=6):
                fraction = (t - left.t) / (right.t - left.t)
                return left.price + fraction * (right.price - left.price)
        return None

    path = []
    for t in times:
        ce, pe = at(call, t), at(put, t)
        if ce is not None and pe is not None:
            path.append((t, ce, pe))
    return path


def pair_hits(path: list[tuple[datetime, float, float]], pair: Pair) -> list[Hit]:
    hits = []
    for (t0, c0, p0), (t1, c1, p1) in zip(path, path[1:]):
        if t1 - t0 > timedelta(minutes=6):
            continue
        d0, d1 = c0 - p0, c1 - p1
        if d0 * d1 >= 0:
            continue
        ce_delta, pe_delta = c1 - c0, p1 - p0
        side = "CE" if ce_delta > 0 and pe_delta < 0 else "PE" if ce_delta < 0 and pe_delta > 0 else None
        if side is None:
            continue
        u = d0 / (d0 - d1)
        crossed = t0 + (t1 - t0) * u
        bucket = ist(crossed).replace(second=0, microsecond=0).isoformat()
        price = (c0 + u * ce_delta) if side == "CE" else (p0 + u * pe_delta)
        hits.append(Hit(f"1m|{pair.call_id}|{pair.put_id}|{bucket}", crossed, side, price,
                        pair.call_id, pair.put_id))
    return hits


def _close(book: Book, trade: Trade, quote: float, t: datetime, reason: str) -> None:
    trade.exit_t, trade.exit, trade.reason = t, quote, reason
    trade.pnl = (quote - trade.entry) * 65 - 60
    book.closed.insert(0, trade)
    del book.closed[120:]
    book.open = None


def turn_of(book: Book, path: list[tuple[datetime, float, float]], until: datetime) -> None:
    trade = book.open
    if trade is None:
        return
    entry_minute = ist(trade.entry_t).replace(second=0, microsecond=0)
    for t, call, put in path:
        if t > until or ist(t).replace(second=0, microsecond=0) <= entry_minute:
            continue
        lead = call - put if trade.side == "CE" else put - call
        quote = call if trade.side == "CE" else put
        if lead > trade.high_lead:
            trade.high_lead, trade.high_quote = lead, quote
            continue
        if trade.high_lead >= 5 and trade.high_lead - lead >= 2:
            _close(book, trade, trade.high_quote, t, "turned")
            return
        if lead <= -2:
            _close(book, trade, quote, t, "flipped")
            return


def reduce_atm(book: Book, pair: Pair, now: datetime) -> Book:
    now = ist(now)
    if book.day != now.date().isoformat():
        book.day, book.minutes, book.open, book.closed, book.seen = now.date().isoformat(), {}, None, [], []
    call = samples_of(book.minutes.get(pair.call_id, []), now)
    put = samples_of(book.minutes.get(pair.put_id, []), now)
    path = pair_path(call, put)
    hits = pair_hits(path, pair)
    open_path = path
    if book.open and (book.open.call_id, book.open.put_id) != (pair.call_id, pair.put_id):
        open_call = samples_of(book.minutes.get(book.open.call_id, []), now)
        open_put = samples_of(book.minutes.get(book.open.put_id, []), now)
        open_path = pair_path(open_call, open_put)
    seen = set(book.seen)
    for hit in hits:
        if book.open and (book.open.call_id, book.open.put_id) == (pair.call_id, pair.put_id):
            turn_of(book, path, hit.t)
        if hit.id in seen:
            continue
        if book.open:
            continue
        book.seen.append(hit.id)
        seen.add(hit.id)
        ht = ist(hit.t)
        latest = book.closed[0].exit_t if book.closed else None
        if ht.weekday() >= 5 or not START <= ht.time() < ENTRY_END or (latest and hit.t < latest):
            continue
        book.open = Trade(hit.id, hit.side, hit.call_id, hit.put_id, hit.t, hit.entry,
                          high_quote=hit.entry, mark=hit.entry)
    turn_of(book, open_path, now)
    if book.open and (now.time() >= FLAT or now.weekday() >= 5):
        cutoff = now.replace(hour=15, minute=29, second=0, microsecond=0)
        prior = [point for point in open_path if point[0] <= cutoff]
        if prior:
            t, call_price, put_price = prior[-1]
            _close(book, book.open, call_price if book.open.side == "CE" else put_price, t, "flat")
        else:
            leg = samples_of(book.minutes.get(
                book.open.call_id if book.open.side == "CE" else book.open.put_id, []), now)
            if leg:
                _close(book, book.open, leg[-1].price, leg[-1].t, "flat")
    del book.seen[:-8000]
    return book


def apply_tick(book: Book, security_id: int, price: float, now: datetime) -> None:
    """Only update the forming minute and open mark; never reduce trades."""
    now = ist(now)
    if book.day != now.date().isoformat():
        book.day, book.minutes, book.open, book.closed, book.seen = now.date().isoformat(), {}, None, [], []
    minute = now.replace(second=0, microsecond=0)
    points = book.minutes.setdefault(security_id, [])
    if points and points[-1].t == minute:
        points[-1] = Close(minute, price)
    else:
        points.append(Close(minute, price))
    del points[:-400]
    if book.open and security_id == (book.open.call_id if book.open.side == "CE" else book.open.put_id):
        book.open.mark = price

