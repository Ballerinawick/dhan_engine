class TradeState:
    def __init__(self, entry_price: float, ts: float):
        self.entry = float(entry_price)
        self.entry_ts = float(ts)
        self.last_ts = float(ts)

        self.best_price = float(entry_price)
        self.worst_price = float(entry_price)
        self.last_price = float(entry_price)

        self.mfe = 0.0
        self.mae = 0.0

        self.seconds_in_trade = 0.0
        self.seconds_below_entry = 0.0

        self.retests = 0
        self.accepted = False

    def update(self, price: float, ts: float):
        price = float(price)
        ts = float(ts)

        dt = max(ts - self.last_ts, 0.0)
        self.last_ts = ts
        self.seconds_in_trade = max(ts - self.entry_ts, 0.0)

        if price > self.best_price:
            self.best_price = price

        if price < self.worst_price:
            self.worst_price = price

        self.mfe = max(self.best_price - self.entry, 0.0)
        self.mae = max(self.entry - self.worst_price, 0.0)

        if price < self.entry:
            self.seconds_below_entry += dt
        else:
            self.seconds_below_entry = 0.0

        retest_buffer = max(self.entry * 0.002, 0.10)
        if self.last_price >= self.entry and price < (self.entry - retest_buffer):
            self.retests += 1

        if self.seconds_in_trade >= 6.0 and self.mfe > 0:
            self.accepted = True

        self.last_price = price
