class TradeState:
    def __init__(self, entry_price: float, ts: float):
        self.entry = entry_price
        self.entry_ts = ts
        self.last_ts = ts

        self.best_price = entry_price
        self.worst_price = entry_price

        self.mfe = 0.0
        self.mae = 0.0

        self.seconds_in_trade = 0.0
        self.seconds_below_entry = 0.0

        self.retests = 0
        self.last_price = entry_price

        self.accepted = False

    def update(self, price: float, ts: float):
        dt = ts - self.entry_ts
        self.seconds_in_trade = dt

        if price > self.best_price:
            self.best_price = price

        if price < self.worst_price:
            self.worst_price = price

        self.mfe = self.best_price - self.entry
        self.mae = self.entry - self.worst_price

        delta = ts - self.last_ts

        if price < self.entry:
            self.seconds_below_entry += delta

        self.last_ts = ts

        buffer = 0.05
        if self.last_price >= self.entry + buffer and price < self.entry - buffer:
            self.retests += 1

        if price > self.entry and dt > 2:
            self.accepted = True

        self.last_price = price
