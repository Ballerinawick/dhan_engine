class MultiTimeframeBiasFilter:
    def check(self, candles_1s, candles_3s):
        if len(candles_1s) < 2 or len(candles_3s) < 2:
            return True

        trend1 = candles_1s[-1]["close"] > candles_1s[-2]["close"]
        trend3 = candles_3s[-1]["close"] > candles_3s[-2]["close"]

        bias_ok = trend3

        print(
            f"🧭 MTF_BIAS | 1s={trend1} | 3s={trend3} | ok={bias_ok}"
        )

        return bias_ok
