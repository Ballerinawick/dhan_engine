# dhan_marketfeed_rest.py

from typing import Dict, List, Any
from dhan_http_client import DhanHTTP


class MarketFeed:
    """
    Thin wrapper over Dhan /marketfeed REST APIs:
      - /marketfeed/ltp
      - /marketfeed/ohlc
      - /marketfeed/quote
    """

    def __init__(self, dhan_http: DhanHTTP | None = None):
        self.dhan_http = dhan_http or DhanHTTP()

    def _build_payload(self, securities: Dict[str, List[int]]) -> Dict[str, List[int]]:
        """
        Dhan expects:
            {
              "NSE_EQ": [11536],
              "NSE_FNO": [49081, 49082]
            }
        So we just pass through the dict.
        """
        return {exchange_segment: security_ids
                for exchange_segment, security_ids in securities.items()}

    def ticker_data(self, securities: Dict[str, List[int]]) -> Any:
        """
        LTP only.
        """
        endpoint = "/marketfeed/ltp"
        payload = self._build_payload(securities)
        return self.dhan_http.post(endpoint, payload)

    def ohlc_data(self, securities: Dict[str, List[int]]) -> Any:
        """
        OHLC + LTP.
        """
        endpoint = "/marketfeed/ohlc"
        payload = self._build_payload(securities)
        return self.dhan_http.post(endpoint, payload)

    def quote_data(self, securities: Dict[str, List[int]]) -> Any:
        """
        Full quote: depth + OHLC + OI + volume + LTP.
        """
        endpoint = "/marketfeed/quote"
        payload = self._build_payload(securities)
        return self.dhan_http.post(endpoint, payload)
