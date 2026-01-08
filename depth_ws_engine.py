# depth_ws_engine.py
import os
import json
import struct
import websocket
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

if not TOKEN or not CLIENT_ID:
    raise RuntimeError("Missing DHAN_ACCESS_TOKEN or DHAN_CLIENT_ID in .env")

WS_URL = (
    "wss://depth-api-feed.dhan.co/twentydepth"
    f"?token={TOKEN}&clientId={CLIENT_ID}&authType=2"
)

HEADER_SIZE = 12
ROW_SIZE = 16
BID_CODE = 41
ASK_CODE = 51

def _u16_le(b, off):
    return struct.unpack_from("<H", b, off)[0]

def parse_twenty_depth(binary_msg: bytes):
    """
    Returns list of packets:
      each item: dict {security_id, packet_type(41/51), rows[(price,qty,orders), ...]}
    Handles stacked messages.
    """
    out = []
    offset = 0
    total = len(binary_msg)

    while offset + HEADER_SIZE <= total:
        msg_len = _u16_le(binary_msg, offset)
        if msg_len <= HEADER_SIZE:
            break
        if offset + msg_len > total:
            break  # incomplete frame

        feed_code = binary_msg[offset + 2]   # byte 3 in docs (0-based index 2)
        exch_code = binary_msg[offset + 3]   # byte 4
        sec_id = struct.unpack_from("<I", binary_msg, offset + 4)[0]

        payload_start = offset + HEADER_SIZE
        packet_type = binary_msg[payload_start]  # 41/51
        ptr = payload_start + 1
        end = offset + msg_len

        rows = []
        # read until end, in 16-byte rows
        while ptr + ROW_SIZE <= end:
            price, qty, orders = struct.unpack_from("<dII", binary_msg, ptr)
            # keep only meaningful rows
            if price > 0 and qty >= 0:
                rows.append((price, qty, orders))
            ptr += ROW_SIZE

        out.append({
            "security_id": str(sec_id),
            "packet_type": int(packet_type),
            "rows": rows,
            "feed_code": int(feed_code),
            "exch_code": int(exch_code),
        })

        offset += msg_len

    return out


class DepthBook:
    def __init__(self):
        self.bids = []
        self.asks = []

    def update(self, packet_type, rows):
        if packet_type == BID_CODE:
            self.bids = rows
        elif packet_type == ASK_CODE:
            self.asks = rows

    def ready(self):
        return bool(self.bids and self.asks)

    def top5_stats(self):
        bids = self.bids[:5]
        asks = self.asks[:5]
        if not bids or not asks:
            return None

        best_bid = bids[0][0]
        best_ask = asks[0][0]
        bid_qty = sum(q for _, q, _ in bids)
        ask_qty = sum(q for _, q, _ in asks)
        spread = best_ask - best_bid if best_bid and best_ask else 0.0
        mid = (best_bid + best_ask) / 2.0 if best_bid and best_ask else 0.0
        imb = ((bid_qty - ask_qty) / (bid_qty + ask_qty)) if (bid_qty + ask_qty) > 0 else 0.0

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": mid,
            "bid_qty": bid_qty,
            "ask_qty": ask_qty,
            "spread": spread,
            "imbalance": imb,
        }


class DepthWSEngine:
    def __init__(self, instruments):
        """
        instruments = list of dict:
          {"ExchangeSegment":"NSE_FNO", "SecurityId":"49543", "tag":"NIFTY_FUT"}
        """
        self.instruments = instruments
        self.books = {}  # sec_id -> DepthBook
        self.tags = {i["SecurityId"]: i.get("tag", i["SecurityId"]) for i in instruments}

    def _ensure_book(self, sec_id):
        if sec_id not in self.books:
            self.books[sec_id] = DepthBook()
        return self.books[sec_id]

    def on_open(self, ws):
        print("✅ WS CONNECTED (20 LEVEL DEPTH)")
        sub_msg = {
            "RequestCode": 23,
            "InstrumentCount": len(self.instruments),
            "InstrumentList": [
                {"ExchangeSegment": i["ExchangeSegment"], "SecurityId": i["SecurityId"]}
                for i in self.instruments
            ]
        }
        ws.send(json.dumps(sub_msg))
        print("📡 SUBSCRIBED:", ", ".join([f'{i.get("tag","")}[{i["SecurityId"]}]' for i in self.instruments]))
        print("---- LIVE ----")

    def on_message(self, ws, message):
        packets = parse_twenty_depth(message)
        if not packets:
            return

        for p in packets:
            sec = p["security_id"]
            book = self._ensure_book(sec)
            book.update(p["packet_type"], p["rows"])

            if book.ready():
                s = book.top5_stats()
                if not s:
                    continue

                tag = self.tags.get(sec, sec)
                ts = datetime.now().strftime("%H:%M:%S")

                print(
                    f"{ts} | {tag} | SEC:{sec} | "
                    f"BID:{s['best_bid']:.2f}({s['bid_qty']}) "
                    f"ASK:{s['best_ask']:.2f}({s['ask_qty']}) "
                    f"SP:{s['spread']:.2f} IMB:{s['imbalance']:+.3f}"
                )

    def on_error(self, ws, error):
        print("❌ WS ERROR:", error)

    def on_close(self, ws, code, msg):
        print(f"🔌 WS CLOSED | code={code}, msg={msg}")

    def run(self):
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.run_forever(ping_interval=10, ping_timeout=5)
