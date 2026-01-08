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

EXCHANGE_SEGMENT = "NSE_FNO"   # ✅ from Dhan Annexure (Derivatives)

# ✅ FUT Security IDs (from your CSV output)
SEC_IDS = {
    "BANKNIFTY_FUT": "49508",
    "NIFTY_FUT": "49543",
    "FINNIFTY_FUT": "49521",
}

HEADER_SIZE = 12
ROW_SIZE = 16
LEVELS = 20

BID_CODE = 41
ASK_CODE = 51


# ================= DEPTH STATE =================
class DepthState:
    def __init__(self):
        self.book = {}  # security_id -> {"bid": [...], "ask": [...]}

    def update(self, security_id: int, side: str, rows):
        if security_id not in self.book:
            self.book[security_id] = {"bid": [], "ask": []}
        self.book[security_id][side] = rows

    def get(self, security_id: int):
        return self.book.get(security_id, {"bid": [], "ask": []})


state = DepthState()


# ================= PARSER (CORRECT) =================
def parse_depth_frames(binary_msg: bytes):
    """
    Dhan 20-depth packet = 332 bytes:
    Header (12 bytes) + Payload (320 bytes = 20 * 16)
    Header layout (0-based):
      0-1   : uint16 message_length (full frame length)
      2     : uint8 feed_response_code  -> 41 BID / 51 ASK
      3     : uint8 exchange_segment (enum)
      4-7   : int32 security_id
      8-11  : uint32 seq (ignore)
      12..  : 20 rows of (float64 price, uint32 qty, uint32 orders)
    Multiple frames can be stacked back-to-back in one websocket message.
    """
    frames = []
    offset = 0
    total = len(binary_msg)

    while offset + HEADER_SIZE <= total:
        msg_len = struct.unpack_from("<H", binary_msg, offset)[0]

        # safety
        if msg_len < 332:
            # too small / corrupted
            break
        if offset + msg_len > total:
            # incomplete stacked frame
            break

        feed_code = binary_msg[offset + 2]  # ✅ 41/51 is HERE
        security_id = struct.unpack_from("<i", binary_msg, offset + 4)[0]

        payload_start = offset + HEADER_SIZE
        payload_end = offset + msg_len

        rows = []
        ptr = payload_start

        # read exactly 20 levels (if available)
        for _ in range(LEVELS):
            if ptr + ROW_SIZE > payload_end:
                break
            price, qty, orders = struct.unpack_from("<dII", binary_msg, ptr)
            if price > 0 and qty > 0:
                rows.append((price, qty, orders))
            ptr += ROW_SIZE

        frames.append((feed_code, security_id, rows))
        offset += msg_len

    return frames


# ================= WS CALLBACKS =================
def on_open(ws):
    print("✅ WS CONNECTED (20 LEVEL DEPTH)")

    inst_list = []
    for name, sec_id in SEC_IDS.items():
        inst_list.append({"ExchangeSegment": EXCHANGE_SEGMENT, "SecurityId": str(sec_id)})

    sub_msg = {
        "RequestCode": 23,
        "InstrumentCount": len(inst_list),
        "InstrumentList": inst_list
    }

    ws.send(json.dumps(sub_msg))
    print(f"📡 Subscribed to {len(inst_list)} FUT instruments:", list(SEC_IDS.keys()))
    print("Waiting for BID/ASK packets...\n")


def on_message(ws, message):
    # websocket-client gives bytes for binary frames
    if isinstance(message, str):
        # should not happen for depth, but just in case
        return

    frames = parse_depth_frames(message)
    if not frames:
        return

    for feed_code, security_id, rows in frames:
        if feed_code == BID_CODE:
            state.update(security_id, "bid", rows)
        elif feed_code == ASK_CODE:
            state.update(security_id, "ask", rows)

        book = state.get(security_id)
        bids = book["bid"]
        asks = book["ask"]

        if not bids or not asks:
            continue

        top_bids = bids[:5]
        top_asks = asks[:5]

        best_bid = top_bids[0][0]
        best_ask = top_asks[0][0]

        bid_qty = sum(q for _, q, _ in top_bids)
        ask_qty = sum(q for _, q, _ in top_asks)

        spread = (best_ask - best_bid) if (best_bid and best_ask) else 0.0
        imb = ((bid_qty - ask_qty) / (bid_qty + ask_qty)) if (bid_qty + ask_qty) else 0.0

        print(
            f"{datetime.now().strftime('%H:%M:%S')} | "
            f"SEC:{security_id} | "
            f"BID:{best_bid:.2f} ({bid_qty}) | "
            f"ASK:{best_ask:.2f} ({ask_qty}) | "
            f"SPREAD:{spread:.2f} | IMB:{imb:+.3f}"
        )


def on_error(ws, error):
    print("❌ WS ERROR:", error)


def on_close(ws, code, msg):
    print(f"🔌 WS CLOSED | code={code}, msg={msg}")


if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever(ping_interval=10, ping_timeout=5)
