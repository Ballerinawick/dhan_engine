import os
import json
import websocket
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
CLIENT_ID = os.getenv("DHAN_CLIENT_ID")

WS_URL = f"wss://api-feed.dhan.co/ws?token={TOKEN}&clientId={CLIENT_ID}"

NIFTY_FUT_SEC_ID = "49543"  # confirmed working

def on_open(ws):
    print("✅ LTP WS CONNECTED")

    sub = {
        "RequestCode": 15,
        "InstrumentCount": 1,
        "InstrumentList": [
            {
                "ExchangeSegment": "NSE_FNO",
                "SecurityId": NIFTY_FUT_SEC_ID
            }
        ]
    }
    ws.send(json.dumps(sub))
    print("📡 Subscribed to NIFTY FUT LTP")

def on_message(ws, msg):
    print("📥", msg)

def on_error(ws, err):
    print("❌ WS ERROR:", err)

def on_close(ws, code, msg):
    print("🔌 WS CLOSED")

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()
