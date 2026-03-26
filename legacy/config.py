# config.py
import os
from dotenv import load_dotenv

load_dotenv()

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_BASE_URL     = os.getenv("DHAN_BASE_URL", "https://api.dhan.co")
DHAN_CLIENT_ID    = os.getenv("DHAN_CLIENT_ID")

if not DHAN_ACCESS_TOKEN:
    raise RuntimeError("DHAN_ACCESS_TOKEN not set in .env")

if not DHAN_CLIENT_ID:
    raise RuntimeError("DHAN_CLIENT_ID not set in .env")


WS_URL=""