# dhan_http_client.py

import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()  # load DHAN_ACCESS_TOKEN, DHAN_BASE_URL from .env

DHAN_ACCESS_TOKEN = os.getenv("DHAN_ACCESS_TOKEN")
DHAN_BASE_URL = os.getenv("DHAN_BASE_URL", "https://api.dhan.co").rstrip("/")

if not DHAN_ACCESS_TOKEN:
    raise RuntimeError("DHAN_ACCESS_TOKEN not set in .env")

class DhanHTTP:
    """
    Minimal Dhan HTTP client for POST JSON calls.
    Uses access-token from .env; no client-id needed.
    """

    def __init__(self,
                 access_token: str = DHAN_ACCESS_TOKEN,
                 base_url: str = DHAN_BASE_URL):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
            "access-token": access_token,
        })

    def post(self, endpoint: str, payload: dict):
        """
        POST JSON to the given Dhan endpoint.
        Prints request + response for debugging.
        """
        url = self.base_url + endpoint
        print(f"\nRequesting: {url}")
        print("Payload:")
        print(json.dumps(payload, indent=2))

        resp = self.session.post(url, data=json.dumps(payload))
        print("Status code:", resp.status_code)

        try:
            body = resp.json()
        except Exception:
            body = resp.text

        print("Body:", body)
        return body
