import os, ssl, websocket, socket
from dotenv import load_dotenv

load_dotenv()

token = os.getenv("DHAN_ACCESS_TOKEN", "").strip()
client_id = os.getenv("DHAN_CLIENT_ID", "").strip()

if not token or not client_id:
    raise RuntimeError("Missing DHAN_ACCESS_TOKEN / DHAN_CLIENT_ID")

url = f"wss://depth-api-feed.dhan.co/twentydepth?token={token}&clientId={client_id}&authType=2"
print("URL:", url)

# Quick DNS + TCP check
host = "depth-api-feed.dhan.co"
ip = socket.gethostbyname(host)
print("Resolved IP:", ip)

sock = socket.create_connection((host, 443), timeout=8)
print("TCP 443 OK")
sock.close()

# WS handshake check
ws = websocket.create_connection(
    url,
    timeout=10,
    sslopt={"cert_reqs": ssl.CERT_REQUIRED},
)
print("WS HANDSHAKE OK ✅")
ws.close()
print("DONE")
