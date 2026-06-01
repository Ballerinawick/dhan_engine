import json
import logging
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)


def start_session_viewer_server() -> None:
    enabled = str(os.getenv("TRIWAVE_VIEWER_ENABLED", "true")).strip().lower()
    if enabled in {"0", "false", "no", "off"}:
        logger.info("TRIWAVE_VIEWER_DISABLED")
        return

    port = int(os.getenv("PORT", os.getenv("TRIWAVE_VIEWER_PORT", "8080")) or 8080)
    host = os.getenv("TRIWAVE_VIEWER_HOST", "0.0.0.0")
    server = ThreadingHTTPServer((host, port), SessionViewerHandler)
    thread = threading.Thread(target=server.serve_forever, name="TriWaveSessionViewer", daemon=True)
    thread.start()
    logger.info("TRIWAVE_VIEWER_ACTIVE | host=%s | port=%s", host, port)


class SessionViewerHandler(BaseHTTPRequestHandler):
    server_version = "TriWaveSessionViewer/1.0"

    def log_message(self, fmt, *args):
        logger.info("TRIWAVE_VIEWER_HTTP | " + fmt, *args)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/"):
            self._handle_api(parsed)
            return
        self._serve_static(parsed.path)

    def _handle_api(self, parsed):
        if not self._authorized(parsed):
            self._json({"error": "unauthorized"}, HTTPStatus.UNAUTHORIZED)
            return

        query = parse_qs(parsed.query)
        if parsed.path == "/api/sessions":
            self._json({"sessions": self._list_sessions()})
            return
        if parsed.path == "/api/session/latest":
            session = self._latest_session()
            self._json(self._session_payload(session, query))
            return
        if parsed.path == "/api/session":
            date = (query.get("date") or [""])[0]
            expiry = (query.get("expiry") or [""])[0]
            session = self._resolve_session(date, expiry)
            self._json(self._session_payload(session, query))
            return
        self._json({"error": "not found"}, HTTPStatus.NOT_FOUND)

    def _authorized(self, parsed) -> bool:
        token = os.getenv("TRIWAVE_VIEWER_TOKEN", "").strip()
        if not token:
            return True
        query_token = (parse_qs(parsed.query).get("token") or [""])[0]
        return query_token == token

    def _serve_static(self, request_path: str) -> None:
        static_root = _static_root()
        name = "index.html" if request_path in {"", "/"} else request_path.lstrip("/")
        path = (static_root / name).resolve()
        if static_root not in path.parents and path != static_root:
            self.send_error(HTTPStatus.FORBIDDEN)
            return
        if not path.exists() or path.is_dir():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        content_type = {
            ".html": "text/html; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".css": "text/css; charset=utf-8",
        }.get(path.suffix, "application/octet-stream")
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _session_payload(self, session: Path | None, query: dict) -> dict:
        if session is None:
            return {"session": None, "ticks": [], "trades": [], "signals": []}
        limit = _positive_int((query.get("limit") or ["50000"])[0], 50000)
        return {
            "session": {
                "date": session.parent.name,
                "expiry": session.name.replace("expiry=", "", 1),
                "path": str(session),
            },
            "ticks": _read_jsonl(session / "ticks.jsonl", limit=limit),
            "trades": _read_jsonl(session / "trades.jsonl", limit=limit),
            "signals": _read_jsonl(session / "signals.jsonl", limit=limit),
        }

    def _list_sessions(self) -> list[dict]:
        root = _session_root()
        sessions = []
        if not root.exists():
            return sessions
        for date_dir in sorted(root.iterdir(), reverse=True):
            if not date_dir.is_dir():
                continue
            for expiry_dir in sorted(date_dir.iterdir()):
                if expiry_dir.is_dir() and expiry_dir.name.startswith("expiry="):
                    sessions.append({
                        "date": date_dir.name,
                        "expiry": expiry_dir.name.replace("expiry=", "", 1),
                        "path": str(expiry_dir),
                        "mtime": expiry_dir.stat().st_mtime,
                    })
        return sorted(sessions, key=lambda item: item["mtime"], reverse=True)

    def _latest_session(self) -> Path | None:
        sessions = self._list_sessions()
        if not sessions:
            return None
        return Path(sessions[0]["path"])

    def _resolve_session(self, date: str, expiry: str) -> Path | None:
        if not date or not expiry:
            return None
        path = (_session_root() / date / f"expiry={expiry}").resolve()
        root = _session_root().resolve()
        if root not in path.parents:
            return None
        return path if path.exists() else None


def _session_root() -> Path:
    return Path(os.getenv("TRIWAVE_SESSION_BASE_DIR", "data/triwave_sessions")).resolve()


def _static_root() -> Path:
    configured = os.getenv("TRIWAVE_VIEWER_STATIC_DIR")
    if configured:
        return Path(configured).resolve()
    return (Path.cwd() / "tools" / "triwave_viewer").resolve()


def _positive_int(value: str, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        return default
    return parsed if parsed > 0 else default


def _read_jsonl(path: Path, limit: int) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
                if len(rows) > limit:
                    rows = rows[-limit:]
    except Exception:
        logger.warning("TRIWAVE_VIEWER_READ_FAILED | path=%s", path, exc_info=True)
    return rows
