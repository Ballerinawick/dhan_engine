import json
import logging
import os
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

logger = logging.getLogger(__name__)
SLOW_BROADCAST_MS = float(os.getenv("TRIWAVE_BROADCAST_SLOW_MS", "50") or 50)


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
        if parsed.path == "/api/live/stream":
            self._stream_live_session(query)
            return
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
        if parsed.path == "/api/session/health":
            session = self._stream_session(query)
            self._json(self._session_health(session, query))
            return
        if parsed.path == "/api/session/download":
            session = self._stream_session(query)
            self._download_session_file(session, query)
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
            return {"session": None, "ticks": [], "trades": [], "signals": [], "portfolio": []}
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
            "portfolio": _read_jsonl(session / "portfolio.jsonl", limit=limit),
        }

    def _session_health(self, session: Path | None, query: dict) -> dict:
        if session is None:
            return {"session": None, "status": "offline", "summary": {}, "feeds": [], "events": []}

        now = time.time()
        tick_limit = _positive_int((query.get("tick_limit") or ["2000"])[0], 2000)
        event_limit = _positive_int((query.get("event_limit") or ["50"])[0], 50)
        ticks = _read_jsonl(session / "ticks.jsonl", limit=tick_limit)
        trades = _read_jsonl(session / "trades.jsonl", limit=500)
        portfolio = _read_jsonl(session / "portfolio.jsonl", limit=100)
        signals = _read_jsonl(session / "signals.jsonl", limit=300)

        latest_by_feed: dict[str, dict] = {}
        for row in ticks:
            tick = row if isinstance(row, dict) else {}
            features = tick.get("features") if isinstance(tick.get("features"), dict) else {}
            raw = tick.get("raw") if isinstance(tick.get("raw"), dict) else {}
            index = str(tick.get("index") or features.get("index") or "").upper()
            stream = str(tick.get("stream") or features.get("stream") or "").upper()
            secid = tick.get("secid") or features.get("secid") or raw.get("security_id") or raw.get("SecurityId") or ""
            key = f"{index}_{stream}_{secid}".strip("_")
            if not key:
                continue
            ts = _number(tick.get("ts") or features.get("ts") or raw.get("ts"))
            ltp = _number(tick.get("ltp") or features.get("ltp") or raw.get("LTP") or raw.get("ltp"))
            current = latest_by_feed.get(key)
            if current is None or ts >= current.get("ts", 0):
                latest_by_feed[key] = {
                    "key": key,
                    "index": index,
                    "stream": stream,
                    "secid": secid,
                    "ts": ts,
                    "age_sec": max(0.0, now - ts) if ts else None,
                    "ltp": ltp,
                    "ticks": 1,
                }
            else:
                current["ticks"] = int(current.get("ticks", 1)) + 1

        feeds = sorted(latest_by_feed.values(), key=lambda item: (item.get("index") or "", item.get("stream") or ""))
        stale_feeds = [feed for feed in feeds if feed.get("age_sec") is not None and feed["age_sec"] > 20]
        latest_portfolio = _last_json_object(portfolio)
        latest_trade_rows = [_last_json_object([row]) for row in trades]
        latest_trade_rows = [row for row in latest_trade_rows if row]
        net = sum(_number((row.get("trade") or row).get("net_pnl")) for row in latest_trade_rows)
        open_positions = 0
        if latest_portfolio:
            p = latest_portfolio.get("portfolio") if isinstance(latest_portfolio.get("portfolio"), dict) else latest_portfolio
            open_positions = int(_number(p.get("open_positions") or len(p.get("positions") or [])))

        events = _recent_health_events(trades, signals, stale_feeds, event_limit)
        status = "healthy"
        if not feeds:
            status = "waiting"
        if stale_feeds:
            status = "stale"
        if any(event.get("level") == "error" for event in events):
            status = "error"

        return {
            "session": {
                "date": session.parent.name,
                "expiry": session.name.replace("expiry=", "", 1),
                "path": str(session),
            },
            "status": status,
            "summary": {
                "tick_rows_loaded": len(ticks),
                "feeds": len(feeds),
                "stale_feeds": len(stale_feeds),
                "trades": len(latest_trade_rows),
                "net_pnl": net,
                "open_positions": open_positions,
                "portfolio_snapshots": len(portfolio),
                "last_file_update_sec": _session_file_age_sec(session, now),
            },
            "feeds": feeds,
            "events": events,
        }

    def _download_session_file(self, session: Path | None, query: dict) -> None:
        if session is None:
            self._json({"error": "session not found"}, HTTPStatus.NOT_FOUND)
            return
        file_key = (query.get("file") or [""])[0].strip().lower()
        allowed = {
            "ticks": "ticks.jsonl",
            "trades": "trades.jsonl",
            "signals": "signals.jsonl",
            "portfolio": "portfolio.jsonl",
        }
        filename = allowed.get(file_key)
        if not filename:
            self._json({"error": "invalid file"}, HTTPStatus.BAD_REQUEST)
            return
        path = (session / filename).resolve()
        if session.resolve() not in path.parents or not path.exists():
            self._json({"error": "file not found"}, HTTPStatus.NOT_FOUND)
            return
        body = path.read_bytes()
        download_name = f"{session.parent.name}_{session.name.replace('expiry=', '')}_{filename}"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/jsonl; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Disposition", f'attachment; filename="{download_name}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _stream_live_session(self, query: dict) -> None:
        session = self._stream_session(query)
        if session is None:
            self._json({"error": "session not found"}, HTTPStatus.NOT_FOUND)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        files = {
            "tick": session / "ticks.jsonl",
            "trade": session / "trades.jsonl",
            "signal": session / "signals.jsonl",
            "portfolio": session / "portfolio.jsonl",
        }
        offsets = {
            name: path.stat().st_size if path.exists() else 0
            for name, path in files.items()
        }
        heartbeat_at = time.time()
        while True:
            try:
                sent = False
                for event_name, path in files.items():
                    for row, offset in _read_jsonl_since(path, offsets.get(event_name, 0)):
                        offsets[event_name] = offset
                        self._sse(event_name, row)
                        sent = True
                if time.time() - heartbeat_at >= 15:
                    heartbeat_at = time.time()
                    self._sse("heartbeat", {"ts": time.time()})
                    sent = True
                if sent:
                    self.wfile.flush()
                time.sleep(1.0)
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception:
                logger.warning("TRIWAVE_VIEWER_STREAM_FAILED | session=%s", session, exc_info=True)
                return

    def _sse(self, event_name: str, payload: dict) -> None:
        start = time.monotonic()
        body = f"event: {event_name}\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
        self.wfile.write(body.encode("utf-8"))
        duration_ms = (time.monotonic() - start) * 1000.0
        if duration_ms > SLOW_BROADCAST_MS:
            logger.warning(
                "BROADCAST_SLOW | event=%s | duration_ms=%.1f | threshold_ms=%.1f",
                event_name,
                duration_ms,
                SLOW_BROADCAST_MS,
            )

    def _stream_session(self, query: dict) -> Path | None:
        date = (query.get("date") or [""])[0]
        expiry = (query.get("expiry") or [""])[0]
        if date and expiry:
            return self._resolve_session(date, expiry)
        return self._latest_session()

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


def _read_jsonl_since(path: Path, offset: int) -> list[tuple[dict, int]]:
    if not path.exists():
        return []
    rows = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            handle.seek(max(0, int(offset or 0)))
            while True:
                line = handle.readline()
                if not line:
                    break
                next_offset = handle.tell()
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append((json.loads(line), next_offset))
                except json.JSONDecodeError:
                    continue
    except Exception:
        logger.warning("TRIWAVE_VIEWER_TAIL_FAILED | path=%s", path, exc_info=True)
    return rows


def _number(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _last_json_object(rows: list[dict]) -> dict:
    for row in reversed(rows or []):
        if isinstance(row, dict):
            return row
    return {}


def _session_file_age_sec(session: Path, now: float) -> float | None:
    mtimes = []
    for filename in ("ticks.jsonl", "trades.jsonl", "signals.jsonl", "portfolio.jsonl"):
        path = session / filename
        if path.exists():
            mtimes.append(path.stat().st_mtime)
    if not mtimes:
        return None
    return max(0.0, now - max(mtimes))


def _recent_health_events(trades: list[dict], signals: list[dict], stale_feeds: list[dict], limit: int) -> list[dict]:
    events = []
    for feed in stale_feeds:
        events.append({
            "level": "warning",
            "time": "",
            "message": f"Stale feed {feed.get('index')} {feed.get('stream')} age {feed.get('age_sec', 0):.0f}s",
        })
    for row in reversed(trades or []):
        trade = row.get("trade") if isinstance(row.get("trade"), dict) else row
        reason = str(trade.get("exit_reason") or "")
        if "STALE" in reason or "ADVERSE" in reason or "TIME_LOSS" in reason or "ERROR" in reason:
            level = "error" if "ERROR" in reason else "warning"
            events.append({
                "level": level,
                "time": str(row.get("time") or ""),
                "message": f"{trade.get('tag', 'TRADE')} exit {reason.replace('TRI_WAVE_V2_EXIT:', '')}",
            })
        if len(events) >= limit:
            return events[:limit]
    for row in reversed(signals or []):
        text = json.dumps(row, ensure_ascii=False)
        upper = text.upper()
        if any(key in upper for key in ("ERROR", "WARNING", "STALE", "BLOCKED", "FAILED")):
            events.append({
                "level": "warning",
                "time": str(row.get("time") or ""),
                "message": text[:220],
            })
        if len(events) >= limit:
            return events[:limit]
    return events[:limit]
