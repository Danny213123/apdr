from __future__ import annotations

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
import json
import mimetypes
import threading
import urllib.parse

from .service import BenchmarkService
from .state import AppState


class BenchmarkHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        request_handler: type[BaseHTTPRequestHandler],
        service: BenchmarkService,
        static_root: Path,
        api_only: bool,
    ) -> None:
        super().__init__(server_address, request_handler)
        self.service = service
        self.static_root = static_root
        self.api_only = api_only


class BenchmarkRequestHandler(BaseHTTPRequestHandler):
    server: BenchmarkHTTPServer

    def handle_one_request(self) -> None:
        try:
            super().handle_one_request()
        except (BrokenPipeError, ConnectionResetError):
            return

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/bootstrap":
            self._send_json(HTTPStatus.OK, self.server.service.bootstrap())
            return
        if path == "/api/status":
            self._send_json(HTTPStatus.OK, self.server.service.status())
            return
        if path == "/api/runs":
            self._send_json(HTTPStatus.OK, {"runs": self.server.service.runs()})
            return
        if path.startswith("/api/runs/"):
            run_id = path.rsplit("/", 1)[-1]
            self._send_json(HTTPStatus.OK, self.server.service.load_run(run_id))
            return
        if path == "/api/loadouts":
            self._send_json(HTTPStatus.OK, {"loadouts": self.server.service.loadouts()})
            return
        if path == "/api/models":
            self._send_json(HTTPStatus.OK, {"modelConfigs": self.server.service.model_configs()})
            return
        if path.startswith("/api/"):
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "API endpoint not found."})
            return
        self._serve_static(path)

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        payload = self._read_json_body()

        try:
            if path == "/api/preview":
                self._send_json(HTTPStatus.OK, self.server.service.preview(payload))
                return
            if path == "/api/benchmark/start":
                self._send_json(HTTPStatus.OK, self.server.service.start_benchmark(payload))
                return
            if path == "/api/benchmark/stop":
                self._send_json(HTTPStatus.OK, self.server.service.stop_benchmark())
                return
            if path.startswith("/api/runs/") and path.endswith("/resume"):
                run_id = path.split("/api/runs/", 1)[1].rsplit("/resume", 1)[0].strip("/")
                self._send_json(HTTPStatus.OK, self.server.service.resume_run(run_id))
                return
            if path == "/api/models/refresh":
                tool = str(payload.get("tool") or "").strip()
                base_url = str(payload.get("base_url") or payload.get("baseUrl") or "").strip()
                self._send_json(HTTPStatus.OK, self.server.service.refresh_models(tool, base_url))
                return
            if path == "/api/models/save":
                self._send_json(HTTPStatus.OK, self.server.service.save_model_configs(payload))
                return
            if path == "/api/loadouts/save":
                self._send_json(HTTPStatus.OK, self.server.service.save_loadout(payload))
                return
            if path == "/api/doctor/run":
                self._send_json(HTTPStatus.OK, self.server.service.start_doctor(payload))
                return
            if path == "/api/doctor/fix":
                self._send_json(HTTPStatus.OK, self.server.service.start_doctor_fix(payload))
                return
            if path == "/api/server/shutdown":
                self._send_json(HTTPStatus.OK, {"ok": True, "message": "Server is shutting down."})
                threading.Thread(target=self.server.shutdown, daemon=True).start()
                return
        except ValueError as exc:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return
        except RuntimeError as exc:
            self._send_json(HTTPStatus.CONFLICT, {"error": str(exc)})
            return
        except Exception as exc:
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(exc)})
            return

        self._send_json(HTTPStatus.NOT_FOUND, {"error": "API endpoint not found."})

    def do_DELETE(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path.startswith("/api/loadouts/"):
            slug = path.rsplit("/", 1)[-1]
            self._send_json(HTTPStatus.OK, self.server.service.delete_loadout(slug))
            return
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "API endpoint not found."})

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            return {}

    def _send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            return

    def _serve_static(self, request_path: str) -> None:
        if self.server.api_only:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "Static frontend is disabled in api-only mode."})
            return

        normalized = urllib.parse.unquote(request_path or "/")
        candidate = (self.server.static_root / normalized.lstrip("/")).resolve()
        static_root = self.server.static_root.resolve()
        index_path = (self.server.static_root / "index.html").resolve()

        if normalized in {"", "/"}:
            candidate = index_path

        try:
            candidate.relative_to(static_root)
        except ValueError:
            self.send_error(HTTPStatus.FORBIDDEN)
            return

        if not candidate.exists() or candidate.is_dir():
            if "." not in Path(normalized).name:
                candidate = index_path
            else:
                self.send_error(HTTPStatus.NOT_FOUND)
                return

        content_type, _encoding = mimetypes.guess_type(str(candidate))
        try:
            data = candidate.read_bytes()
        except OSError:
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type or "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            return


def run_server(
    host: str = "127.0.0.1",
    port: int = 4173,
    api_only: bool = False,
    state: AppState | None = None,
) -> None:
    repo_root = (state.repo_root if state else AppState().repo_root).resolve()
    static_root = repo_root / "web"
    service = BenchmarkService(state or AppState(repo_root))
    server = BenchmarkHTTPServer((host, port), BenchmarkRequestHandler, service, static_root, api_only)
    bound_port = int(server.server_address[1])
    service.set_server_context(host, bound_port, api_only)
    local_url = service.bootstrap()["app"]["server"]["localUrl"]
    network_url = service.bootstrap()["app"]["server"]["networkUrl"]
    print(f"FSE AIWare Bench web app listening on {local_url}")
    if network_url:
        print(f"Network URL: {network_url}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
