"""Serve the separate frontend and proxy its /api requests to the backend."""

from __future__ import annotations

import os
import json
import urllib.error
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def make_handler(backend: str, timeout: float = 8):
    directory = Path(__file__).resolve().parent

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(directory), **kwargs)

        def end_headers(self):
            self.send_header("Cache-Control", "no-store")
            super().end_headers()

        def do_GET(self):
            if self.path.startswith("/api/"):
                self.proxy()
            else:
                super().do_GET()

        def do_PUT(self):
            self.proxy()

        def do_POST(self):
            self.proxy()

        def proxy(self):
            size = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(size) if size else None
            request = urllib.request.Request(
                backend + self.path,
                data=body,
                headers={
                    key: value
                    for key, value in self.headers.items()
                    if key.lower() in ("accept", "content-type")
                },
                method=self.command,
            )
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    status, content_type, payload = response.status, response.headers.get("Content-Type", "application/octet-stream"), response.read()
            except urllib.error.HTTPError as response:
                status, content_type, payload = response.code, response.headers.get("Content-Type", "application/json"), response.read()
            except (OSError, urllib.error.URLError) as exc:
                timed_out = isinstance(exc, TimeoutError) or isinstance(getattr(exc, "reason", None), TimeoutError)
                status = 504 if timed_out else 502
                message = "Backend request timed out. Try again." if timed_out else "Backend is unavailable. Check that it is running."
                content_type, payload = "application/json", json.dumps({"error": message}).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(payload)

    return Handler


def main() -> None:
    backend = os.environ.get("BACKEND_URL", "http://127.0.0.1:8080").rstrip("/")
    server = ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("FRONTEND_PORT", "5173"))), make_handler(backend))
    print(f"Frontend: http://127.0.0.1:{server.server_port} (API -> {backend})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
