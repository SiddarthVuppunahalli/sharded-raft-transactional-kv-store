"""Start one durable metadata service replica."""

from __future__ import annotations

import base64
import json
import os
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from backend.sharing.metadata import MetadataError, MetadataStore, apply_command, decode_key, find_range
from backend.sharing.raft import RaftNode, serve_raft


def parse_addresses(name: str, value: str) -> dict[int, str]:
    result = {}
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        node, separator, address = item.partition("=")
        if not separator or not address:
            raise ValueError(f"{name} entries must use id=host:port")
        node_id = int(node)
        if node_id <= 0 or node_id in result:
            raise ValueError(f"{name} contains an invalid or duplicate node ID")
        if address.startswith(":"):
            address = f"127.0.0.1{address}"
        result[node_id] = address
    if not result:
        raise ValueError(f"{name} must contain at least one id=host:port entry")
    return result


def listen_address(value: str) -> tuple[str, int]:
    host, separator, port = value.rpartition(":")
    if not separator or not port:
        raise ValueError(f"invalid listen address {value!r}; expected host:port")
    return (host or "0.0.0.0", int(port))


def make_api_handler(node: RaftNode, store: MetadataStore, http_peers: dict[int, str], range_nodes: dict[int, str]):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.dispatch()

        def do_POST(self):
            self.dispatch()

        def do_PUT(self):
            self.dispatch()

        def do_OPTIONS(self):
            self.send_error(405)

        def log_message(self, *_args):
            return

        def dispatch(self):
            if self.path == "/healthz":
                self.send_bytes(200, b"ok\n", "text/plain; charset=utf-8")
                return
            if self.path.startswith("/api/") and not self.is_leader():
                self.forward_to_leader()
                return
            try:
                if self.path.startswith("/api/metadata/"):
                    self.metadata_request()
                elif self.path.startswith("/api/kv"):
                    self.kv_request()
                else:
                    self.send_json(404, {"error": "endpoint not found"})
            except (MetadataError, ValueError) as exc:
                message = str(exc)
                status = 409 if message.startswith("metadata version conflict") else 400
                self.send_json(status, {"error": message})
            except Exception as exc:
                self.send_json(503, {"error": str(exc)})

        def is_leader(self) -> bool:
            with node.lock:
                return node.role == "leader"

        def forward_to_leader(self):
            with node.lock:
                leader_id = node.leader_id
            address = http_peers.get(leader_id) if leader_id else None
            if not address:
                self.send_json(503, {"error": "metadata Raft leader is unknown"})
                return
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            request = urllib.request.Request(
                f"http://{address}{self.path}",
                data=body if self.command in ("POST", "PUT") else None,
                headers={"Content-Type": self.headers.get("Content-Type", "application/json")},
                method=self.command,
            )
            try:
                with urllib.request.urlopen(request, timeout=6) as response:
                    self.send_bytes(response.status, response.read(), response.headers.get("Content-Type", "application/octet-stream"))
            except urllib.error.HTTPError as response:
                self.send_bytes(response.code, response.read(), response.headers.get("Content-Type", "application/json"))
            except (OSError, urllib.error.URLError):
                self.send_json(503, {"error": "metadata leader is unavailable"})

        def metadata_request(self):
            path, _, query = self.path.partition("?")
            params = urllib.parse.parse_qs(query, keep_blank_values=True)
            if path == "/api/metadata/ranges":
                if self.command != "GET":
                    self.method_not_allowed("GET")
                    return
                node.read_barrier()
                self.send_json(200, store.snapshot())
                return
            if path == "/api/metadata/lookup":
                if self.command != "GET":
                    self.method_not_allowed("GET")
                    return
                if "key" not in params:
                    self.send_json(400, {"error": "key is required as base64url in the key query parameter"})
                    return
                key = decode_url_key(params["key"][0])
                node.read_barrier()
                snapshot = store.snapshot()
                descriptor = find_range(snapshot["ranges"], key)
                if descriptor is None:
                    self.send_json(404, {"error": "no range contains the key"})
                else:
                    self.send_json(200, {"version": snapshot["version"], "range": descriptor})
                return
            if path == "/api/metadata/commands":
                if self.command != "POST":
                    self.method_not_allowed("POST")
                    return
                command = self.read_json()
                snapshot = store.snapshot()
                # Catch malformed commands before logging them; version races are
                # still checked again by every replica while applying the entry.
                apply_command(snapshot["ranges"], snapshot["version"], command)
                node.propose(command)
                self.send_json(200, store.snapshot())
                return
            self.send_json(404, {"error": "metadata endpoint not found"})

        def kv_request(self):
            path, _, query = self.path.partition("?")
            params = urllib.parse.parse_qs(query, keep_blank_values=True)
            if path != "/api/kv":
                self.send_json(404, {"error": "endpoint not found"})
                return
            if self.command not in ("GET", "PUT"):
                self.method_not_allowed("GET, PUT")
                return
            if "key" not in params:
                self.send_json(400, {"error": "key is required as unpadded base64url in the key query parameter"})
                return
            key = decode_url_key(params["key"][0])
            operation = "get" if self.command == "GET" else "put"
            value = self.rfile.read((1 << 20) + 1) if operation == "put" else b""
            if len(value) > 1 << 20:
                self.send_json(413, {"error": "value body exceeds 1 MiB"})
                return
            for _attempt in range(3):
                node.read_barrier()
                snapshot = store.snapshot()
                descriptor = find_range(snapshot["ranges"], key)
                if descriptor is None:
                    self.send_json(404, {"error": "no range contains the key"})
                    return
                leader = descriptor["leaderId"]
                address = range_nodes.get(leader)
                if not address:
                    self.send_json(503, {"error": f"range {descriptor['id']} leader {leader} has no configured endpoint"})
                    return
                payload = {
                    "operation": operation,
                    "rangeId": descriptor["id"],
                    "key": base64.b64encode(key).decode(),
                    "value": base64.b64encode(value).decode(),
                    "metadataVersion": snapshot["version"],
                }
                request = urllib.request.Request(
                    f"http://{address}/_range/execute",
                    data=json.dumps(payload).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                try:
                    with urllib.request.urlopen(request, timeout=5) as response:
                        result = json.loads(response.read() or b"{}")
                except urllib.error.HTTPError as response:
                    result = json.loads(response.read() or b"{}")
                except TimeoutError:
                    self.send_json(504, {"error": f"range request to leader {leader} timed out"})
                    return
                except (OSError, urllib.error.URLError):
                    self.send_json(503, {"error": f"range leader {leader} is unavailable"})
                    return

                error = result.get("error")
                if error in ("stale_metadata", "not_leader"):
                    if error == "not_leader":
                        new_leader = int(result.get("leaderId", 0))
                        if new_leader == leader:
                            self.send_json(503, {"error": "range rejected its current leader record"})
                            return
                        if new_leader and new_leader not in descriptor["replicaIds"]:
                            self.send_json(503, {"error": "range reported a leader outside its replica list"})
                            return
                        try:
                            node.propose({
                                "type": "set_leader",
                                "expectedVersion": snapshot["version"],
                                "rangeId": descriptor["id"],
                                "leaderId": new_leader,
                            })
                        except ValueError:
                            pass  # Refresh the map; another router may have updated it.
                    continue
                if error:
                    self.send_json(503, {"error": str(error)})
                elif operation == "put":
                    self.send_bytes(204, b"", "application/octet-stream")
                elif not result.get("found", False):
                    self.send_json(404, {"error": "key does not exist"})
                else:
                    result_value = base64.b64decode(result.get("value", ""), validate=True)
                    self.send_bytes(200, result_value, "application/octet-stream")
                return
            self.send_json(503, {"error": "range routing retries exhausted"})

        def read_json(self):
            size = int(self.headers.get("Content-Length", "0"))
            if size > 1 << 20:
                raise ValueError("request body is too large")
            body = json.loads(self.rfile.read(size))
            if not isinstance(body, dict):
                raise ValueError("request body must be one JSON object")
            return body

        def method_not_allowed(self, allowed: str):
            body = json.dumps({"error": "method not allowed"}).encode()
            self.send_response(405)
            self.send_header("Allow", allowed)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def send_json(self, status: int, body: dict):
            self.send_bytes(status, json.dumps(body).encode(), "application/json")

        def send_bytes(self, status: int, body: bytes, content_type: str):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

    return Handler


def decode_url_key(value: str) -> bytes:
    if "=" in value:
        raise ValueError("key must be unpadded base64url")
    try:
        return base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except (ValueError, base64.binascii.Error) as exc:
        raise ValueError("key must be valid unpadded base64url") from exc


def run() -> None:
    node_id = int(os.environ.get("METADATA_NODE_ID", "1"))
    if node_id <= 0:
        raise ValueError("METADATA_NODE_ID must be a positive integer")
    raft_address = os.environ.get("METADATA_RAFT_ADDR", "127.0.0.1:9090")
    http_address = os.environ.get("METADATA_HTTP_ADDR", "127.0.0.1:8080")
    peers_value = os.environ.get("METADATA_PEERS", f"{node_id}={raft_address}")
    peers = parse_addresses("METADATA_PEERS", peers_value)
    if node_id not in peers:
        raise ValueError(f"METADATA_PEERS must include node {node_id}")
    http_peers_value = os.environ.get("METADATA_HTTP_PEERS")
    if http_peers_value:
        http_peers = parse_addresses("METADATA_HTTP_PEERS", http_peers_value)
    elif len(peers) == 1:
        http_peers = {node_id: http_address.replace(":", "127.0.0.1:", 1) if http_address.startswith(":") else http_address}
    else:
        raise ValueError("METADATA_HTTP_PEERS is required for clusters with multiple metadata replicas")
    range_nodes = parse_addresses("RANGE_NODES", os.environ["RANGE_NODES"]) if os.environ.get("RANGE_NODES") else {}

    store = MetadataStore(os.environ.get("METADATA_DATA_DIR", "./data/metadata"))
    node = RaftNode(node_id, peers, store)
    raft_server = serve_raft(node, listen_address(raft_address))
    node.start()
    http_server = ThreadingHTTPServer(
        listen_address(http_address), make_api_handler(node, store, http_peers, range_nodes)
    )
    http_server.daemon_threads = True
    print(f"Metadata node {node_id}: HTTP {http_address}; Raft {raft_address}", flush=True)
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        node.close()
        http_server.shutdown()
        raft_server.shutdown()
        store.close()


if __name__ == "__main__":
    run()
