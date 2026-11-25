"""Small HTTP-transport Raft group for the metadata state machine."""

from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .metadata import MetadataStore


def make_raft_handler(node: "RaftNode"):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                size = int(self.headers.get("Content-Length", "0"))
                body = json.loads(self.rfile.read(size) or b"{}")
                status, response = node.handle_rpc(self.path, body)
            except Exception as exc:
                status, response = 400, {"error": str(exc)}
            payload = json.dumps(response).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_args):
            return

    return Handler


class RaftNode:
    """Raft elections, log replication, and majority-backed read barriers."""

    def __init__(self, node_id: int, peers: dict[int, str], store: MetadataStore):
        self.id = node_id
        self.peers = peers
        self.store = store
        self.majority = len(peers) // 2 + 1
        self.lock = threading.RLock()
        self.proposal_lock = threading.Lock()
        self.round_lock = threading.Lock()
        self.stop_event = threading.Event()
        term, voted_for, commit, last_index, _ = store.raft_state()
        self.term = term
        self.voted_for = voted_for
        self.commit_index = commit
        self.role = "follower"
        self.leader_id: int | None = None
        self.match_index = {peer_id: 0 for peer_id in peers}
        self.match_index[node_id] = last_index
        self.next_index = {peer_id: last_index + 1 for peer_id in peers if peer_id != node_id}
        self.election_deadline = 0.0
        self.last_heartbeat = 0.0
        self.results: dict[int, str | None] = {}
        self._reset_election_deadline()
        store.apply_committed()

    def start(self) -> threading.Thread:
        thread = threading.Thread(target=self._run, name=f"metadata-raft-{self.id}", daemon=True)
        thread.start()
        return thread

    def close(self) -> None:
        self.stop_event.set()

    def _reset_election_deadline(self) -> None:
        # A stable per-node offset reduces simultaneous elections in local demos.
        self.election_deadline = time.monotonic() + 0.45 + (self.id % 7) * 0.075 + random.random() * 0.12

    def _run(self) -> None:
        while not self.stop_event.wait(0.05):
            with self.lock:
                role = self.role
                due = time.monotonic() >= self.election_deadline
                heartbeat_due = time.monotonic() - self.last_heartbeat >= 0.12
            if role == "leader":
                if heartbeat_due:
                    self.last_heartbeat = time.monotonic()
                    self.replicate_round()
            elif due:
                self._start_election()

    def _start_election(self) -> None:
        with self.lock:
            self.term += 1
            election_term = self.term
            self.role = "candidate"
            self.leader_id = None
            self.voted_for = self.id
            self.store.update_term_vote(self.term, self.id)
            self._reset_election_deadline()
            _, _, _, last_index, _ = self.store.raft_state()
            last_term = self.store.log_term(last_index) or 0

        votes = 1
        request = {
            "term": election_term,
            "candidateId": self.id,
            "lastLogIndex": last_index,
            "lastLogTerm": last_term,
        }
        others = [peer for peer in self.peers if peer != self.id]
        with ThreadPoolExecutor(max_workers=max(1, len(others))) as pool:
            replies = list(pool.map(lambda peer: self._rpc(peer, "/_raft/vote", request), others))
        for reply in replies:
            if not reply:
                continue
            reply_term = int(reply.get("term", 0))
            if reply_term > election_term:
                self._step_down(reply_term)
                return
            if reply.get("voteGranted"):
                votes += 1

        with self.lock:
            if self.role != "candidate" or self.term != election_term or votes < self.majority:
                return
            self.role = "leader"
            self.leader_id = self.id
            self.match_index = {peer_id: 0 for peer_id in self.peers}
            last_index = self.store.raft_state()[3]
            self.match_index[self.id] = last_index
            self.next_index = {peer_id: last_index + 1 for peer_id in self.peers if peer_id != self.id}
            self.store.append_local(self.term, {"kind": "noop"})
            self.match_index[self.id] = last_index + 1
            self.last_heartbeat = 0.0
        self.replicate_round()

    def _step_down(self, term: int) -> None:
        with self.lock:
            if term > self.term:
                self.term = term
                self.voted_for = None
                self.store.update_term_vote(term, None)
            self.role = "follower"
            self.leader_id = None
            self._reset_election_deadline()

    def handle_rpc(self, path: str, body: dict) -> tuple[int, dict]:
        if path == "/_raft/vote":
            return 200, self._request_vote(body)
        if path == "/_raft/append":
            return 200, self._append_entries(body)
        return 404, {"error": "unknown Raft RPC"}

    def _request_vote(self, request: dict) -> dict:
        candidate_term = int(request["term"])
        candidate_id = int(request["candidateId"])
        with self.lock:
            if candidate_term > self.term:
                self.term = candidate_term
                self.voted_for = None
                self.role = "follower"
                self.leader_id = None
                self.store.update_term_vote(self.term, None)
            if candidate_term < self.term:
                return {"term": self.term, "voteGranted": False}

            _, _, _, last_index, _ = self.store.raft_state()
            last_term = self.store.log_term(last_index) or 0
            up_to_date = (int(request["lastLogTerm"]), int(request["lastLogIndex"])) >= (last_term, last_index)
            can_vote = self.voted_for in (None, candidate_id)
            granted = can_vote and up_to_date
            if granted:
                self.voted_for = candidate_id
                self.store.update_term_vote(self.term, candidate_id)
                self._reset_election_deadline()
            return {"term": self.term, "voteGranted": granted}

    def _append_entries(self, request: dict) -> dict:
        leader_term = int(request["term"])
        leader_id = int(request["leaderId"])
        with self.lock:
            if leader_term < self.term:
                return {"term": self.term, "success": False, "matchIndex": 0}
            if leader_term > self.term:
                self.term = leader_term
                self.voted_for = None
                self.store.update_term_vote(self.term, None)
            self.role = "follower"
            self.leader_id = leader_id
            self._reset_election_deadline()

            success, match_index = self.store.append_from_leader(
                int(request["previousIndex"]),
                int(request["previousTerm"]),
                request.get("entries", []),
            )
            if success:
                commit = min(int(request.get("leaderCommit", 0)), match_index)
                self.store.set_commit_index(commit)
                self.commit_index = max(self.commit_index, commit)
        if success:
            self.store.apply_committed()
        return {"term": self.term, "success": success, "matchIndex": match_index}

    def _rpc(self, peer_id: int, path: str, request: dict) -> dict | None:
        address = self.peers.get(peer_id)
        if not address:
            return None
        url = f"http://{address}{path}"
        data = json.dumps(request, separators=(",", ":")).encode()
        call = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(call, timeout=0.45) as response:
                return json.loads(response.read())
        except (OSError, ValueError, urllib.error.URLError):
            return None

    def _replicate_peer(self, peer_id: int) -> bool:
        for _ in range(8):
            with self.lock:
                if self.role != "leader":
                    return False
                term = self.term
                next_index = self.next_index.get(peer_id, 1)
                previous_index = next_index - 1
                previous_term = self.store.log_term(previous_index)
                if previous_term is None:
                    self.next_index[peer_id] = max(1, next_index - 1)
                    continue
                entries = self.store.log_entries(next_index)
                request = {
                    "term": term,
                    "leaderId": self.id,
                    "previousIndex": previous_index,
                    "previousTerm": previous_term,
                    "entries": entries,
                    "leaderCommit": self.commit_index,
                }
            reply = self._rpc(peer_id, "/_raft/append", request)
            if not reply:
                return False
            if int(reply.get("term", 0)) > term:
                self._step_down(int(reply["term"]))
                return False
            with self.lock:
                if self.role != "leader" or self.term != term:
                    return False
                if reply.get("success"):
                    matched = int(reply.get("matchIndex", previous_index + len(entries)))
                    self.match_index[peer_id] = max(self.match_index.get(peer_id, 0), matched)
                    self.next_index[peer_id] = matched + 1
                    return True
                conflict = int(reply.get("matchIndex", 0))
                self.next_index[peer_id] = max(1, min(next_index - 1, conflict + 1))
        return False

    def replicate_round(self) -> int:
        if not self.round_lock.acquire(timeout=5):
            return 0
        try:
            with self.lock:
                if self.role != "leader":
                    return 0
                peers = [peer for peer in self.peers if peer != self.id]
            with ThreadPoolExecutor(max_workers=max(1, len(peers))) as pool:
                outcomes = list(pool.map(self._replicate_peer, peers))
            acknowledgments = 1 + sum(outcomes)
            with self.lock:
                if self.role != "leader":
                    return 0
                last_index = self.store.raft_state()[3]
                matches = {self.id: last_index, **self.match_index}
                new_commit = self.commit_index
                for index in range(last_index, self.commit_index, -1):
                    if self.store.log_term(index) == self.term:
                        replicated = sum(1 for peer in self.peers if matches.get(peer, 0) >= index)
                        if replicated >= self.majority:
                            new_commit = index
                            break
                if new_commit > self.commit_index:
                    self.commit_index = new_commit
                    self.store.set_commit_index(new_commit)
            if new_commit > 0:
                self.results.update(self.store.apply_committed())
                # Notify replicas that the newly replicated entries are committed.
                if acknowledgments >= self.majority:
                    with ThreadPoolExecutor(max_workers=max(1, len(peers))) as pool:
                        list(pool.map(self._replicate_peer, peers))
            return acknowledgments
        finally:
            self.round_lock.release()

    def read_barrier(self) -> None:
        with self.lock:
            if self.role != "leader":
                raise RuntimeError("metadata node is not the Raft leader")
            term = self.term
        acknowledgments = self.replicate_round()
        with self.lock:
            current_term_committed = any(
                self.store.log_term(index) == term
                for index in range(self.commit_index, 0, -1)
            )
            if self.role != "leader" or self.term != term:
                raise RuntimeError("metadata leadership changed during read")
            if acknowledgments < self.majority or not current_term_committed:
                raise RuntimeError("metadata Raft majority is unavailable")

    def propose(self, command: dict) -> None:
        with self.proposal_lock:
            with self.lock:
                if self.role != "leader":
                    raise RuntimeError("metadata node is not the Raft leader")
                index = self.store.append_local(self.term, {"kind": "command", "command": command})
                self.match_index[self.id] = index
            acknowledgments = self.replicate_round()
            with self.lock:
                if acknowledgments < self.majority or self.commit_index < index:
                    raise RuntimeError("metadata Raft majority is unavailable; command was not committed")
                error = self.results.get(index)
                if error:
                    raise ValueError(error)


def serve_raft(node: RaftNode, address: tuple[str, int]) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(address, make_raft_handler(node))
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, name=f"metadata-raft-http-{node.id}", daemon=True)
    thread.start()
    return server
