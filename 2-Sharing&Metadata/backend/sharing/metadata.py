"""Durable metadata state machine for the sharing service."""

from __future__ import annotations

import base64
import json
import sqlite3
import threading
from pathlib import Path


class MetadataError(ValueError):
    """A metadata command is invalid or uses a stale version."""


class MetadataStore:
    """Stores the Raft log and applied metadata in a synchronous SQLite DB."""

    def __init__(self, directory: str | Path):
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path / "metadata.sqlite3", check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS raft_meta (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                current_term INTEGER NOT NULL,
                voted_for INTEGER,
                commit_index INTEGER NOT NULL
            );
            INSERT OR IGNORE INTO raft_meta VALUES (1, 0, NULL, 0);
            CREATE TABLE IF NOT EXISTS raft_log (
                log_index INTEGER PRIMARY KEY,
                term INTEGER NOT NULL,
                entry TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metadata_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL,
                last_applied INTEGER NOT NULL,
                ranges TEXT NOT NULL
            );
            INSERT OR IGNORE INTO metadata_state VALUES (1, 0, 0, '[]');
            """
        )

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def raft_state(self) -> tuple[int, int | None, int, int, int]:
        with self.lock:
            term, voted_for, commit = self.db.execute(
                "SELECT current_term, voted_for, commit_index FROM raft_meta WHERE id=1"
            ).fetchone()
            row = self.db.execute(
                "SELECT log_index, term FROM raft_log ORDER BY log_index DESC LIMIT 1"
            ).fetchone()
            return term, voted_for, commit, row[0] if row else 0, row[1] if row else 0

    def update_term_vote(self, term: int, voted_for: int | None) -> None:
        with self.lock, self.db:
            self.db.execute(
                "UPDATE raft_meta SET current_term=?, voted_for=? WHERE id=1",
                (term, voted_for),
            )

    def log_term(self, index: int) -> int | None:
        if index == 0:
            return 0
        with self.lock:
            row = self.db.execute(
                "SELECT term FROM raft_log WHERE log_index=?", (index,)
            ).fetchone()
            return row[0] if row else None

    def log_entries(self, start: int, limit: int = 256) -> list[dict]:
        with self.lock:
            rows = self.db.execute(
                "SELECT log_index, term, entry FROM raft_log "
                "WHERE log_index>=? ORDER BY log_index LIMIT ?",
                (start, limit),
            ).fetchall()
        return [
            {"index": index, "term": term, "entry": json.loads(entry)}
            for index, term, entry in rows
        ]

    def append_local(self, term: int, entry: dict) -> int:
        with self.lock, self.db:
            index = self.db.execute(
                "SELECT COALESCE(MAX(log_index), 0)+1 FROM raft_log"
            ).fetchone()[0]
            self.db.execute(
                "INSERT INTO raft_log VALUES (?, ?, ?)",
                (index, term, json.dumps(entry, separators=(",", ":"))),
            )
            return index

    def append_from_leader(
        self, previous_index: int, previous_term: int, entries: list[dict]
    ) -> tuple[bool, int]:
        with self.lock, self.db:
            if previous_index:
                row = self.db.execute(
                    "SELECT term FROM raft_log WHERE log_index=?", (previous_index,)
                ).fetchone()
                if not row or row[0] != previous_term:
                    last = self.db.execute(
                        "SELECT COALESCE(MAX(log_index), 0) FROM raft_log"
                    ).fetchone()[0]
                    return False, last + 1

            for item in entries:
                index, term = int(item["index"]), int(item["term"])
                existing = self.db.execute(
                    "SELECT term FROM raft_log WHERE log_index=?", (index,)
                ).fetchone()
                if existing and existing[0] == term:
                    continue
                commit = self.db.execute(
                    "SELECT commit_index FROM raft_meta WHERE id=1"
                ).fetchone()[0]
                if index <= commit:
                    return False, commit + 1
                self.db.execute("DELETE FROM raft_log WHERE log_index>=?", (index,))
                self.db.execute(
                    "INSERT INTO raft_log VALUES (?, ?, ?)",
                    (
                        index,
                        term,
                        json.dumps(item["entry"], separators=(",", ":")),
                    ),
                )
            return True, previous_index + len(entries)

    def set_commit_index(self, index: int) -> None:
        with self.lock, self.db:
            self.db.execute(
                "UPDATE raft_meta SET commit_index=MAX(commit_index, ?) WHERE id=1",
                (index,),
            )

    def apply_committed(self) -> dict[int, str | None]:
        """Apply committed commands in order and persist each resulting snapshot."""
        results: dict[int, str | None] = {}
        with self.lock, self.db:
            commit = self.db.execute(
                "SELECT commit_index FROM raft_meta WHERE id=1"
            ).fetchone()[0]
            version, applied, ranges_json = self.db.execute(
                "SELECT version, last_applied, ranges FROM metadata_state WHERE id=1"
            ).fetchone()
            ranges = json.loads(ranges_json)
            for index in range(applied + 1, commit + 1):
                row = self.db.execute(
                    "SELECT entry FROM raft_log WHERE log_index=?", (index,)
                ).fetchone()
                if not row:
                    raise RuntimeError(f"committed Raft entry {index} is missing")
                entry = json.loads(row[0])
                error = None
                if entry.get("kind") == "command":
                    try:
                        ranges = apply_command(ranges, version, entry["command"])
                        version += 1
                    except MetadataError as exc:
                        error = str(exc)
                results[index] = error
                self.db.execute(
                    "UPDATE metadata_state SET version=?, last_applied=?, ranges=? "
                    "WHERE id=1",
                    (version, index, json.dumps(ranges, separators=(",", ":"))),
                )
        return results

    def snapshot(self) -> dict:
        with self.lock:
            version, ranges = self.db.execute(
                "SELECT version, ranges FROM metadata_state WHERE id=1"
            ).fetchone()
            return {"version": version, "ranges": json.loads(ranges)}


def decode_key(value: str | None) -> bytes:
    if value in (None, ""):
        return b""
    try:
        return base64.b64decode(value, validate=True)
    except (TypeError, ValueError, base64.binascii.Error) as exc:
        raise MetadataError("range boundaries must be standard base64") from exc


def encode_key(value: bytes) -> str | None:
    return base64.b64encode(value).decode("ascii") if value else None


def prepare_range(item: dict) -> dict:
    try:
        range_id = int(item["id"])
        start = decode_key(item.get("startKey"))
        end = decode_key(item.get("endKey"))
        replica_ids = [int(value) for value in item["replicaIds"]]
        leader_id = int(item.get("leaderId", 0))
    except (KeyError, TypeError, ValueError) as exc:
        raise MetadataError("range requires id, replicaIds, and valid boundaries") from exc
    if range_id <= 0 or not replica_ids or any(value <= 0 for value in replica_ids):
        raise MetadataError("range and replica IDs must be greater than zero")
    if len(set(replica_ids)) != len(replica_ids):
        raise MetadataError(f"range {range_id} has duplicate replica IDs")
    if leader_id and leader_id not in replica_ids:
        raise MetadataError(f"range {range_id} leader is not one of its replicas")
    if end and start >= end:
        raise MetadataError(f"range {range_id} has an invalid key interval")
    return {
        "id": range_id,
        "startKey": encode_key(start),
        "endKey": encode_key(end),
        "replicaIds": replica_ids,
        "leaderId": leader_id,
        "_start": start,
        "_end": end,
    }


def validate_ranges(ranges: list[dict]) -> list[dict]:
    if not ranges:
        raise MetadataError("range map must contain at least one range")
    prepared = [prepare_range(item) for item in ranges]
    if len({item["id"] for item in prepared}) != len(prepared):
        raise MetadataError("range IDs must be unique")
    prepared.sort(key=lambda item: item["_start"])
    if prepared[0]["_start"]:
        raise MetadataError("range map must start at the beginning of the keyspace")
    for left, right in zip(prepared, prepared[1:]):
        if not left["_end"] or left["_end"] != right["_start"]:
            raise MetadataError("ranges must meet exactly without gaps or overlaps")
    if prepared[-1]["_end"]:
        raise MetadataError("range map must extend to the end of the keyspace")
    return [public_range(item) for item in prepared]


def public_range(item: dict) -> dict:
    return {
        "id": item["id"],
        "startKey": item.get("startKey"),
        "endKey": item.get("endKey"),
        "replicaIds": item["replicaIds"],
        "leaderId": item["leaderId"],
    }


def apply_command(ranges: list[dict], version: int, command: dict) -> list[dict]:
    expected = command.get("expectedVersion")
    if not isinstance(expected, int) or expected != version:
        raise MetadataError(f"metadata version conflict: expected {expected}, current {version}")

    kind = command.get("type")
    if kind == "replace_ranges":
        remove_ids = command.get("removeRangeIds", [])
        if len(remove_ids) != len(set(remove_ids)) or any(int(value) <= 0 for value in remove_ids):
            raise MetadataError("removeRangeIds must contain unique positive IDs")
        remaining = {item["id"]: item for item in ranges}
        for range_id in remove_ids:
            if int(range_id) not in remaining:
                raise MetadataError(f"range {range_id} does not exist")
            del remaining[int(range_id)]
        additions = command.get("ranges", [])
        for item in additions:
            descriptor = prepare_range(item)
            if descriptor["id"] in remaining:
                raise MetadataError(f"duplicate range ID {descriptor['id']}")
            remaining[descriptor["id"]] = public_range(descriptor)
        return validate_ranges(list(remaining.values()))

    range_id = int(command.get("rangeId", 0))
    updated = [dict(item) for item in ranges]
    for index, item in enumerate(updated):
        if item["id"] != range_id:
            continue
        if kind == "set_leader":
            item["leaderId"] = int(command.get("leaderId", 0))
        elif kind == "set_replicas":
            item["replicaIds"] = [int(value) for value in command.get("replicaIds", [])]
            item["leaderId"] = int(command.get("leaderId", 0))
        else:
            raise MetadataError(f"unknown metadata command type {kind!r}")
        checked = prepare_range(item)
        updated[index] = public_range(checked)
        return updated
    raise MetadataError(f"range {range_id} does not exist")


def find_range(ranges: list[dict], key: bytes) -> dict | None:
    for item in ranges:
        start = decode_key(item.get("startKey"))
        end = decode_key(item.get("endKey"))
        if key >= start and (not end or key < end):
            return item
    return None
