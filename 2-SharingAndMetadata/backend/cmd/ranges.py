"""Inspect range stats or register a complete map from the range HTTP service."""

import argparse
import json
import urllib.request
from urllib.parse import urlsplit

from backend.sharing.metadata import integer, validate_ranges


def request(url, body=None):
    call = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(),
                                  headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(call, timeout=5) as response:
        return json.load(response)


def read_range(node_url, range_id):
    result = request(f"{node_url.rstrip('/')}/_range/metadata?rangeId={range_id}")
    if integer(result.get("rangeId"), "rangeId") != range_id:
        raise ValueError("range service returned the wrong range")
    replicas = result.get("replicas")
    if not isinstance(replicas, list) or not replicas:
        raise ValueError("range metadata must include replicas")
    addresses = {}
    for replica in replicas:
        node_id = integer(replica.get("nodeId"), "nodeId")
        url = urlsplit(replica.get("apiUrl", ""))
        if url.scheme != "http" or not url.hostname or url.path not in ("", "/") or url.query or url.fragment or url.username:
            raise ValueError("replica apiUrl must be an HTTP base URL without credentials")
        if node_id in addresses:
            raise ValueError("duplicate replica ID")
        addresses[node_id] = url.netloc
    # The range service's version is not the metadata Raft map's version.
    descriptor = dict(id=range_id, startKey=result.get("startKey"), endKey=result.get("endKey"),
                      replicaIds=list(addresses), leaderId=result.get("leaderNodeId", 0))
    return descriptor, addresses


def register(node_url, range_ids, metadata_url):
    descriptors, addresses = [], {}
    for range_id in range_ids:
        descriptor, replicas = read_range(node_url, integer(range_id, "rangeId"))
        descriptors.append(descriptor)
        for node_id, address in replicas.items():
            if node_id in addresses and addresses[node_id] != address:
                raise ValueError("ranges disagree on a replica's API address")
            addresses[node_id] = address
    validate_ranges(descriptors)  # Refuse incomplete maps before writing anything.
    base = metadata_url.rstrip('/')
    snapshot = request(base + "/api/metadata/ranges")
    updated = request(base + "/api/metadata/commands", dict(type="replace_ranges",
        expectedVersion=snapshot["version"], removeRangeIds=[r["id"] for r in snapshot["ranges"]],
        ranges=descriptors))
    return {"metadata": updated, "RANGE_NODES": ",".join(f"{i}={a}" for i, a in sorted(addresses.items()))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["stats", "register"])
    parser.add_argument("--node-url", required=True, help="range node HTTP base URL")
    parser.add_argument("--range-ids", required=True, help="comma-separated range IDs; register needs the full map")
    parser.add_argument("--metadata-url", default="http://localhost:8080")
    args = parser.parse_args()
    ids = [integer(int(value), "rangeId") for value in args.range_ids.split(',')]
    if args.action == "register":
        result = register(args.node_url, ids, args.metadata_url)
    else:
        result = [request(f"{args.node_url.rstrip('/')}/_range/stats?rangeId={i}") for i in ids]
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
