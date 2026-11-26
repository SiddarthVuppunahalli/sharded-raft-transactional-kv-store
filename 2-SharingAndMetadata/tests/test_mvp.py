import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from http.server import ThreadingHTTPServer
from backend.sharing.metadata import MetadataStore, MetadataError, apply_command, validate_ranges, find_range
from backend.sharing.raft import RaftNode, serve_raft


def descriptor(i=1, start=None, end=None):
    return dict(id=i, startKey=start, endKey=end, replicaIds=[1, 2, 3], leaderId=1)


class MVPTests(unittest.TestCase):
    def test_timestamps_are_unique_and_survive_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(directory)
            try:
                node = RaftNode(1, {1: '127.0.0.1:1'}, store)
                node._start_election()
                with ThreadPoolExecutor(max_workers=4) as pool:
                    timestamps = list(pool.map(lambda _: node.propose({'type': 'next_timestamp'}), range(12)))
                self.assertEqual(sorted(timestamps), list(range(1, 13)))
                self.assertEqual(store.snapshot()['version'], 0)
            finally:
                store.close()
            restored = MetadataStore(directory)
            try:
                node = RaftNode(1, {1: '127.0.0.1:1'}, restored)
                node._start_election()
                self.assertEqual(node.propose({'type': 'next_timestamp'}), 13)
            finally:
                restored.close()

    def test_boundaries_and_invalid_maps(self):
        ranges = validate_ranges([descriptor(end='m'), descriptor(2, 'm')])
        self.assertEqual(find_range(ranges, 'l')['id'], 1)
        self.assertEqual(find_range(ranges, 'm')['id'], 2)
        for invalid in ([], [descriptor(end='m')], [descriptor(), descriptor(2)],
                        [descriptor(end='n'), descriptor(2, 'm')]):
            with self.subTest(invalid=invalid), self.assertRaises(MetadataError):
                validate_ranges(invalid)

    def test_version_and_membership_validation(self):
        ranges = [descriptor()]
        for command in (
            dict(type='set_leader', expectedVersion=2, rangeId=1, leaderId=2),
            dict(type='set_leader', expectedVersion=1, rangeId=1, leaderId=4),
            dict(type='set_replicas', expectedVersion=1, rangeId=1, replicaIds=[1,1]),
            dict(type='replace_ranges', expectedVersion=1, removeRangeIds=[{}]),
        ):
            with self.subTest(command=command), self.assertRaises(MetadataError):
                apply_command(ranges, 1, command)
        self.assertEqual(ranges[0]['leaderId'], 1)

    def test_committed_state_survives_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(directory)
            store.update_term_vote(2, 1)
            index = store.append_local(2, dict(kind='command', command=dict(
                type='replace_ranges', expectedVersion=0, ranges=[descriptor()])))
            store.set_commit_index(index)
            store.apply_committed()
            store.append_local(2, dict(kind='command', command=dict(
                type='set_leader', expectedVersion=1, rangeId=1, leaderId=2)))
            store.close()
            restored = MetadataStore(directory)
            try:
                restored.apply_committed()
                self.assertEqual(restored.snapshot()['version'], 1)
                self.assertEqual(restored.snapshot()['ranges'][0]['leaderId'], 1)
                self.assertEqual(restored.raft_state()[:2], (2, 1))
            finally:
                restored.close()

    def test_three_node_replication_failover_and_quorum_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            stores = [MetadataStore(Path(directory) / str(i)) for i in range(3)]
            servers = [ThreadingHTTPServer(('127.0.0.1', 0), object) for _ in range(3)]
            peers = {i+1: f'127.0.0.1:{server.server_port}' for i, server in enumerate(servers)}
            from backend.sharing.raft import make_raft_handler
            nodes = [RaftNode(i+1, peers, stores[i]) for i in range(3)]
            threads = []
            for server, node in zip(servers, nodes):
                server.RequestHandlerClass = make_raft_handler(node)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                threads.append(thread)
            ticks = [node.start() for node in nodes]
            active = set(range(3))
            def wait_for(predicate):
                until = time.monotonic() + 12
                while time.monotonic() < until:
                    result = predicate()
                    if result:
                        return result
                    time.sleep(.05)
                self.fail('cluster did not converge')
            def leader():
                leaders = [i for i in active if nodes[i].role == 'leader']
                return [leaders[0]] if len(leaders) == 1 else None
            try:
                first = wait_for(leader)[0]
                nodes[first].propose(dict(type='replace_ranges', expectedVersion=0, ranges=[descriptor()]))
                self.assertEqual(nodes[first].propose({'type': 'next_timestamp'}), 1)
                wait_for(lambda: all(s.snapshot()['version'] == 1 for s in stores))
                nodes[first].close()
                ticks[first].join(5)
                servers[first].shutdown()
                servers[first].server_close()
                active.remove(first)
                second = wait_for(leader)[0]
                self.assertEqual(nodes[second].propose({'type': 'next_timestamp'}), 2)
                nodes[second].propose(dict(type='set_leader', expectedVersion=1, rangeId=1, leaderId=2))
                wait_for(lambda: all(stores[i].snapshot()['version'] == 2 for i in active))
                other = next(i for i in active if i != second)
                nodes[other].close()
                ticks[other].join(5)
                servers[other].shutdown()
                servers[other].server_close()
                active.remove(other)
                with self.assertRaises(RuntimeError):
                    nodes[second].read_barrier()
                with self.assertRaises(RuntimeError):
                    nodes[second].propose({'type': 'next_timestamp'})
            finally:
                for node in nodes:
                    node.close()
                for tick in ticks:
                    tick.join(5)
                for i in active:
                    servers[i].shutdown()
                    servers[i].server_close()
                for store in stores:
                    store.close()
