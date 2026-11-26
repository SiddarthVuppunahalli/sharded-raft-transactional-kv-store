import json
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from backend.cmd.metadata import make_api_handler
from backend.sharing.metadata import MetadataStore
from backend.sharing.raft import RaftNode
from frontend.dev_server import make_handler


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = MetadataStore(self.temp.name)
        self.addCleanup(self.store.close)
        self.node = RaftNode(1, {1: '127.0.0.1:1'}, self.store)
        self.node._start_election()
        self.calls = []
        self.values = {}
        self.failures = []
        owner = self
        class Receiver(BaseHTTPRequestHandler):
            def do_POST(self):
                payload = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                owner.calls.append(payload)
                status, result = 200, None
                if owner.failures:
                    status, result = owner.failures.pop(0)
                if result is None:
                    key = payload['key']
                    if payload['operation'] == 'put':
                        owner.values[key] = payload['value']
                    result = dict(found=key in owner.values, value=owner.values.get(key, ''))
                data = json.dumps(result).encode()
                self.send_response(status)
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            def log_message(self, *_):
                pass
        self.range_url = self.serve(Receiver)
        self.node.propose(dict(type='replace_ranges', expectedVersion=0, ranges=[dict(
            id=1, startKey=None, endKey=None, replicaIds=[1,2], leaderId=1)]))
        nodes = {1:self.range_url.removeprefix('http://'), 2:self.range_url.removeprefix('http://')}
        self.api = self.serve(make_api_handler(self.node, self.store, {}, nodes))
        self.proxy = self.serve(make_handler(self.api))

    def serve(self, handler):
        server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def close():
            server.shutdown()
            server.server_close()
            thread.join(2)
        self.addCleanup(close)
        return f'http://127.0.0.1:{server.server_port}'

    def request(self, method='GET', body=None, path='/api/kv?key=apple'):
        request = urllib.request.Request(self.proxy + path, data=body, method=method)
        try:
            response = urllib.request.urlopen(request, timeout=3)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.code, response.read()

    def test_text_put_get_and_missing_key_through_proxy(self):
        value = 'hello caf\u00e9'.encode('utf-8')
        self.assertEqual(self.request('PUT', value)[0], 204)
        self.assertEqual(self.request(), (200, value))
        self.assertEqual(self.request(path='/api/kv?key=missing')[0], 404)
        self.assertEqual(self.calls[0]['key'], 'apple')
        self.assertEqual(self.calls[0]['value'], value.decode('utf-8'))
        key = 'caf\u00e9 & + /?'
        path = '/api/kv?key=' + urllib.parse.quote(key, safe='')
        self.assertEqual(self.request('PUT', b'', path)[0], 204)
        self.assertEqual(self.request(path=path), (200, b''))
        self.assertEqual(self.calls[-1]['key'], key)

    def test_plain_lookup_validation_and_endpoint_scope(self):
        status, body = self.request(path='/api/metadata/lookup?key=apple')
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)['range']['id'], 1)
        self.assertEqual(self.request('PUT', b'\xff')[0], 400)
        self.assertEqual(self.request(path='/api/kv?key=%FF')[0], 400)
        self.failures = [(200, dict(found=True, value=123))]
        self.assertEqual(self.request()[0], 502)
        self.proxy = self.api
        self.assertEqual(self.request(path='/healthz'), (200, b'ok\n'))
        self.assertEqual(self.request(path='/api/transactions')[0], 404)
        self.assertEqual(self.request(path='/api/metadata/ranges')[0], 200)

    def test_timestamp_and_metadata_update_api(self):
        # Internal timestamps are called directly, not through the browser proxy.
        self.proxy = self.api
        for expected in (1, 2):
            status, body = self.request('POST', b'{}', '/internal/timestamps/next')
            self.assertEqual((status, json.loads(body)), (200, {'timestamp': expected}))
        self.assertEqual(self.request(path='/internal/timestamps/next')[0], 405)
        self.assertEqual(self.request('POST', b'{"extra":1}', '/internal/timestamps/next')[0], 400)
        commands = [
            dict(type='replace_ranges', expectedVersion=1, removeRangeIds=[1], ranges=[
                dict(id=1, startKey=None, endKey='m', replicaIds=[1,2], leaderId=1),
                dict(id=2, startKey='m', endKey=None, replicaIds=[1,2], leaderId=2)]),
            dict(type='set_replicas', expectedVersion=2, rangeId=2, replicaIds=[1,3], leaderId=1),
        ]
        for command in commands:
            status, body = self.request('POST', json.dumps(command).encode(), '/api/metadata/commands')
            self.assertEqual(status, 200, body)
        self.assertEqual(json.loads(body)['ranges'][1]['replicaIds'], [1, 3])

    def test_leader_change_and_stale_map_retry(self):
        self.failures = [(409, dict(error='not_leader', leaderId=2)),
                         (409, dict(error='stale_metadata'))]
        self.assertEqual(self.request('PUT', b'new')[0], 204)
        self.assertEqual(len(self.calls), 3)
        self.assertEqual(self.store.snapshot()['ranges'][0]['leaderId'], 2)

    def test_retry_limit_and_invalid_leader(self):
        self.failures = [(409, dict(error='stale_metadata'))] * 3
        status, body = self.request()
        self.assertEqual(status, 503)
        self.assertIn('retries exhausted', json.loads(body)['error'])
        self.failures = [(409, dict(error='not_leader', leaderId=999))]
        self.assertEqual(self.request()[0], 503)
        self.assertEqual(self.store.snapshot()['ranges'][0]['leaderId'], 1)

    def test_upstream_errors_cannot_be_successful_writes(self):
        self.failures = [(500, {})]
        self.assertEqual(self.request('PUT', b'data')[0], 502)
        self.failures = [(504, dict(error='range timed out'))]
        status, body = self.request()
        self.assertEqual(status, 504)
        self.assertEqual(json.loads(body)['error'], 'range timed out')

    def test_bad_key_and_oversize_body(self):
        self.assertEqual(self.request(path='/api/kv')[0], 400)
        self.assertEqual(self.request('PUT', b'x' * ((1 << 20) + 1))[0], 413)
        self.assertEqual(self.calls, [])

    def test_proxy_failure_is_readable_json(self):
        self.proxy = self.serve(make_handler('http://127.0.0.1:1'))
        status, body = self.request()
        self.assertEqual(status, 502)
        self.assertIn('Backend is unavailable', json.loads(body)['error'])
