"""Exercise the exported C ABI through ctypes, not Rust-only entry points.
Build the native library first. Loopback correctness tests, no NAT/speed claims.
"""
import concurrent.futures as futures
import ctypes as c
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import time
import unittest

name = 'phantom_tunnel_native.dll' if sys.platform == 'win32' else ('libphantom_tunnel_native.dylib' if sys.platform == 'darwin' else 'libphantom_tunnel_native.so')
lib = c.CDLL(os.environ.get('PHANTOM_NATIVE_LIBRARY', str(Path(__file__).parent / 'target' / 'debug' / name)))
ptr, size, integer, ms = c.c_void_p, c.c_size_t, c.c_int, c.c_uint64
signatures = {
    'prepare': [ptr, size, c.POINTER(ptr)],
    'offer': [ptr, ptr, size, c.POINTER(size)],
    'connect': [ptr, ptr, size, integer],
    'open_stream': [ptr, integer, ms, c.POINTER(ptr)],
    'stream_write': [ptr, ptr, size, ms],
    'stream_read': [ptr, ptr, size, c.POINTER(size), ms],
    'stream_finish': [ptr],
    'send_datagram': [ptr, ptr, size],
    'recv_datagram': [ptr, ptr, size, c.POINTER(size), ms],
    'status': [ptr, ptr, size, c.POINTER(size)],
    'close': [ptr], 'session_free': [ptr], 'stream_free': [ptr],
}
for name, args in signatures.items():
    fn = getattr(lib, 'phantom_tunnel_' + name)
    fn.argtypes = args
    fn.restype = None if name.endswith('_free') else integer


def call(name, *args):
    return getattr(lib, 'phantom_tunnel_' + name)(*args)


def prepare(local, remote, session, stun_port=None):
    config = dict(localPeer=local, remotePeer=remote, sessionId=session,
                  bindAddress='127.0.0.1', deadlineMs=3000)
    if stun_port:
        config.update(stunHost='127.0.0.1', stunPort=stun_port)
    config = json.dumps(config).encode()
    handle = ptr()
    assert call('prepare', config, len(config), c.byref(handle)) == 0
    return handle


def offer(handle):
    length = size()
    assert call('offer', handle, None, 0, c.byref(length)) == -1
    data = c.create_string_buffer(length.value)
    assert call('offer', handle, data, len(data), c.byref(length)) == 0
    return data.raw[:length.value]


class NativeBindingTest(unittest.TestCase):
    def pair(self):
        session = secrets.token_hex(16)
        a, b = prepare('alice', 'bob', session), prepare('bob', 'alice', session)
        self.addCleanup(call, 'session_free', b)
        self.addCleanup(call, 'session_free', a)
        ao, bo = offer(a), offer(b)
        with futures.ThreadPoolExecutor(2) as pool:
            connected = pool.submit(call, 'connect', a, bo, len(bo), 1)
            self.assertEqual(call('connect', b, ao, len(ao), 0), 0)
            self.assertEqual(connected.result(5), 0)
        return a, b

    def stream(self, session, accept=0):
        result = ptr()
        self.assertEqual(call('open_stream', session, accept, 3000, c.byref(result)), 0)
        self.addCleanup(call, 'stream_free', result)
        return result

    def test_c_stream_backpressure_hash_receipt_and_datagram(self):
        a, b = self.pair()
        source = self.stream(a)
        block = b'\xa7' * 65536
        self.assertEqual(call('stream_write', source, block, len(block), 3000), 0)
        sink = self.stream(b, 1)
        count = 64  # 4 MiB, consumed through foreign-language buffers.

        def write():
            for _ in range(count - 1):
                self.assertEqual(call('stream_write', source, block, len(block), 3000), 0)
            self.assertEqual(call('stream_finish', source), 0)

        with futures.ThreadPoolExecutor(1) as pool:
            sending = pool.submit(write)
            buf, got, digest, total = c.create_string_buffer(32768), size(), hashlib.sha256(), 0
            while True:
                self.assertEqual(call('stream_read', sink, buf, len(buf), c.byref(got), 3000), 0)
                if not got.value:
                    break
                total += got.value
                digest.update(buf.raw[:got.value])
            sending.result(5)
        self.assertEqual(total, count * len(block))
        self.assertEqual(digest.digest(), hashlib.sha256(block * count).digest())
        self.assertEqual(call('stream_write', sink, b'ok', 2, 3000), 0)
        self.assertEqual(call('stream_finish', sink), 0)
        self.assertEqual(call('stream_read', source, buf, len(buf), c.byref(got), 3000), 0)
        self.assertEqual(buf.raw[:got.value], b'ok')
        self.assertEqual(call('send_datagram', a, b'bulk', 4), 0)
        self.assertEqual(call('recv_datagram', b, buf, len(buf), c.byref(got), 3000), 0)
        self.assertEqual(buf.raw[:got.value], b'bulk')
        self.assertEqual(call('status', a, buf, len(buf), c.byref(got)), 0)
        status = json.loads(buf.raw[:got.value])
        self.assertEqual(status['carrier'], 'ice-quic')
        self.assertNotIn('iceSdp', status)
        self.assertEqual(call('close', a), 0)
        self.assertEqual(call('close', a), 0)

    def test_stun_only_candidates_connect_through_filtered_mappings(self):
        from nat_fixture import FilteredMappings
        mappings = FilteredMappings()
        self.addCleanup(mappings.close)
        session = secrets.token_hex(16)
        a = prepare('alice', 'bob', session, mappings.port)
        b = prepare('bob', 'alice', session, mappings.port)
        self.addCleanup(call, 'session_free', b)
        self.addCleanup(call, 'session_free', a)
        ao, bo = json.loads(offer(a)), json.loads(offer(b))
        for value in (ao, bo):
            self.assertIn('typ srflx', value['iceSdp'])
            value['iceSdp'] = '\r\n'.join(line for line in value['iceSdp'].splitlines() if 'typ host' not in line) + '\r\n'
            self.assertNotIn('typ host', value['iceSdp'])
        ao, bo = json.dumps(ao).encode(), json.dumps(bo).encode()
        with futures.ThreadPoolExecutor(2) as pool:
            pending = pool.submit(call, 'connect', a, bo, len(bo), 1)
            self.assertEqual(call('connect', b, ao, len(ao), 0), 0)
            self.assertEqual(pending.result(5), 0)
        buf, length = c.create_string_buffer(4096), size()
        self.assertEqual(call('status', a, buf, len(buf), c.byref(length)), 0)
        status = json.loads(buf.raw[:length.value])
        public_ports = {m['public'][1] for m in mappings.mappings.values()}
        self.assertIn(int(status['selectedRemote'].rsplit(':', 1)[1]), public_ports)
        self.assertGreater(mappings.filtered, 0)
        self.assertGreater(mappings.forwarded, 0)
        self.assertEqual(call('send_datagram', a, b'punch', 5), 0)
        self.assertEqual(call('recv_datagram', b, buf, len(buf), c.byref(length), 3000), 0)
        self.assertEqual(buf.raw[:length.value], b'punch')

    def test_close_cancels_an_inflight_establishment(self):
        a = prepare('alice', 'bob', secrets.token_hex(16))
        self.addCleanup(call, 'session_free', a)
        # A valid-shaped remote offer, but the other endpoint never connects.
        remote = json.loads(offer(a))
        remote['from'], remote['to'] = 'bob', 'alice'
        remote = json.dumps(remote).encode()
        with futures.ThreadPoolExecutor(1) as pool:
            pending = pool.submit(call, 'connect', a, remote, len(remote), 1)
            time.sleep(0.05)
            start = time.monotonic()
            self.assertEqual(call('close', a), 0)
            self.assertEqual(pending.result(2), -1)
            self.assertLess(time.monotonic() - start, 1)

    def test_session_is_consumed_and_buffers_are_checked(self):
        a = prepare('alice', 'bob', secrets.token_hex(16))
        self.addCleanup(call, 'session_free', a)
        remote = json.loads(offer(a))
        remote['from'] = 'mallory'
        remote = json.dumps(remote).encode()
        self.assertEqual(call('connect', a, remote, len(remote), 1), -1)
        self.assertEqual(call('connect', a, remote, len(remote), 1), -1)
        out = ptr()
        self.assertEqual(call('open_stream', a, 0, 1, c.byref(out)), -1)
        self.assertFalse(out.value)
        self.assertEqual(call('offer', a, None, 0, None), -1)
        self.assertEqual(call('close', None), -1)
        call('session_free', None)
        call('stream_free', None)


if __name__ == '__main__':
    unittest.main()
