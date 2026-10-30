"""Test-only UDP mapping/filter simulator and minimal RFC 5389 STUN responder.
No external network, kernel namespace privileges, or production relay support.
Mappings deliver inbound traffic only after the destination peer has itself
sent outbound traffic toward that remote mapping. Not a complete NAT model.
"""
import selectors
import socket
import struct
import threading


class FilteredMappings:
    def __init__(self):
        self.selector = selectors.DefaultSelector()
        self.stun = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.stun.bind(('127.0.0.1', 0))
        self.selector.register(self.stun, selectors.EVENT_READ, None)
        self.mappings = {}
        self.running = True
        self.failure = None
        self.filtered = 0
        self.forwarded = 0
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    @property
    def port(self):
        return self.stun.getsockname()[1]

    def run(self):
        try:
            while self.running:
                for key, _ in self.selector.select(0.05):
                    data, source = key.fileobj.recvfrom(65536)
                    if key.data is None:
                        if len(data) < 20 or data[:2] != b'\x00\x01' or data[4:8] != b'\x21\x12\xa4\x42':
                            continue
                        if source not in self.mappings:
                            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            sock.bind(('127.0.0.1', 0))
                            mapping = dict(socket=sock, private=source, public=sock.getsockname(), allowed=set())
                            self.mappings[source] = mapping
                            self.selector.register(sock, selectors.EVENT_READ, mapping)
                        address = self.mappings[source]['public']
                        port = address[1] ^ 0x2112
                        ip = struct.unpack('!I', socket.inet_aton(address[0]))[0] ^ 0x2112a442
                        attribute = struct.pack('!HHBBHI', 0x20, 8, 0, 1, port, ip)
                        self.stun.sendto(struct.pack('!HH', 0x101, len(attribute)) + data[4:20] + attribute, source)
                    else:
                        destination = key.data
                        sender = self.mappings.get(source)
                        if sender is None or sender is destination:
                            continue
                        sender['allowed'].add(destination['public'])
                        if sender['public'] not in destination['allowed']:
                            self.filtered += 1
                            continue
                        # Translate the observed source too, so the receiving
                        # ICE agent sees the other peer's advertised mapping.
                        sender['socket'].sendto(data, destination['private'])
                        self.forwarded += 1
        except Exception as exc:
            self.failure = exc

    def close(self):
        self.running = False
        self.thread.join(2)
        for mapping in self.mappings.values():
            mapping['socket'].close()
        self.stun.close()
        self.selector.close()
        if self.failure:
            raise self.failure
