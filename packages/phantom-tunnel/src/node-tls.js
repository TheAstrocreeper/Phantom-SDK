/** Experimental reference binding. Native TLS/TCP bulk bytes, no WebRTC payloads.
 * Offers MUST travel over an already authenticated, confidential peer channel.
 * This is an explicit-endpoint binding, not NAT traversal or a browser API.
 */
import tls from 'node:tls';
import {randomBytes, timingSafeEqual, X509Certificate} from 'node:crypto';
import {once} from 'node:events';

const protocol = 'phantom-bulk-tls-v1';
const limit = 2048;
const identity = value => typeof value === 'string' && /^[a-zA-Z0-9:_-]{1,128}$/.test(value);
function check(condition, message) { if (!condition) throw new Error(message); }
function frame(value) {
  const body = Buffer.from(JSON.stringify(value));
  check(body.length <= limit, 'Handshake too large');
  const head = Buffer.alloc(4); head.writeUInt32BE(body.length);
  return Buffer.concat([head, body]);
}
// Use paused reads: bytes coalesced after the handshake remain in the socket.
function readFrame(socket) {
  return new Promise((resolve, reject) => {
    let size;
    function cleanup() { socket.off('readable', read); socket.off('error', fail); socket.off('end', end); socket.off('close', end); }
    function fail(error) { cleanup(); reject(error); }
    function end() { fail(new Error('Handshake interrupted')); }
    function read() {
      try {
        if (size === undefined) {
          const header = socket.read(4); if (!header) return;
          size = header.readUInt32BE(); check(size > 0 && size <= limit, 'Invalid handshake length');
        }
        const body = socket.read(size); if (!body) return;
        const value = JSON.parse(body.toString('utf8')); cleanup(); resolve(value);
      } catch (error) { fail(error); }
    }
    socket.on('readable', read); socket.once('error', fail); socket.once('end', end); socket.once('close', end); read();
  });
}
function guard(socket, ms, signal) {
  const abort = () => socket.destroy(new Error('Tunnel cancelled'));
  const timer = setTimeout(() => socket.destroy(new Error('Tunnel handshake deadline')), ms);
  timer.unref();
  signal?.addEventListener('abort', abort, {once:true});
  if (signal?.aborted) abort();
  return () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); };
}
function deadline(ms) { check(Number.isInteger(ms) && ms > 0 && ms <= 120000, 'Invalid handshake deadline'); }

export async function listenTunnel({key, cert, localPeer, remotePeer, host = '127.0.0.1', advertisedHost = host,
  port = 0, handshakeMs = 10000, offerMs = 60000, onStream}) {
  check(identity(localPeer) && identity(remotePeer) && localPeer !== remotePeer, 'Invalid peer binding');
  check(typeof onStream === 'function', 'onStream required'); deadline(handshakeMs); deadline(offerMs);
  check(typeof advertisedHost === 'string' && advertisedHost.length > 0 && advertisedHost.length <= 253, 'Invalid endpoint');
  const sessionId = randomBytes(16).toString('hex');
  const token = randomBytes(32).toString('hex');
  const fingerprint = new X509Certificate(cert).fingerprint256;
  const sockets = new Set(); let consumed = false; let closed = false;
  const expires = performance.now() + offerMs;
  const server = tls.createServer({key, cert, minVersion:'TLSv1.3', maxVersion:'TLSv1.3', handshakeTimeout:handshakeMs}, async socket => {
    const stop = guard(socket, handshakeMs);
    try {
      const hello = await readFrame(socket);
      check(!closed && !consumed && performance.now() < expires, 'Offer unavailable');
      check(hello.protocol === protocol && hello.sessionId === sessionId && hello.from === remotePeer && hello.to === localPeer, 'Peer binding rejected');
      check(typeof hello.token === 'string' && /^[a-f0-9]{64}$/.test(hello.token) && timingSafeEqual(Buffer.from(hello.token,'hex'), Buffer.from(token,'hex')), 'Credential rejected');
      consumed = true; // Claim atomically, before acknowledging or invoking application code.
      socket.write(frame({protocol, sessionId, accepted:true}));
      stop(); socket.setNoDelay(true);
      // App owns stream errors after admission; protect process if it has no listener yet.
      socket.on('error', () => {});
      onStream(socket, {localPeer, remotePeer, sessionId});
    } catch { socket.destroy(); }
    finally { stop(); }
  });
  server.on('connection', socket => {
    if (closed || sockets.size >= 8) { socket.destroy(); return; }
    sockets.add(socket); socket.once('close', () => sockets.delete(socket));
  });
  server.on('tlsClientError', () => {});
  server.listen({host, port});
  await once(server, 'listening');
  return {
    offer: {protocol, sessionId, from:localPeer, to:remotePeer, host:advertisedHost, port:server.address().port, fingerprint, token},
    close() {
      if (closed) return Promise.resolve(); closed = true;
      for (const socket of sockets) socket.destroy();
      return new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    }
  };
}

export async function connectTunnel({offer, localPeer, remotePeer, handshakeMs = 10000, signal}) {
  deadline(handshakeMs);
  check(identity(localPeer) && identity(remotePeer) && localPeer !== remotePeer, 'Invalid peer binding');
  check(offer?.protocol === protocol && offer.from === remotePeer && offer.to === localPeer, 'Peer binding rejected');
  check(/^[a-f0-9]{32}$/.test(offer.sessionId) && /^[a-f0-9]{64}$/.test(offer.token), 'Invalid credential');
  check(typeof offer.host === 'string' && offer.host.length > 0 && offer.host.length <= 253 && Number.isInteger(offer.port) && offer.port > 0 && offer.port <= 65535, 'Invalid endpoint');
  check(typeof offer.fingerprint === 'string' && /^([A-F0-9]{2}:){31}[A-F0-9]{2}$/.test(offer.fingerprint), 'Invalid certificate pin');
  signal?.throwIfAborted();
  // Peer identity is supplied by the authenticated control channel, not Web PKI.
  // No credential or application bytes are sent until the exact certificate matches.
  const socket = tls.connect({host:offer.host, port:offer.port, rejectUnauthorized:false, minVersion:'TLSv1.3', maxVersion:'TLSv1.3'});
  const stop = guard(socket, handshakeMs, signal);
  try {
    await once(socket, 'secureConnect');
    check(socket.getPeerCertificate().fingerprint256 === offer.fingerprint, 'Certificate pin rejected');
    const response = readFrame(socket);
    socket.write(frame({protocol, sessionId:offer.sessionId, from:localPeer, to:remotePeer, token:offer.token}));
    const ack = await response;
    check(ack.protocol === protocol && ack.sessionId === offer.sessionId && ack.accepted === true, 'Tunnel rejected');
    stop(); socket.setNoDelay(true); return socket;
  } catch (error) { socket.destroy(); throw error; }
  finally { stop(); }
}
