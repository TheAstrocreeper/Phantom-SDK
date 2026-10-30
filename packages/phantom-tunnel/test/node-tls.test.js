import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync, readFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFileSync} from 'node:child_process';
import {Readable, Writable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {createHash} from 'node:crypto';
import {listenTunnel, connectTunnel} from '../src/node-tls.js';
const directory = mkdtempSync(join(tmpdir(), 'phantom-tls-'));
execFileSync('openssl', ['req','-x509','-newkey','ec','-pkeyopt','ec_paramgen_curve:P-256','-nodes','-days','1','-subj','/CN=phantom-test','-keyout',join(directory,'key'),'-out',join(directory,'cert')], {stdio:'ignore'});
const key = readFileSync(join(directory,'key')), cert = readFileSync(join(directory,'cert'));
rmSync(directory, {recursive:true});
const defaults = {key, cert, localPeer:'alice', remotePeer:'bob'};
const dial = offer => ({offer, localPeer:'bob', remotePeer:'alice', handshakeMs:1000});

test('sustained stream preserves exact bytes with slow consumer and reverse response', {timeout:15000}, async t => {
  const total = 16 * 1024 * 1024;
  const block = Buffer.alloc(65536, 0xa7);
  const expected = createHash('sha256'); for (let n=0;n<total;n+=block.length) expected.update(block);
  let finish;
  const received = new Promise((resolve,reject) => { finish = {resolve,reject}; });
  const listener = await listenTunnel({...defaults, onStream(socket) {
    let bytes = 0; const hash = createHash('sha256');
    const sink = new Writable({highWaterMark:32768, write(chunk, encoding, callback) {
      bytes += chunk.length; hash.update(chunk); setImmediate(callback);
    }});
    // Keep the duplex writable half open so a completion receipt can follow EOF.
    socket.allowHalfOpen = true;
    pipeline(socket, sink).then(() => {
      const result = {bytes, digest:hash.digest('hex')}; socket.end(JSON.stringify(result)); finish.resolve(result);
    }, finish.reject);
  }});
  t.after(() => listener.close());
  const socket = await connectTunnel(dial(listener.offer));
  t.after(() => socket.destroy());
  socket.allowHalfOpen = true;
  const response = (async () => { const parts=[]; for await (const chunk of socket) parts.push(chunk); return JSON.parse(Buffer.concat(parts)); })();
  await pipeline(Readable.from((function* () { for (let n=0;n<total;n+=block.length) yield block; })()), socket);
  const result = await received;
  assert.equal(result.bytes, total); assert.equal(result.digest, expected.digest('hex'));
  assert.deepEqual(await response, result);
});

test('certificate mismatch sends no application session to the listener', async t => {
  let admitted = 0;
  const listener = await listenTunnel({...defaults, onStream() { admitted++; }});
  t.after(() => listener.close());
  const offer = {...listener.offer, fingerprint:Array(32).fill('00').join(':')};
  await assert.rejects(connectTunnel(dial(offer)), /pin rejected/);
  assert.equal(admitted, 0);
});

test('wrong credentials rejected; valid offer remains usable and replay is rejected', async t => {
  let admitted = 0;
  const listener = await listenTunnel({...defaults, onStream(socket) { admitted++; socket.resume(); }});
  t.after(() => listener.close());
  await assert.rejects(connectTunnel(dial({...listener.offer, token:'00'.repeat(32)})));
  const socket = await connectTunnel(dial(listener.offer)); t.after(() => socket.destroy());
  assert.equal(admitted, 1);
  await assert.rejects(connectTunnel(dial(listener.offer)));
  assert.equal(admitted, 1);
});

test('concurrent reuse admits exactly one socket', async t => {
  let admitted = 0;
  const listener = await listenTunnel({...defaults, onStream(socket) { admitted++; socket.resume(); }});
  t.after(() => listener.close());
  const results = await Promise.allSettled([connectTunnel(dial(listener.offer)), connectTunnel(dial(listener.offer))]);
  for (const result of results) if (result.status === 'fulfilled') { result.value.on('error',()=>{}); result.value.destroy(); }
  assert.equal(results.filter(r => r.status === 'fulfilled').length, 1); assert.equal(admitted,1);
});

test('peer confusion rejected before dialing', async () => {
  await assert.rejects(connectTunnel(dial({protocol:'phantom-bulk-tls-v1', from:'eve', to:'bob'})), /Peer binding/);
});

test('cancelled attempt never dials', async () => {
  const controller = new AbortController(); controller.abort();
  const listener = await listenTunnel({...defaults, onStream() { assert.fail('admitted'); }});
  try { await assert.rejects(connectTunnel({...dial(listener.offer), signal:controller.signal}), /abort/i); }
  finally { await listener.close(); }
});

test('expired offer cannot admit a stream', async t => {
  const listener = await listenTunnel({...defaults, offerMs:1, onStream() { assert.fail('admitted'); }});
  t.after(() => listener.close());
  await new Promise(resolve => setTimeout(resolve, 10));
  await assert.rejects(connectTunnel(dial(listener.offer)));
});

test('oversized handshake rejected before admission', async t => {
  const tls = await import('node:tls');
  const {once} = await import('node:events');
  const listener = await listenTunnel({...defaults, onStream() { assert.fail('admitted'); }});
  t.after(() => listener.close());
  const socket = tls.connect({host:listener.offer.host, port:listener.offer.port, rejectUnauthorized:false});
  t.after(() => socket.destroy()); socket.on('error',()=>{});
  await once(socket,'secureConnect');
  const closed = new Promise(resolve => socket.once('close',resolve));
  const header = Buffer.alloc(4); header.writeUInt32BE(2049); socket.write(header);
  await closed;
});

test('silent handshake bounded by deadline; cancellation tears down pending connection', {timeout:3000}, async t => {
  const tls = await import('node:tls');
  const {once} = await import('node:events');
  const listener = await listenTunnel({...defaults, handshakeMs:30, onStream() { assert.fail('admitted'); }});
  t.after(() => listener.close());
  const socket = tls.connect({host:listener.offer.host, port:listener.offer.port, rejectUnauthorized:false});
  t.after(() => socket.destroy()); socket.on('error',()=>{});
  const closed = new Promise(resolve => socket.once('close',resolve));
  await once(socket,'secureConnect'); await closed;
  // An unrelated TLS endpoint that accepts TLS but never acknowledges the protocol.
  const silent = tls.createServer({key,cert}, s => s.on('error',()=>{}));
  const sockets = new Set(); silent.on('connection',s => {sockets.add(s);s.on('close',()=>sockets.delete(s));});
  silent.listen(0,'127.0.0.1'); await once(silent,'listening');
  t.after(() => { for (const s of sockets) s.destroy(); return new Promise(resolve => silent.close(resolve)); });
  const controller = new AbortController();
  const pending = connectTunnel({...dial({...listener.offer, port:silent.address().port}), signal:controller.signal});
  setTimeout(() => controller.abort(),30);
  await assert.rejects(pending,/cancelled/);
});
