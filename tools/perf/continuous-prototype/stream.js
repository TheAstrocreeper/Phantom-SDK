// Experimental one-source transport. It is not a Phantom wire protocol.
const hex = bytes => Array.from(new Uint8Array(bytes), b => b.toString(16).padStart(2, '0')).join('');
const digest = async bytes => hex(await crypto.subtle.digest('SHA-256', bytes));
const CHUNK = 65536;
export async function makeManifest(file) {
  const hashes = [];
  for (let at = 0; at < file.size; at += CHUNK) hashes.push(await digest(await file.slice(at, at + CHUNK).arrayBuffer()));
  return {size: file.size, chunkSize: CHUNK, hashes};
}

// Credits bound data sent ahead of committed storage; this is not a 32-piece
// stop-and-wait batch. ACKs are cumulative and replenish a rolling window.
export async function sendStream(channel, file, manifest, {windowChunks = 128, highBytes = 2 * 1024 * 1024} = {}) {
  if (!Number.isInteger(windowChunks) || windowChunks < 16 || windowChunks > 128 || highBytes < CHUNK + 4) throw Error('Invalid limits');
  let committed = 0, sent = 0, failure, wake;
  const notify = () => { wake?.(); wake = undefined; };
  const onMessage = e => { if (typeof e.data !== 'string') return; const m = JSON.parse(e.data); if (m.type === 'ack' && Number.isInteger(m.count) && m.count >= committed && m.count <= sent) { committed = m.count; notify(); } else if (m.type === 'error') { failure = Error(m.error); notify(); } };
  const onClose = () => { failure = Error('Channel closed'); notify(); };
  const wait = () => new Promise(resolve => { wake = resolve; });
  channel.addEventListener('message', onMessage); channel.addEventListener('close', onClose); channel.addEventListener('bufferedamountlow', notify);
  channel.bufferedAmountLowThreshold = Math.floor(highBytes / 2);
  try {
    channel.send(JSON.stringify({type: 'manifest', manifest}));
    while (sent < manifest.hashes.length) {
      if (failure) throw failure;
      if (channel.readyState !== 'open') throw Error('Channel closed');
      if (sent - committed >= windowChunks || channel.bufferedAmount + CHUNK + 4 > highBytes) { await wait(); continue; }
      const bytes = await file.slice(sent * CHUNK, Math.min(file.size, (sent + 1) * CHUNK)).arrayBuffer();
      if (await digest(bytes) !== manifest.hashes[sent]) throw Error('Source changed');
      if (failure) throw failure;
      const frame = new Uint8Array(4 + bytes.byteLength); new DataView(frame.buffer).setUint32(0, sent); frame.set(new Uint8Array(bytes), 4);
      channel.send(frame); sent++;
    }
    while (committed < sent) { if (failure) throw failure; await wait(); }
    return {sent, committed};
  } finally { channel.removeEventListener('message', onMessage); channel.removeEventListener('close', onClose); channel.removeEventListener('bufferedamountlow', notify); }
}

export function receiveStream(channel, store, {ackEvery = 16, maxPending = 128, onCommit = () => {}} = {}) {
  if (!Number.isInteger(ackEvery) || ackEvery < 1 || ackEvery > 16 || !Number.isInteger(maxPending) || maxPending < 16 || maxPending > 128) throw Error('Invalid receive limits');
  let manifest, next = 0, accepted = 0, queue = Promise.resolve(), failed = false;
  const fail = error => { if (failed) return; failed = true; if (channel.readyState === 'open') channel.send(JSON.stringify({type:'error', error:error.message})); rejectDone(error); };
  let resolveDone, rejectDone; const done = new Promise((resolve, reject) => {resolveDone = resolve; rejectDone = reject;});
  const onMessage = e => {
    if (failed) return;
    if (typeof e.data === 'string') { try { const msg = JSON.parse(e.data); if (manifest || msg.type !== 'manifest' || !Number.isSafeInteger(msg.manifest?.size) || msg.manifest.size < 0 || msg.manifest.chunkSize !== CHUNK || !Array.isArray(msg.manifest.hashes) || msg.manifest.hashes.length !== Math.ceil(msg.manifest.size / CHUNK) || msg.manifest.hashes.some(h => !/^[0-9a-f]{64}$/.test(h))) throw Error('Invalid manifest'); manifest = msg.manifest; if (!manifest.size) resolveDone({manifest, count:0}); } catch (error) { fail(error); } return; }
    const frame = e.data;
    if (!(frame instanceof ArrayBuffer) || frame.byteLength < 4 || !manifest) return fail(Error('Invalid frame'));
    const index = new DataView(frame).getUint32(0), bytes = frame.slice(4);
    if (index !== accepted || index >= manifest.hashes.length || accepted - next >= maxPending || bytes.byteLength !== Math.min(CHUNK, manifest.size-index*CHUNK)) return fail(Error('Invalid chunk'));
    accepted++;
    queue = queue.then(async () => {if (failed) return; if (await digest(bytes) !== manifest.hashes[index]) throw Error('Chunk integrity failure'); await store.put('stream', index, bytes); next++; onCommit(bytes.byteLength, next); if (next % ackEvery === 0 || next === manifest.hashes.length) channel.send(JSON.stringify({type:'ack',count:next})); if (next === manifest.hashes.length) resolveDone({manifest,count:next});}).catch(fail);
  };
  const onClose = () => fail(Error('Channel closed'));
  channel.addEventListener('message', onMessage); channel.addEventListener('close', onClose);
  return {done, close(){channel.removeEventListener('message', onMessage);channel.removeEventListener('close', onClose);}};
}
