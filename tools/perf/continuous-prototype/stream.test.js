import {test} from 'node:test';
import assert from 'node:assert/strict';
import {webcrypto} from 'node:crypto';
import {createMemoryChunkStore} from '../../../packages/phantom-core/src/files.js';
import {makeManifest, sendStream, receiveStream} from './stream.js';
globalThis.crypto ??= webcrypto;
class Channel extends EventTarget {
  readyState = 'open'; bufferedAmount = 0; bufferedAmountLowThreshold = 0;
  send(data) {
    const bytes = typeof data === 'string' ? data.length : data.byteLength;
    this.bufferedAmount += bytes;
    const copy = typeof data === 'string' ? data : data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
    setImmediate(() => { const previous = this.bufferedAmount; this.bufferedAmount -= bytes; this.peer.dispatchEvent(new MessageEvent('message',{data:copy})); if(previous > this.bufferedAmountLowThreshold && this.bufferedAmount <= this.bufferedAmountLowThreshold) this.dispatchEvent(new Event('bufferedamountlow')); });
  }
}
const channels = () => {const a=new Channel(),b=new Channel();a.peer=b;b.peer=a;return [a,b];};
test('streams verified chunks to storage with rolling credits', async () => {
  const data = new Uint8Array(2*1024*1024+17); for(let i=0;i<data.length;i++) data[i]=i%251;
  const file=new Blob([data]),manifest=await makeManifest(file),store=createMemoryChunkStore(),[sender,receiver]=channels();
  const session=receiveStream(receiver,store,{ackEvery:4});
  const [sent, result]=await Promise.all([sendStream(sender,file,manifest,{windowChunks:16}),session.done]);
  assert.equal(sent.committed,manifest.hashes.length);assert.equal(result.count,manifest.hashes.length);
  for(let i=0;i<result.count;i++)assert.deepEqual(new Uint8Array(await store.get('stream',i)),data.subarray(i*65536,Math.min(data.length,(i+1)*65536)));
  session.close();
});
test('rejects changed source bytes before sending', async () => {
  const original=new Blob([new Uint8Array([1,2,3])]), manifest=await makeManifest(original), [sender,receiver]=channels();
  const session=receiveStream(receiver,createMemoryChunkStore());
  await assert.rejects(sendStream(sender,new Blob([new Uint8Array([1,2,4])]),manifest),/Source changed/);
  session.close();
});
