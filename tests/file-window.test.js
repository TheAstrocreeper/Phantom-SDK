import test from 'node:test';
import assert from 'node:assert/strict';
import {createMemoryChunkStore,createFileDistribution} from '../packages/phantom-core/src/files.js';
const tick=()=>new Promise(r=>setImmediate(r));
async function fixture({window=8,sourceWindow=window,delay=0,timeout=1000,drop=()=>false,readDelay=0,sourceReadBlockBytes=0,corruptSource=false,receiverStore,receiverCrypto,receiverAllowed=()=>true,length=32*1024,onProgress=()=>{},chunkSize=1024,maxChunkBytes=65536}={}){
 const ids=['a'.repeat(16),'b'.repeat(16)],listeners=[new Set(),new Set()],requests=[],events=[],timers=new Set(),sent=[],timings=[];let offered,reading=0,peakReads=0,sourceReadCalls=0,prepared=false;
 const offer=new Promise(r=>offered=r);
 const schedule=fn=>{if(!delay)return queueMicrotask(fn);const t=setTimeout(()=>{timers.delete(t);fn();},delay);timers.add(t);};
 const cores=ids.map((id,i)=>({onEvent(fn){listeners[i].add(fn);return()=>listeners[i].delete(fn);},command(type,p){const j=ids.indexOf(p.to),deliver=e=>{for(const f of listeners[j])f(e);};if(type==='SEND_DM'){if(p.sys==='PHANTOM_FILE_REQUEST')requests.push(p.file.index);schedule(()=>deliver({type:'INCOMING_MSG',payload:{from:id,...p}}));}else{sent.push(p.file.index);if(!drop(p.file.index,requests.filter(x=>x===p.file.index).length))schedule(()=>deliver({type:'FILE_BIN_CHUNK',payload:{from:id,file:p.file,bytes:p.bytes}}));queueMicrotask(()=>{for(const f of listeners[i])f({type:'FILE_BIN_CHUNK_SENT',payload:{to:p.to,file:p.file,ok:true}});});}}}));
 const services=cores.map((core,i)=>createFileDistribution({core,maxChunkBytes,canUsePeer:i===1?receiverAllowed:()=>true,requestWindow:i===0?sourceWindow:window,requestTimeoutMs:timeout,...(i===1&&receiverStore?{chunkStore:receiverStore}:{}),...(i===1&&receiverCrypto?{crypto:receiverCrypto}:{}),emit:(type,data)=>{if(type==='fileTransferTiming')timings.push({side:i,...data});if(i===1){events.push({type,data});if(type==='fileProgress')onProgress(data);if(type==='fileOffer')offered(data);}}}));
 const bytes=new Uint8Array(length);for(let i=0;i<length;i+=65536)crypto.getRandomValues(bytes.subarray(i,i+65536));const blob=new Blob([bytes]),source={size:blob.size,slice(...args){const part=blob.slice(...args);return {async arrayBuffer(){if(prepared)sourceReadCalls++;const read=async()=>{const bytes=await part.arrayBuffer();if(prepared&&corruptSource)new Uint8Array(bytes)[0]^=1;return bytes;};if(!prepared||!readDelay)return read();reading++;peakReads=Math.max(peakReads,reading);try{await new Promise(r=>setTimeout(r,readDelay));return await read();}finally{reading--;}}};}};const m=await services[0].publish(source,{chunkSize,sourceReadBlockBytes});prepared=true;await services[0].share(ids[1],m.id);await offer;
 return {requests,events,timings,sent,get sourceReadCalls(){return sourceReadCalls;},get peakReads(){return peakReads;},cancel:()=>services[1].cancel(m.id),m,bytes,download:(options={})=>services[1].request(m.id,{peers:[ids[0]],...options}),close(){services.forEach(s=>s.close());for(const t of timers)clearTimeout(t);}};
}
test('opt-in 256 KiB chunks transfer with verified final partial chunk',async()=>{
 const f=await fixture({length:2*262144+17,chunkSize:262144,maxChunkBytes:262144,sourceReadBlockBytes:4*1024*1024});
 try{const blob=await f.download();assert.equal(f.m.total,3);assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);assert.deepEqual(f.requests,[0,1,2]);}finally{f.close();}
});
test('window queues eight unique requests before any delayed response and verifies all bytes',async()=>{const f=await fixture({delay:8});try{const p=f.download();await tick();assert.deepEqual(f.requests,[0,1,2,3,4,5,6,7]);const blob=await p;assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);assert.equal(f.events.filter(e=>e.type==='fileProgress').length,32);}finally{f.close();}});
test('per-transfer window overrides the configured default and rejects invalid values before requesting',async()=>{
 const f=await fixture({delay:10});try{
  for(const requestWindow of [0,33,1.5])await assert.rejects(f.download({requestWindow}),/request window/);
  assert.equal(f.requests.length,0);
  const result=f.download({requestWindow:32});await tick();
  assert.equal(f.requests.length,32);assert.deepEqual(new Uint8Array(await (await result).arrayBuffer()),f.bytes);
 }finally{f.close();}
});
test('opt-in window 32 stays within receive admission and is served by a default-window source without retries',async()=>{
 const store=createMemoryChunkStore();let release,started=0;const gate=new Promise(r=>release=r);
 const f=await fixture({window:32,sourceWindow:8,length:65536,receiverStore:{get:store.get,async put(...args){started++;await gate;return store.put(...args);}}});
 try{
  const result=f.download();await waitUntil(()=>f.requests.length===32&&started===4);
  await new Promise(r=>setTimeout(r,20));assert.equal(f.requests.length,32);
  release();const blob=await result;assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);
  assert.equal(f.events.some(e=>e.type==='fileRetry'||e.type==='fileError'),false);
  assert.equal(f.requests.length,64);
 }finally{release();f.close();}
});
test('one dropped response is retried, with no duplicate verified progress',async()=>{const f=await fixture({timeout:30,drop:(index,attempt)=>index===5&&attempt===1});try{await f.download();assert.equal(f.requests.filter(i=>i===5).length,2);const progress=f.events.filter(e=>e.type==='fileProgress');assert.equal(progress.length,32);assert.equal(new Set(progress.map(e=>e.data.index)).size,32);}finally{f.close();}});
test('persisted progress preserves Breach byte, elapsed, verification and in-flight fields',async()=>{
 const f=await fixture({length:32*1024+17});try{
  await f.download();let bytes=0,elapsed=0,verification=0;
  for(const {data:p} of f.events.filter(e=>e.type==='fileProgress')){
   assert.equal(p.bytes,Math.min(1024,f.bytes.length-p.index*1024));bytes+=p.bytes;
   assert.equal(p.verifiedBytes,bytes);
   assert.ok(Number.isFinite(p.elapsedMs)&&p.elapsedMs>=elapsed);elapsed=p.elapsedMs;
   assert.ok(Number.isFinite(p.verificationMs)&&p.verificationMs>=verification);verification=p.verificationMs;
   assert.ok(Number.isInteger(p.inFlight)&&p.inFlight>=0&&p.inFlight<=8);
   // The recovered application formats this field on every verified chunk.
   assert.doesNotThrow(()=>p.verificationMs.toFixed(0));
  }
  assert.equal(bytes,f.bytes.length);
 }finally{f.close();}
});
test('failed transfer resumes from verified cache and requests only the missing chunk',{timeout:10000},async t=>{
 t.mock.timers.enable({apis:['setTimeout','Date']});
 let block=true,verified;const checkpoint=new Promise(resolve=>verified=resolve);
 const f=await fixture({timeout:1000,drop:i=>block&&i===5,onProgress:progress=>{if(progress.received===31)verified();}});
 try{
  const failed=assert.rejects(f.download(),/remaining chunks/);
  // Do not race real hashing/writes against a 20 ms machine-load deadline.
  await checkpoint;assert.equal(f.events.filter(e=>e.type==='fileProgress').length,31);
  t.mock.timers.tick(1000);await tick();t.mock.timers.tick(1000);await failed;
  assert.equal(f.requests.filter(index=>index===5).length,2);
  const count=f.requests.length;block=false;const blob=await f.download();
  assert.deepEqual(f.requests.slice(count),[5]);assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);
 }finally{f.close();}
});
test('latency fixture benchmark compares verified window 1 and 8 transfers',async t=>{const durations=[];for(const window of [1,8]){const f=await fixture({window,delay:10});try{const start=performance.now();const blob=await f.download();durations.push(performance.now()-start);assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);}finally{f.close();}}t.diagnostic(`Simulated 20 ms request/response latency: window 1 ${durations[0].toFixed(0)} ms; window 8 ${durations[1].toFixed(0)} ms. Not a real-device throughput benchmark.`);assert.ok(durations[1]<durations[0]/2);});

async function waitUntil(check){const end=Date.now()+2000;while(!check()&&Date.now()<end)await new Promise(r=>setTimeout(r,2));assert.ok(check(),'condition did not settle');}
test('overlapping requests cannot replace a download while its storage inventory is loading',async()=>{
 const memory=createMemoryChunkStore();let release,entered;const gate=new Promise(r=>release=r),inside=new Promise(r=>entered=r);let first=true;
 const store={...memory,async get(id,index){if(first){first=false;entered();await gate;}return memory.get(id,index);}};
 const f=await fixture({receiverStore:store});
 try{const original=f.download();await inside;await assert.rejects(f.download(),/already active/);release();const blob=await original;assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);}
 finally{release();f.close();}
});
test('aborting an inventory scan stops before reading the remaining chunks',async()=>{
 const memory=createMemoryChunkStore();let release,entered,reads=0;const gate=new Promise(r=>release=r),inside=new Promise(r=>entered=r);
 const store={...memory,async get(id,index){reads++;if(reads===1){entered();await gate;}return memory.get(id,index);}};
 const f=await fixture({receiverStore:store}),controller=new AbortController();
 try{const rejected=assert.rejects(f.download({signal:controller.signal}),/aborted/);await inside;controller.abort();release();await rejected;assert.equal(reads,1);assert.equal(f.requests.length,0);const blob=await f.download();assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);}
 finally{release();f.close();}
});
test('disjoint concurrent ranges complete independently when one range is aborted',async()=>{
 const f=await fixture({drop:index=>index<4}),controller=new AbortController();
 try{
  const first=f.download({indexes:[0,1,2,3],concurrentRanges:true,signal:controller.signal});
  const rejected=assert.rejects(first,/aborted/);
  const second=f.download({indexes:[4,5,6,7],concurrentRanges:true});
  await assert.rejects(f.download({indexes:[3,8],concurrentRanges:true}),/already active/);
  await waitUntil(()=>f.requests.some(i=>i===0));controller.abort();await rejected;
  assert.deepEqual((await second).indexes,[4,5,6,7]);
  assert.deepEqual(f.events.filter(e=>e.type==='fileProgress').map(e=>e.data.index).sort((a,b)=>a-b),[4,5,6,7]);
 }finally{f.close();}
});
test('opt-in block source reads coalesce actual file requests without changing ordered verified delivery',async()=>{
 const f=await fixture({window:32,length:65536,readDelay:8,sourceReadBlockBytes:4*1024*1024});
 try{const blob=await f.download();assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);assert.equal(f.sourceReadCalls,1);assert.equal(f.peakReads,1);assert.deepEqual(f.sent,Array.from({length:64},(_,i)=>i));}
 finally{f.close();}
});
test('batched source bytes still require the original chunk integrity check',async()=>{
 const f=await fixture({sourceReadBlockBytes:4*1024*1024,corruptSource:true});
 try{await assert.rejects(f.download(),/remaining chunks/);assert.equal(f.events.some(e=>e.type==='fileComplete'),false);assert.equal(f.events.some(e=>e.type==='fileProgress'&&e.data.index===0),false);}
 finally{f.close();}
});
test('source reads overlap while dispatch stays ordered and bytes remain verified',async()=>{
 const f=await fixture({readDelay:8});try{
  const blob=await f.download();assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);
  assert.ok(f.peakReads>1&&f.peakReads<=8);assert.deepEqual(f.sent,Array.from({length:32},(_,i)=>i));
  const send=f.timings.find(t=>t.phase==='send');assert.equal(send.total,32);assert.ok(send.metrics.readMs.p95>=5);assert.equal(send.metrics.hashMs.count,32);
  const receive=f.timings.find(t=>t.phase==='receive');assert.equal(receive.metrics.writeMs.count,32);assert.equal(receive.metrics.requestToReceiveMs.count,32);
 }finally{f.close();}
});
test('verified chunks replenish requests before writes finish, bounded to 32 slots and four writes',async()=>{
 const store=createMemoryChunkStore();let release,active=0,peak=0,started=0;const gate=new Promise(r=>release=r);
 const f=await fixture({length:65536,receiverStore:{get:store.get,async put(...args){started++;active++;peak=Math.max(peak,active);try{await gate;await store.put(...args);}finally{active--;}}}});
 try{let done=false;const result=f.download().then(b=>{done=true;return b;});await waitUntil(()=>f.requests.length===32&&started===4);await new Promise(r=>setTimeout(r,20));
  assert.equal(f.requests.length,32);assert.equal(started,4);assert.equal(done,false);assert.equal(f.events.filter(e=>e.type==='fileProgress').length,0);
  release();const blob=await result;assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);assert.equal(peak,4);assert.equal(started,64);
 }finally{release();f.close();}
});
test('failed background writes reject completion and do not publish progress',async()=>{
 let rejectWrite;const gate=new Promise((_,reject)=>rejectWrite=reject);let started=0;
 const f=await fixture({receiverStore:{async get(){return null;},async put(){started++;await gate;}}});
 try{const result=f.download();const rejected=assert.rejects(result,/disk unavailable/);await waitUntil(()=>started===4);rejectWrite(Error('disk unavailable'));await rejected;await tick();
  assert.equal(f.events.some(e=>e.type==='fileComplete'||e.type==='fileProgress'),false);
 }finally{f.close();}
});
test('cancellation during writes suppresses late completion and progress',async()=>{
 const store=createMemoryChunkStore();let release,started=0;const gate=new Promise(r=>release=r);
 const f=await fixture({receiverStore:{get:store.get,async put(...args){started++;await gate;await store.put(...args);}}});
 try{const result=f.download();const rejected=assert.rejects(result,/cancelled/);await waitUntil(()=>started===4);f.cancel();await rejected;release();await new Promise(r=>setTimeout(r,20));
  assert.equal(started,4);assert.equal(f.events.some(e=>e.type==='fileComplete'||e.type==='fileProgress'),false);
 }finally{release();f.close();}
});

test('membership loss during writes prevents verified progress and completion',async()=>{
 const store=createMemoryChunkStore();let permitted=true,release,started=0;const gate=new Promise(r=>release=r);
 const f=await fixture({receiverAllowed:()=>permitted,receiverStore:{get:store.get,async put(...args){started++;await gate;await store.put(...args);}}});
 try{const rejected=assert.rejects(f.download(),/membership inactive/);await waitUntil(()=>started===4);permitted=false;release();await rejected;await tick();
  assert.equal(f.events.some(e=>e.type==='fileComplete'||e.type==='fileProgress'),false);
 }finally{release();f.close();}
});

test('a stalled background write times out without claiming completion',async()=>{
 let release,started=0;const gate=new Promise(r=>release=r);
 const f=await fixture({timeout:150,receiverStore:{async get(){return null;},async put(){started++;await gate;}}});
 try{const rejected=assert.rejects(f.download(),/storage timed out/);await waitUntil(()=>started===4);await rejected;release();await tick();assert.equal(f.events.some(e=>e.type==='fileComplete'),false);
 }finally{release();f.close();}
});

test('storage-backed completion verifies chunks and delegates assembly without collecting full-file buffers',async()=>{
 const memory=createMemoryChunkStore();let assemblies=0;
 const store={...memory,async getFile(id,m){assemblies++;const parts=[];for(let i=0;i<m.total;i++)parts.push(await memory.get(id,i));return new Blob(parts,{type:m.type});}};
 const f=await fixture({receiverStore:store,length:32785});try{
  const blob=await f.download();assert.equal(assemblies,1);assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);
 }finally{f.close();}
});
test('storage-backed assembly failure rejects the download rather than leaving it pending',async()=>{
 const memory=createMemoryChunkStore(),f=await fixture({receiverStore:{...memory,async getFile(){throw Error('storage assembly failed');}}});
 try{await assert.rejects(f.download(),/storage assembly failed/);}finally{f.close();}
});
test('cancelling during storage-backed assembly settles the request and emits no completion',async()=>{
 const memory=createMemoryChunkStore();let entered,release;
 const inside=new Promise(r=>entered=r),gate=new Promise(r=>release=r);
 const f=await fixture({receiverStore:{...memory,async getFile(){entered();await gate;return new Blob([new Uint8Array(32768)]);}}});
 try{const result=f.download(),rejected=assert.rejects(result,/cancelled/);await inside;f.cancel();release();await rejected;await tick();assert.equal(f.events.some(e=>e.type==='fileComplete'),false);}finally{release();f.close();}
});

test('a cancelled assembly rejecting late cannot fail a replacement request',async()=>{
 const memory=createMemoryChunkStore();let firstEntered,secondEntered,rejectFirst,releaseSecond,assemblies=0;
 const firstInside=new Promise(r=>firstEntered=r),secondInside=new Promise(r=>secondEntered=r);
 const firstGate=new Promise((_,reject)=>rejectFirst=reject),secondGate=new Promise(r=>releaseSecond=r);
 const store={...memory,async getFile(id,m){assemblies++;if(assemblies===1){firstEntered();await firstGate;}else{secondEntered();await secondGate;}const parts=[];for(let i=0;i<m.total;i++)parts.push(await memory.get(id,i));return new Blob(parts,{type:m.type});}};
 const f=await fixture({receiverStore:store});
 try{
  const old=f.download(),oldRejected=assert.rejects(old,/cancelled/);await firstInside;f.cancel();await oldRejected;
  const replacement=f.download();await secondInside;rejectFirst(Error('retired assembly failed'));await tick();releaseSecond();
  const blob=await replacement;assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);
 }finally{releaseSecond();f.close();}
});

test('a cancelled chunk verification rejecting late cannot fail a replacement request',async()=>{
 let arm=false,entered,rejectOld;const inside=new Promise(r=>entered=r),gate=new Promise((_,reject)=>rejectOld=reject);
 const receiverCrypto={subtle:{digest(...args){if(arm){arm=false;entered();return gate;}return crypto.subtle.digest(...args);}}};
 const f=await fixture({receiverCrypto,window:1,length:2048});try{
  arm=true;const old=f.download(),oldRejected=assert.rejects(old,/cancelled/);await inside;f.cancel();await oldRejected;
  const replacement=f.download();rejectOld(Error('retired verification failed'));
  const blob=await replacement;assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);
  assert.equal(f.events.some(e=>e.type==='fileError'&&e.data.error==='retired verification failed'),false);
 }finally{f.close();}
});

test('verified immutable storage skips redundant completion reads and accepts a lazy native file',async()=>{
 const memory=createMemoryChunkStore(),committed=new Set();let begins=0,reads=0,seals=0;
 const store={async begin(id,m){begins++;assert.equal(m.hashes.length,m.total);},async put(id,index,bytes){await memory.put(id,index,bytes);committed.add(index);},async get(){reads++;throw Error('Unexpected finalization reread');},getVerifiedIndexes(){return [...committed];},async getFile(id,m){seals++;const parts=[];for(let i=0;i<m.total;i++)parts.push(await memory.get(id,i));const blob=new Blob(parts);return {size:blob.size,type:m.type,slice:(...args)=>blob.slice(...args)};}};
 const f=await fixture({receiverStore:store});try{const file=await f.download();assert.equal(begins,1);assert.equal(reads,0);assert.equal(seals,1);assert.deepEqual(new Uint8Array(await file.slice().arrayBuffer()),f.bytes);}finally{f.close();}
});
test('verified storage rejects invalid committed indexes',async()=>{
 const f=await fixture({receiverStore:{get:async()=>null,put:async()=>{},getVerifiedIndexes:()=>[-1]}});
 try{await assert.rejects(f.download(),/Invalid verified storage inventory/);}finally{f.close();}
});
