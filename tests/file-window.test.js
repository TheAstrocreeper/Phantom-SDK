import test from 'node:test';
import assert from 'node:assert/strict';
import {createMemoryChunkStore,createFileDistribution} from '../packages/phantom-core/src/files.js';
const tick=()=>new Promise(r=>setImmediate(r));
async function fixture({window=8,delay=0,timeout=1000,drop=()=>false,readDelay=0,receiverStore,receiverAllowed=()=>true,length=32*1024}={}){
 const ids=['a'.repeat(16),'b'.repeat(16)],listeners=[new Set(),new Set()],requests=[],events=[],timers=new Set(),sent=[],timings=[];let offered,reading=0,peakReads=0,prepared=false;
 const offer=new Promise(r=>offered=r);
 const schedule=fn=>{if(!delay)return queueMicrotask(fn);const t=setTimeout(()=>{timers.delete(t);fn();},delay);timers.add(t);};
 const cores=ids.map((id,i)=>({onEvent(fn){listeners[i].add(fn);return()=>listeners[i].delete(fn);},command(type,p){const j=ids.indexOf(p.to),deliver=e=>{for(const f of listeners[j])f(e);};if(type==='SEND_DM'){if(p.sys==='PHANTOM_FILE_REQUEST')requests.push(p.file.index);schedule(()=>deliver({type:'INCOMING_MSG',payload:{from:id,...p}}));}else{sent.push(p.file.index);if(!drop(p.file.index,requests.filter(x=>x===p.file.index).length))schedule(()=>deliver({type:'FILE_BIN_CHUNK',payload:{from:id,file:p.file,bytes:p.bytes}}));queueMicrotask(()=>{for(const f of listeners[i])f({type:'FILE_BIN_CHUNK_SENT',payload:{to:p.to,file:p.file,ok:true}});});}}}));
 const services=cores.map((core,i)=>createFileDistribution({core,canUsePeer:i===1?receiverAllowed:()=>true,requestWindow:window,requestTimeoutMs:timeout,...(i===1&&receiverStore?{chunkStore:receiverStore}:{}),emit:(type,data)=>{if(type==='fileTransferTiming')timings.push({side:i,...data});if(i===1){events.push({type,data});if(type==='fileOffer')offered(data);}}}));
 const bytes=new Uint8Array(length);crypto.getRandomValues(bytes);const blob=new Blob([bytes]),source={size:blob.size,slice(...args){const part=blob.slice(...args);return {async arrayBuffer(){if(!prepared||!readDelay)return part.arrayBuffer();reading++;peakReads=Math.max(peakReads,reading);try{await new Promise(r=>setTimeout(r,readDelay));return await part.arrayBuffer();}finally{reading--;}}};}};const m=await services[0].publish(source,{chunkSize:1024});prepared=true;await services[0].share(ids[1],m.id);await offer;
 return {requests,events,timings,sent,get peakReads(){return peakReads;},cancel:()=>services[1].cancel(m.id),m,bytes,download:()=>services[1].request(m.id,{peers:[ids[0]]}),close(){services.forEach(s=>s.close());for(const t of timers)clearTimeout(t);}};
}
test('window queues eight unique requests before any delayed response and verifies all bytes',async()=>{const f=await fixture({delay:8});try{const p=f.download();await tick();assert.deepEqual(f.requests,[0,1,2,3,4,5,6,7]);const blob=await p;assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);assert.equal(f.events.filter(e=>e.type==='fileProgress').length,32);}finally{f.close();}});
test('one dropped response is retried, with no duplicate verified progress',async()=>{const f=await fixture({timeout:30,drop:(index,attempt)=>index===5&&attempt===1});try{await f.download();assert.equal(f.requests.filter(i=>i===5).length,2);const progress=f.events.filter(e=>e.type==='fileProgress');assert.equal(progress.length,32);assert.equal(new Set(progress.map(e=>e.data.index)).size,32);}finally{f.close();}});
test('failed transfer resumes from verified cache and requests only the missing chunk',async()=>{let block=true;const f=await fixture({timeout:20,drop:i=>block&&i===5});try{await assert.rejects(f.download(),/remaining chunks/);assert.equal(f.events.filter(e=>e.type==='fileProgress').length,31);const count=f.requests.length;block=false;const blob=await f.download();assert.deepEqual(f.requests.slice(count),[5]);assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);}finally{f.close();}});
test('latency fixture benchmark compares verified window 1 and 8 transfers',async t=>{const durations=[];for(const window of [1,8]){const f=await fixture({window,delay:10});try{const start=performance.now();const blob=await f.download();durations.push(performance.now()-start);assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),f.bytes);}finally{f.close();}}t.diagnostic(`Simulated 20 ms request/response latency: window 1 ${durations[0].toFixed(0)} ms; window 8 ${durations[1].toFixed(0)} ms. Not a real-device throughput benchmark.`);assert.ok(durations[1]<durations[0]/2);});

async function waitUntil(check){const end=Date.now()+2000;while(!check()&&Date.now()<end)await new Promise(r=>setTimeout(r,2));assert.ok(check(),'condition did not settle');}
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
