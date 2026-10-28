import test from 'node:test';
import assert from 'node:assert/strict';
import {createSourceBlockReader} from '../packages/phantom-core/src/source-block-reader.js';
const tick=()=>new Promise(r=>setImmediate(r));
test('concurrent chunks share one bounded source read and independent output buffers',async()=>{
 const bytes=Uint8Array.from({length:4*1024*1024+17},(_,i)=>i%251),blob=new Blob([bytes]);let calls=0;
 const source={size:blob.size,slice(start,end){calls++;assert.ok(end-start<=4*1024*1024);return blob.slice(start,end);}};
 const reader=createSourceBlockReader();try{
  const chunks=await Promise.all(Array.from({length:32},(_,i)=>reader.read(source,i,65536)));
  assert.equal(calls,1);for(let i=0;i<32;i++)assert.deepEqual(new Uint8Array(chunks[i]),bytes.subarray(i*65536,(i+1)*65536));
  new Uint8Array(chunks[0])[0]=255;assert.equal(new Uint8Array(await reader.read(source,0,65536))[0],0);
  assert.equal((await reader.read(source,64,65536)).byteLength,17);assert.equal(calls,2);
 }finally{reader.close();}
});
test('different files serialize block loads instead of multiplying the cache budget',async()=>{
 const gates=[];let active=0,peak=0;
 const source=value=>({size:65536,slice(){return {arrayBuffer:()=>new Promise(resolve=>{active++;peak=Math.max(peak,active);gates.push(()=>{active--;resolve(new Uint8Array(65536).fill(value).buffer);});})};}});
 const reader=createSourceBlockReader(),a=reader.read(source(1),0,65536),b=reader.read(source(2),0,65536);
 try{await tick();assert.equal(gates.length,1);gates[0]();await a;await tick();assert.equal(gates.length,2);gates[1]();assert.equal(new Uint8Array(await b)[0],2);assert.equal(peak,1);}
 finally{reader.close();}
});
test('close rejects active and queued requests and fences a late native read',async()=>{
 let release;const source={size:65536,slice:()=>({arrayBuffer:()=>new Promise(r=>release=r)})},reader=createSourceBlockReader();
 const results=Promise.allSettled([reader.read(source,0,65536),reader.read(source,0,65536)]);await tick();reader.close();
 assert.ok((await results).every(r=>r.status==='rejected'));release(new ArrayBuffer(65536));await tick();await assert.rejects(reader.read(source,0,65536),/closed/);
});
test('non-power-of-two chunks stay within a block and clearing discards retained bytes',async()=>{
 const blob=new Blob([new Uint8Array(5*1024*1024)]);let calls=0;
 const source={size:blob.size,slice(start,end){calls++;assert.ok(end-start<=4*1024*1024);return blob.slice(start,end);}},reader=createSourceBlockReader();
 try{assert.equal((await reader.read(source,69,60000)).byteLength,60000);assert.equal(calls,1);reader.clear();await reader.read(source,69,60000);assert.equal(calls,2);}
 finally{reader.close();}
});
test('failed block reads reject coalesced callers without poisoning future reads',async()=>{
 let fail=true;const source={size:65536,slice:()=>({async arrayBuffer(){if(fail)throw Error('read failed');return new ArrayBuffer(65536);}})},reader=createSourceBlockReader();
 try{const results=await Promise.allSettled([reader.read(source,0,65536),reader.read(source,0,65536)]);assert.ok(results.every(r=>r.status==='rejected'));fail=false;assert.equal((await reader.read(source,0,65536)).byteLength,65536);}
 finally{reader.close();}
});
test('interleaved distribution ranges avoid repeatedly fetching full blocks within the same reader budget',async()=>{
 const {createPreparedSendQueue}=await import('../packages/phantom-core/src/file-pipeline.js');
 const size=24*1024*1024,chunk=262144,data=new Uint8Array(size);
 for(let i=0;i<96;i++)data.fill(i,i*chunk,(i+1)*chunk);
 const blob=new Blob([data]),requests=[];let active=0,peak=0;
 const source={size,slice(start,end){requests.push({start,end});assert.ok(end-start<=4194304);return {async arrayBuffer(){active++;peak=Math.max(peak,active);await tick();try{return await blob.slice(start,end).arrayBuffer();}finally{active--;}}};}};
 const reader=createSourceBlockReader(),queue=createPreparedSendQueue({maxPerPeer:32}),order=Array.from({length:32},(_,i)=>[i,32+i,64+i]).flat(),tasks=new Set(),sent=new Set();let cursor=0;
 function admit(){while(cursor<order.length&&tasks.size<32){const index=order[cursor++],task=queue.enqueue('peer',()=>reader.read(source,index,chunk),bytes=>{const view=new Uint8Array(bytes);assert.equal(view.length,chunk);assert.equal(view[0],index);assert.equal(view.at(-1),index);sent.add(index);},Math.floor(index/32));tasks.add(task);task.then(()=>{tasks.delete(task);admit();},()=>tasks.delete(task));}}
 try{admit();while(tasks.size)await Promise.all([...tasks]);assert.equal(sent.size,96);assert.equal(peak,1);assert.ok(requests.reduce((n,r)=>n+r.end-r.start,0)<=size+8*1024*1024,'interleaved reads stay close to useful data rather than the old 104 MiB');}
 finally{queue.close();reader.close();}
});
