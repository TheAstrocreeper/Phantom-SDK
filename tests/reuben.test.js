import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {createReubenTransfer,encodeReubenFrame,decodeReubenFrame} from '../packages/phantom-core/experimental/reuben.js';
import {createMemoryChunkStore} from '../packages/phantom-core/src/files.js';
import {impairedRTC} from '../tools/perf/reuben/impaired-rtc.mjs';
const peer='1111111111111111',id='sha256:'+'ab'.repeat(32);
async function pair(model){const a=new model.RTC(),b=new model.RTC(),dc=a.createDataChannel('reuben');let remote;b.ondatachannel=e=>remote=e.channel;await a.setLocalDescription(await a.createOffer());await b.setRemoteDescription(a.localDescription);await b.setLocalDescription(await b.createAnswer());await a.setRemoteDescription(b.localDescription);await new Promise(r=>setImmediate(r));return {dc,remote,close(){a.close();b.close();model.close();}};}
test('frame carries exact identity, index and payload; rejects malformed lengths and magic',()=>{
 const bytes=Uint8Array.of(4,5,6).buffer,frame=encodeReubenFrame({id,index:42},bytes),decoded=decodeReubenFrame(frame);
 assert.deepEqual(decoded.file,{id,index:42});assert.deepEqual(decoded.bytes,bytes);assert.equal(frame.byteLength,47);
 const bad=frame.slice(0);new DataView(bad).setUint32(8,99);assert.throws(()=>decodeReubenFrame(bad));
 new DataView(bad).setUint32(0,0);assert.throws(()=>decodeReubenFrame(bad));assert.throws(()=>encodeReubenFrame({id,index:-1},bytes));assert.throws(()=>decodeReubenFrame(frame.slice(0,44)));
});
test('rolling requests verify a multi-window file with backpressure and four concurrent writes',{timeout:15000},async()=>{
 const model=impairedRTC({oneWayMs:5,mbps:40}),p=await pair(model),bytes=Uint8Array.from({length:2*1024*1024+19},(_,i)=>i%251),store=createMemoryChunkStore();let resolve,active=0,peak=0,received=0;const complete=new Promise(r=>resolve=r);
 const source=createReubenTransfer({maxMessageSize:262144,channel:p.dc,peerId:peer,highWaterMark:256*1024,lowWaterMark:64*1024});
 const receiver=createReubenTransfer({maxMessageSize:262144,channel:p.remote,peerId:peer,chunkStore:{get:store.get,async put(...args){active++;peak=Math.max(peak,active);await new Promise(r=>setTimeout(r,2));await store.put(...args);active--; }},emit:(type,data)=>{if(type==='fileOffer')resolve(data.manifest);if(type==='fileProgress')received++;}});
 try{const m=await source.publish(new Blob([bytes]),{chunkSize:65536});await source.share(peer,m.id);await complete;const blob=await receiver.request(m.id,{peers:[peer]});assert.equal(received,33);assert.ok(peak<=4);assert.equal(createHash('sha256').update(new Uint8Array(await blob.arrayBuffer())).digest('hex'),createHash('sha256').update(bytes).digest('hex'));assert.ok(model.metrics()[0].peakBufferedBytes<=256*1024+1024);}finally{source.close();receiver.close();p.close();}
});
test('channel closure rejects an active request and releases pending work',{timeout:10000},async()=>{
 const model=impairedRTC({oneWayMs:5,mbps:10}),p=await pair(model);let offer;const ready=new Promise(r=>offer=r);
 const a=createReubenTransfer({maxMessageSize:262144,channel:p.dc,peerId:peer}),b=createReubenTransfer({maxMessageSize:262144,channel:p.remote,peerId:peer,emit:(type,data)=>{if(type==='fileOffer')offer(data.manifest);}});
 try{const m=await a.publish(new Blob([new Uint8Array(1048576)]),{chunkSize:65536});await a.share(peer,m.id);await ready;const pending=b.request(m.id,{peers:[peer]});p.dc.close();await assert.rejects(pending);}finally{a.close();b.close();p.close();}
});
test('unreliable or unordered channels are rejected',()=>{assert.throws(()=>createReubenTransfer({maxMessageSize:262144,channel:{addEventListener(){},send(){},ordered:false},peerId:peer}));});
test('verified stored chunks resume without being requested again',{timeout:15000},async()=>{
 const model=impairedRTC({oneWayMs:2,mbps:60}),p=await pair(model),bytes=Uint8Array.from({length:1024*1024+7},(_,i)=>i%251),store=createMemoryChunkStore(),committed=new Set();let offered,writes=0;
 const ready=new Promise(r=>offered=r),a=createReubenTransfer({maxMessageSize:262144,channel:p.dc,peerId:peer});
 const durable={get:store.get,async put(file,index,data){writes++;await store.put(file,index,data);committed.add(index);},getVerifiedIndexes:()=>[...committed],async getFile(file,m){const parts=[];for(let i=0;i<m.total;i++)parts.push(await store.get(file,i));return new Blob(parts);}};
 const b=createReubenTransfer({maxMessageSize:262144,channel:p.remote,peerId:peer,chunkStore:durable,emit:(type,data)=>{if(type==='fileOffer')offered(data.manifest);}});
 try{const m=await a.publish(new Blob([bytes]),{chunkSize:65536});for(let i=0;i<8;i++){await store.put(m.id,i,bytes.slice(i*65536,(i+1)*65536).buffer);committed.add(i);}await a.share(peer,m.id);await ready;const file=await b.request(m.id,{peers:[peer]});assert.equal(writes,m.total-8);assert.deepEqual(new Uint8Array(await file.arrayBuffer()),bytes);assert.equal(model.metrics()[0].binaryMessages,m.total-8);}finally{a.close();b.close();p.close();}
});
test('corrupted payload fails verification instead of completing',{timeout:15000},async()=>{
 const model=impairedRTC({oneWayMs:2,mbps:50}),p=await pair(model);const send=p.dc.send.bind(p.dc);
 p.dc.send=data=>{if(data instanceof ArrayBuffer){data=data.slice(0);new Uint8Array(data)[44]^=1;}return send(data);};
 let offer;const ready=new Promise(r=>offer=r),a=createReubenTransfer({maxMessageSize:262144,channel:p.dc,peerId:peer}),b=createReubenTransfer({maxMessageSize:262144,channel:p.remote,peerId:peer,emit:(type,data)=>{if(type==='fileOffer')offer(data.manifest);}});
 try{const m=await a.publish(new Blob([new Uint8Array(65536)]),{chunkSize:65536});await a.share(peer,m.id);await ready;await assert.rejects(b.request(m.id,{peers:[peer]}));}finally{a.close();b.close();p.close();}
});
test('default framing fits a 64 KiB negotiated message limit',async()=>{
 class Channel extends EventTarget {readyState='open';send(){}}
 const transfer=createReubenTransfer({channel:new Channel(),peerId:peer});
 try{const manifest=await transfer.publish(new Blob([new Uint8Array(70000)]));assert.equal(manifest.chunkSize,65536-44);assert.throws(()=>transfer.publish(new Blob(),{chunkSize:65536}),/message limit/);}finally{transfer.close();}
});
test('sender receives a verified completion receipt independently of the data pump',{timeout:10000},async()=>{
 const model=impairedRTC({oneWayMs:3,mbps:50}),p=await pair(model);let offer,receipt;
 const ready=new Promise(r=>offer=r),ack=new Promise(r=>receipt=r);
 const a=createReubenTransfer({channel:p.dc,peerId:peer,emit:(type,data)=>{if(type==='reubenReceipt')receipt(data);}}),b=createReubenTransfer({channel:p.remote,peerId:peer,emit:(type,data)=>{if(type==='fileOffer')offer(data.manifest);}});
 try{const m=await a.publish(new Blob([new Uint8Array(70000)]));await a.share(peer,m.id);await ready;const file=await b.request(m.id,{peers:[peer]});assert.equal(file.size,70000);assert.deepEqual(await ack,{id:m.id,total:m.total,bytes:m.size});}finally{a.close();b.close();p.close();}
});
