/** Actual file service over an ordered, rate/latency-limited software wire. Not native WebRTC. */
import assert from 'node:assert/strict';
import {createFileDistribution} from '../../packages/phantom-core/src/files.js';
const ids=['a'.repeat(16),'b'.repeat(16)],size=4*1024*1024,chunkSize=65536,rate=8*1024*1024;
async function run(requestWindow,rttMs){
 const callbacks=[new Set(),new Set()],timers=new Set(),events=[];
 const wire=ids.map(()=>({until:0,deliveries:[],lastBinaryEnd:null,idleMs:0,binaryMs:0,peakQueuedBytes:0}));
 function transmit(side,bytes,deliver,binary=false){
  const w=wire[side],at=performance.now(),start=Math.max(at,w.until),end=start+bytes/rate*1000;
  w.until=end;w.peakQueuedBytes=Math.max(w.peakQueuedBytes,(end-at)*rate/1000);
  if(binary){if(w.lastBinaryEnd!==null)w.idleMs+=Math.max(0,start-w.lastBinaryEnd);w.binaryMs+=end-start;w.lastBinaryEnd=end;}
  const entry={ready:false,deliver};w.deliveries.push(entry);
  const timer=setTimeout(()=>{timers.delete(timer);entry.ready=true;while(w.deliveries[0]?.ready)w.deliveries.shift().deliver();},Math.max(0,end+rttMs/2-performance.now()));
  timers.add(timer);
 }
 const fire=(side,event)=>{for(const fn of callbacks[side])fn(event);};
 const cores=ids.map((id,i)=>({onEvent(fn){callbacks[i].add(fn);return()=>callbacks[i].delete(fn);},command(type,p){
  const other=1-i;
  if(type==='SEND_DM')transmit(i,Buffer.byteLength(JSON.stringify(p)),()=>fire(other,{type:'INCOMING_MSG',payload:{...p,from:id}}));
  else if(type==='SEND_FILE_BIN_CHUNK'){
   // Metadata and raw bytes share one ordered stream, as in the SDK.
   transmit(i,Buffer.byteLength(JSON.stringify(p.file)),()=>{});
   transmit(i,p.bytes.byteLength,()=>fire(other,{type:'FILE_BIN_CHUNK',payload:{...p,from:id}}),true);
   queueMicrotask(()=>fire(i,{type:'FILE_BIN_CHUNK_SENT',payload:{...p,ok:true}}));
  }
 }}));
 let offer;const offered=new Promise(r=>offer=r);
 const services=cores.map((core,i)=>createFileDistribution({core,requestWindow,emit(type,data){events.push({side:i,type,data});if(i===1&&type==='fileOffer')offer();}}));
 const bytes=Uint8Array.from({length:size},(_,i)=>i%251);
 try{
  const manifest=await services[0].publish(new Blob([bytes]),{chunkSize});
  await services[0].share(ids[1],manifest.id);await offered;
  const start=performance.now(),blob=await services[1].request(manifest.id,{peers:[ids[0]]}),elapsedMs=performance.now()-start;
  assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),bytes);
  assert.equal(events.filter(e=>e.side===1&&e.type==='fileProgress').length,size/chunkSize);
  assert.equal(events.some(e=>e.type==='fileRetry'||e.type==='fileError'),false);
  const sender=wire[0];
  return {transport:'ordered software wire, actual SDK file service; not native/device evidence',requestWindow,rttMs,configuredBytesPerSecond:rate,bytes:size,
   elapsedMs:Math.round(elapsedMs),verifiedBytesPerSecond:Math.round(size/(elapsedMs/1000)),
   senderGapBetweenBinaryMessagesMs:Math.round(sender.idleMs),senderBinarySerializationMs:Math.round(sender.binaryMs),
   peakSenderQueuedBytes:Math.round(sender.peakQueuedBytes),applicationOutstandingLimitBytes:requestWindow*chunkSize,verified:true};
 }finally{services.forEach(s=>s.close());for(const timer of timers)clearTimeout(timer);}
}
for(const [window,rtt] of [[8,10],[8,200],[32,200],[1,200]])console.log(JSON.stringify(await run(window,rtt)));
