/** Controlled scheduling experiment. Artificial latency, not a hardware throughput benchmark. */
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {createNetworkDistribution} from '../../packages/phantom-core/src/network-distribution.js';
import {createAdminIdentity} from '../../packages/phantom-core/src/authorization.js';
import {createMemoryChunkStore} from '../../packages/phantom-core/src/files.js';
import {memoryRTC} from '../../tests/fixtures/memory-rtc.js';
const size=Number(process.env.PHANTOM_PROFILE_BYTES??(8*1024*1024+17));
const repeats=Number(process.env.PHANTOM_PROFILE_REPEATS??2);
assert.ok(Number.isSafeInteger(size)&&size>0&&Number.isInteger(repeats)&&repeats>0);
const bytes=Uint8Array.from({length:size},(_,i)=>(i*17+29)%251),source=new Blob([bytes]);
const expected=createHash('sha256').update(bytes).digest('hex');
async function run({requestWindow,batchSize=32,oneWayMs,writeMs}){
 const ids=['1111111111111111','2222222222222222'],callbacks=new Map(),timers=new Set(),metrics=new Map(),errors=[];
 const schedule=(fn,delay)=>{if(!delay)return queueMicrotask(fn);const timer=setTimeout(()=>{timers.delete(timer);fn();},delay);timers.add(timer);};
 const RTC=memoryRTC({onSend(data,ch){schedule(()=>{if(ch.other?.readyState==='open')ch.other.onmessage?.({data});},oneWayMs);return false;}});
 const admin=await createAdminIdentity(),store=createMemoryChunkStore(),committed=new Set();let completed,firstBatchAt,lastBatchAt,batchWorkMs=0,batches=0,peakWrites=0,activeWrites=0;
 const complete=new Promise(resolve=>completed=resolve);
 const receiverStore={get:store.get,async put(id,index,data){activeWrites++;peakWrites=Math.max(peakWrites,activeWrites);try{if(writeMs)await new Promise(r=>schedule(r,writeMs));await store.put(id,index,data);committed.add(index);}finally{activeWrites--; }},getVerifiedIndexes:()=>[...committed],async getFile(id,m){const parts=[];for(let i=0;i<m.total;i++)parts.push(await store.get(id,i));return new Blob(parts,{type:m.type});}};
 const services=ids.map((id,i)=>createNetworkDistribution({networkId:'window-profile',adminPublicKey:admin.publicKey,
  core:{getState:()=>({peerId:id,leader:ids[0],mirror:ids[1],role:i?'syscon2':'syscon1',state:'admitted',admitted:true}),onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{frame}){if(type==='PHANTOM_DIST_SEND')schedule(()=>callbacks.get(frame.to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}),frame.to===id?0:oneWayMs);}},
  lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'window-profile',action,target,...fields,operationId:crypto.randomUUID()})},runtime:{crypto,RTCPeerConnection:RTC},
  options:{intervalMs:10,batchSize,requestWindow:i?requestWindow:8,chunkStore:i?receiverStore:undefined},
  emit:(type,data)=>{if(type==='distributionError')errors.push(data.error);if(i&&type==='distributionBatchStarted'){firstBatchAt??=performance.now();}if(i&&type==='distributionBatchComplete'){lastBatchAt=performance.now();batchWorkMs+=data.elapsedMs;batches++;}if(i&&type==='distributionTransferTiming')for(const [key,m]of Object.entries(data.metrics)){const row=metrics.get(key)||{sum:0,count:0,max:0};row.sum+=m.sum;row.count+=m.count;row.max=Math.max(row.max,m.max);metrics.set(key,row);}if(i&&type==='distributionComplete')completed(data.blob);},
 }));
 for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
 let timeout;
 try{
  const prepared=await services[0].prepare(source,{chunkSize:65536,sourceReadBlockBytes:4*1024*1024}),start=performance.now();await services[0].publish(source,{prepared,recipients:[ids[1]]});
  const blob=await Promise.race([complete,new Promise((_,reject)=>timeout=setTimeout(()=>reject(Error('Profile timeout '+JSON.stringify(errors))),30000))]);
  const elapsedMs=performance.now()-start;assert.equal(createHash('sha256').update(new Uint8Array(await blob.arrayBuffer())).digest('hex'),expected);assert.deepEqual(errors,[]);assert.ok(peakWrites<=4,'Storage concurrency exceeded SDK admission');
  return {requestWindow,sourceWindow:8,batchSize,oneWayMs,writeMs,bytes:size,elapsedMs:Math.round(elapsedMs),batchWorkMs:Math.round(batchWorkMs),interBatchGapMs:Math.round(lastBatchAt-firstBatchAt-batchWorkMs),batches,peakWrites,verified:true,metrics:Object.fromEntries([...metrics].map(([k,v])=>[k,{meanMs:+(v.sum/v.count).toFixed(3),maxMs:+v.max.toFixed(3),count:v.count}]))};
 }finally{clearTimeout(timeout);services.forEach(s=>s.close());for(const t of timers)clearTimeout(t);}
}
console.log(JSON.stringify({experiment:'deterministic memory RTC with imposed per-message latency and write delay; not a LAN speed claim',bytes:size,repeats}));
for(const oneWayMs of [25,100])for(const writeMs of [0,8])for(let repeat=0;repeat<repeats;repeat++)for(const requestWindow of (repeat%2?[32,16,8]:[8,16,32]))console.log(JSON.stringify({repeat,...await run({requestWindow,oneWayMs,writeMs})}));
