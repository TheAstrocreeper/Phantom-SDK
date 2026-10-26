/** Controlled scheduling experiment. Artificial latency, not a hardware throughput benchmark. */
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {createNetworkDistribution} from '../../packages/phantom-core/src/network-distribution.js';
import {createAdminIdentity} from '../../packages/phantom-core/src/authorization.js';
import {createMemoryChunkStore} from '../../packages/phantom-core/src/files.js';
import {impairedRTC} from './reuben/impaired-rtc.mjs';
import {createReubenTransfer} from '../../packages/phantom-core/experimental/reuben.js';
const size=Number(process.env.PHANTOM_PROFILE_BYTES??(8*1024*1024+17));
const repeats=Number(process.env.PHANTOM_PROFILE_REPEATS??2);
assert.ok(Number.isSafeInteger(size)&&size>0&&Number.isInteger(repeats)&&repeats>0);
const bytes=Uint8Array.from({length:size},(_,i)=>(i*17+29)%251),source=new Blob([bytes]);
const expected=createHash('sha256').update(bytes).digest('hex');
async function baseline({requestWindow=32,batchSize=32,oneWayMs,writeMs,mbps,recoverEvery}){
 const ids=['1111111111111111','2222222222222222'],callbacks=new Map(),timers=new Set(),metrics=new Map(),errors=[];
 const schedule=(fn,delay)=>{if(!delay)return queueMicrotask(fn);const timer=setTimeout(()=>{timers.delete(timer);fn();},delay);timers.add(timer);};
 const model=impairedRTC({oneWayMs,mbps,recoverEvery}),RTC=model.RTC;
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
  const totalMs=performance.now()-start,elapsedMs=performance.now()-firstBatchAt;assert.equal(createHash('sha256').update(new Uint8Array(await blob.arrayBuffer())).digest('hex'),expected);assert.deepEqual(errors,[]);assert.ok(peakWrites<=4,'Storage concurrency exceeded SDK admission');
  return {engine:"current",setupMs:Math.round(firstBatchAt-start),totalMs:Math.round(totalMs),transport:model.metrics(),requestWindow,sourceWindow:8,batchSize,oneWayMs,writeMs,bytes:size,elapsedMs:Math.round(elapsedMs),batchWorkMs:Math.round(batchWorkMs),interBatchGapMs:Math.round(lastBatchAt-firstBatchAt-batchWorkMs),batches,peakWrites,verified:true,metrics:Object.fromEntries([...metrics].map(([k,v])=>[k,{meanMs:+(v.sum/v.count).toFixed(3),maxMs:+v.max.toFixed(3),count:v.count}]))};
 }finally{clearTimeout(timeout);services.forEach(s=>s.close());model.close();for(const t of timers)clearTimeout(t);}
}

async function reuben({oneWayMs,mbps,recoverEvery,writeMs}){
 const model=impairedRTC({oneWayMs,mbps,recoverEvery}),a=new model.RTC(),b=new model.RTC(),dc=a.createDataChannel('reuben');let incoming;
 b.ondatachannel=e=>incoming=e.channel;
 await a.setLocalDescription(await a.createOffer());await b.setRemoteDescription(a.localDescription);await b.setLocalDescription(await b.createAnswer());await a.setRemoteDescription(b.localDescription);await new Promise(r=>setImmediate(r));
 let offered,peakWrites=0,activeWrites=0;const offer=new Promise(r=>offered=r),store=createMemoryChunkStore();
 const sender=createReubenTransfer({maxMessageSize:262144,channel:dc,peerId:'2222222222222222'});
 const receiver=createReubenTransfer({maxMessageSize:262144,channel:incoming,peerId:'1111111111111111',chunkStore:{get:store.get,async put(...args){activeWrites++;peakWrites=Math.max(peakWrites,activeWrites);try{if(writeMs)await new Promise(r=>setTimeout(r,writeMs));await store.put(...args);}finally{activeWrites--;}}},emit:(type,data)=>{if(type==='fileOffer')offered(data.manifest);}});
 try{
  const manifest=await sender.publish(source,{chunkSize:65536,sourceReadBlockBytes:4*1024*1024}),totalStart=performance.now();await sender.share('2222222222222222',manifest.id);await offer;const start=performance.now();
  const blob=await receiver.request(manifest.id,{peers:['1111111111111111']});
  const elapsedMs=performance.now()-start;assert.equal(createHash('sha256').update(new Uint8Array(await blob.arrayBuffer())).digest('hex'),expected);assert.ok(peakWrites<=4);
  return {engine:'reuben',setupMs:Math.round(start-totalStart),totalMs:Math.round(performance.now()-totalStart),bytes:size,elapsedMs:Math.round(elapsedMs),peakWrites,verified:true,transport:model.metrics()};
 }finally{sender.close();receiver.close();a.close();b.close();model.close();}
}
const cases=[{name:'low-latency',oneWayMs:1,mbps:100,writeMs:8,recoverEvery:0},{name:'80ms-rtt',oneWayMs:40,mbps:20,writeMs:8,recoverEvery:0},{name:'200ms-rtt',oneWayMs:100,mbps:20,writeMs:8,recoverEvery:0},{name:'80ms-rtt-recovery',oneWayMs:40,mbps:20,writeMs:8,recoverEvery:8}].filter(row=>!process.env.PHANTOM_REUBEN_CASE||row.name===process.env.PHANTOM_REUBEN_CASE);
assert.ok(cases.length,'Unknown comparison case');
const results=[];
for(const scenario of cases)for(let repeat=0;repeat<repeats;repeat++)for(const engine of (repeat%2?['reuben','current']:['current','reuben'])){
 const row={scenario:scenario.name,repeat,...scenario,...await (engine==='current'?baseline:reuben)(scenario)};results.push(row);console.log(JSON.stringify(row));
}
const summary=cases.map(s=>{const mean=engine=>{const rows=results.filter(r=>r.scenario===s.name&&r.engine===engine);return rows.reduce((n,r)=>n+r.elapsedMs,0)/rows.length;};const currentMs=mean('current'),reubenMs=mean('reuben');return {scenario:s.name,currentMs,reubenMs,elapsedReductionPercent:+((1-reubenMs/currentMs)*100).toFixed(2),speedup:+(currentMs/reubenMs).toFixed(3)};});
const report={model:'Reliable ordered application-message emulator with serialization, propagation, and optional one-RTT recovery stall every eighth binary message. Not SCTP congestion control, real packet loss, NAT, Chrome, or device performance.',bytes:size,repeats,results,summary};
console.log(JSON.stringify({summary},null,2));
if(process.env.PHANTOM_REUBEN_OUTPUT){const {writeFile}=await import('node:fs/promises');await writeFile(process.env.PHANTOM_REUBEN_OUTPUT,JSON.stringify(report,null,2)+'\n');}
