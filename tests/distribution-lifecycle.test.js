import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
import {createMemoryChunkStore} from '../packages/phantom-core/src/files.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const ids=['1111111111111111','2222222222222222'],leader='3333333333333333';
const pause=ms=>new Promise(r=>setTimeout(r,ms));
async function until(check){const end=Date.now()+2000;while(Date.now()<end){if(check())return;await pause(5);}assert.fail('distribution did not settle');}
async function fixture({reuseConnections=true,prepareTransferTransport,intervalMs=1000,onSend,legacyProtocol=false,receiverStore,assignedRanges=false,scout=false,capabilityExchange=false,controlObservations=null,maxChunkBytes=65536,chunkSize=16384,fileSize=65536,maxMessageSize=262144}={}){
 const admin=await createAdminIdentity(),callbacks=new Map(),events=[],frames=[],RTC=memoryRTC({onSend,maxMessageSize});
 const states=ids.map(peerId=>({peerId,leader,mirror:ids[0],role:'peer',state:'admitted',admitted:true}));
 const deliver=(to,from,body)=>{if(legacyProtocol&&body.type==='signal')body={...body,readyProtocol:undefined};return callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from:leader,frame:{networkId:'room',from,to,body}}});};
 const ds=ids.map((id,i)=>createNetworkDistribution({core:{getState:()=>states[i],...(controlObservations?{scoutPeer:async()=>typeof controlObservations[i]==='function'?controlObservations[i]():controlObservations[i]}:{}),onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{frame}){if(type!=='PHANTOM_DIST_SEND')return;frames.push(frame);queueMicrotask(()=>deliver(frame.to,frame.from,frame.body));}},networkId:'room',adminPublicKey:admin.publicKey,lifecycle:{issue:(action,target)=>admin.sign({networkId:'room',action,target,operationId:crypto.randomUUID()})},runtime:{crypto,RTCPeerConnection:RTC},options:{reuseConnections,prepareTransferTransport:prepareTransferTransport?context=>prepareTransferTransport(context,i):undefined,intervalMs,requestTimeoutMs:1000,assignedRanges,scout,capabilityExchange,maxChunkBytes,...(i===1&&receiverStore?{chunkStore:receiverStore}:{})},emit:(type,data)=>events.push({peer:id,type,data})}));
 for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:[...ids,leader].map(hKey=>({hKey}))}});
 const file=new Blob([Uint8Array.from({length:fileSize},(_,i)=>i%251)]),manifest=await ds[0].publish(file,{chunkSize});
 await ds[1].publish(file,{chunkSize});
 function job(id,source,receiver,indexes,deadlineMs=2000){const job={id,source,receiver,indexes,asset:manifest.id,needsManifest:false,deadline:Date.now()+deadlineMs,...(assignedRanges?{assigned:true}:{})};const body={type:'job',job,asset:{...manifest,proof:frames.find(f=>f.body.type==='inventory').body.proof}};for(const to of [receiver,source])deliver(to,leader,body);}
 return {ds,events,frames,states,job,deliver,file,manifest,close(){ds.forEach(d=>d.close());}};
}
test('advertised large chunks fragment on a smaller data-channel limit and remain verified',async()=>{
 const sizes=[];const f=await fixture({capabilityExchange:true,assignedRanges:true,maxChunkBytes:262144,chunkSize:262144,fileSize:262144+17,maxMessageSize:65536,onSend(data){if(data instanceof ArrayBuffer)sizes.push(data.byteLength);return true;}});
 try{f.ds[0].requestCapabilities(ids[1]);await until(()=>f.ds[0].getCapabilities(ids[1]));f.job('fragmented',ids[0],ids[1],[0,1]);await until(()=>f.frames.some(x=>x.body.type==='done')||f.events.some(x=>x.type==='distributionError'));assert.deepEqual(f.events.filter(x=>x.type==='distributionError'),[]);assert.ok(f.frames.some(x=>x.body.type==='done'));assert.ok(sizes.length>=5);assert.ok(sizes.every(n=>n<=65536));assert.equal(f.events.filter(e=>e.type==='distributionProgress').length,2);}finally{f.close();}
});
test('oversize chunks do not use fragmentation without peer capability',async()=>{
 const sizes=[];const f=await fixture({capabilityExchange:false,assignedRanges:true,maxChunkBytes:262144,chunkSize:262144,fileSize:262144,maxMessageSize:65536,onSend(data){if(data instanceof ArrayBuffer)sizes.push(data.byteLength);return true;}});
 try{f.job('legacy-oversize',ids[0],ids[1],[0]);await until(()=>f.events.some(x=>x.type==='distributionError'));assert.ok(f.events.some(x=>x.type==='distributionError'&&/message limit/.test(x.data.error)));assert.equal(sizes.length,0);}finally{f.close();}
});
test('missing fragment fails the job without claiming a verified chunk',async()=>{
 let dropNext=false,dropped=false;const f=await fixture({capabilityExchange:true,assignedRanges:true,maxChunkBytes:262144,chunkSize:262144,fileSize:262144,maxMessageSize:65536,onSend(data){if(typeof data==='string'){try{if(JSON.parse(data).kind==='binary-fragment'&&!dropped)dropNext=true;}catch{}}else if(dropNext){dropNext=false;dropped=true;return false;}return true;}});
 try{f.ds[0].requestCapabilities(ids[1]);await until(()=>f.ds[0].getCapabilities(ids[1]));f.job('fragment-loss',ids[0],ids[1],[0]);await until(()=>f.frames.some(x=>x.body.type==='failed'));assert.equal(f.events.filter(x=>x.type==='distributionProgress').length,0);assert.ok(f.frames.some(x=>x.body.reason==='invalid_fragment'));}finally{f.close();}
});
test('distribution exchanges compatible capabilities before creating a file job',async()=>{
 const f=await fixture({capabilityExchange:true});
 try{f.ds[0].requestCapabilities(ids[1]);await until(()=>f.ds[0].getCapabilities(ids[1]));assert.equal(f.ds[0].getCapabilities(ids[1]).maxChunkBytes,65536);assert.equal(f.ds[0].getCapabilities(ids[1]).messageLimit.status,'unknown');assert.equal(f.frames.filter(x=>x.body.type==='job').length,0);
  const id=f.ds[0].proposePlan(ids[1]);await until(()=>f.ds[0].getPlan(ids[1])?.id===id);const file=new Blob([new Uint8Array(100000)]);const manifest=await f.ds[0].prepareAgreed(file,ids[1]);assert.equal(manifest.chunkSize,65536);
  await assert.rejects(f.ds[0].publish(file,{prepared:manifest,recipients:[leader]}),/recipient/);
  f.ds[0].dropPeer(ids[1]);assert.equal(f.ds[0].getCapabilities(ids[1]),null);assert.equal(f.ds[0].getPlan(ids[1]),null);
 }finally{f.close();}
});
test('peer control observations inform a plan before any transfer job',async()=>{
 const observation=bytes=>({messageLimit:{status:'known',bytes},route:{localType:'host',remoteType:'srflx',protocol:'udp',rttSeconds:.05,address:'192.0.2.1'},stats:{rows:[{address:'private'}]}});
 const f=await fixture({capabilityExchange:true,maxChunkBytes:262144,controlObservations:[observation(262144),observation(131072)]});
 try{f.ds[0].requestCapabilities(ids[1]);await until(()=>f.ds[0].getCapabilities(ids[1]));const remote=f.ds[0].getCapabilities(ids[1]);assert.equal(remote.messageLimit.bytes,131072);assert.equal(remote.observationScope,'control');assert.equal(JSON.stringify(remote).includes('192.0.2.1'),false);
  const id=f.ds[0].proposePlan(ids[1],{maxPayloadBytes:262144});await until(()=>f.ds[0].getPlan(ids[1])?.id===id);const plan=f.ds[0].getPlan(ids[1]).plan;assert.equal(plan.messageBytes,131072);assert.equal(plan.payloadBytes,131072);assert.equal(plan.observations.local.scope,'control');assert.equal(f.frames.some(x=>x.body.type==='job'),false);
 }finally{f.close();}
});
test('a late observation cannot send capabilities after the peer is removed',async()=>{
 let resolveObservation;const pending=new Promise(resolve=>{resolveObservation=resolve;});
 const f=await fixture({capabilityExchange:true,controlObservations:[pending,null]});
 try{f.ds[0].requestCapabilities(ids[1]);f.ds[0].dropPeer(ids[1]);resolveObservation({messageLimit:{status:'known',bytes:262144}});await pause(20);assert.equal(f.frames.some(x=>x.body.type==='capabilityRequest'),false);assert.equal(f.ds[0].getCapabilities(ids[1]),null);}finally{f.close();}
});
test('both peers can start scouting at the same time',async()=>{
 const observation={messageLimit:{status:'known',bytes:262144},route:null};const f=await fixture({capabilityExchange:true,controlObservations:[observation,observation]});
 try{f.ds[0].requestCapabilities(ids[1]);f.ds[1].requestCapabilities(ids[0]);await until(()=>f.ds[0].getCapabilities(ids[1])&&f.ds[1].getCapabilities(ids[0]));assert.equal(f.ds[0].getCapabilities(ids[1]).messageLimit.bytes,262144);assert.equal(f.ds[1].getCapabilities(ids[0]).messageLimit.bytes,262144);}finally{f.close();}
});
test('a second observation cannot erase a verified one while a plan is proposed',async()=>{
 let calls=0,releaseLocal,releaseRemote;const localPending=new Promise(resolve=>releaseLocal=resolve),remotePending=new Promise(resolve=>releaseRemote=resolve);
 const observation={messageLimit:{status:'known',bytes:262144},route:null};
 const f=await fixture({capabilityExchange:true,maxChunkBytes:262144,controlObservations:[()=>++calls===1?observation:localPending,()=>remotePending]});
 try{
  f.ds[0].requestCapabilities(ids[1]);await until(()=>f.frames.some(x=>x.from===ids[0]&&x.body.type==='capabilityRequest'));
  f.deliver(ids[0],ids[1],{type:'capabilityRequest',version:1});await until(()=>calls===2);
  f.deliver(ids[0],ids[1],{type:'capabilityReport',version:1,report:{version:1,messageLimit:{status:'known',bytes:131072},maxChunkBytes:262144,protocols:['phantom-file-v1'],observationScope:'control'}});
  await until(()=>f.ds[0].getCapabilities(ids[1]));f.ds[0].proposePlan(ids[1],{maxPayloadBytes:262144});
  const proposal=f.frames.find(x=>x.from===ids[0]&&x.body.type==='planProposal');assert.equal(proposal.body.messageBytes,131072);assert.equal(proposal.body.payloadBytes,131072);
 }finally{releaseLocal(observation);releaseRemote(observation);f.close();}
});
test('simultaneous inbound and outbound scouting share the in-flight observation',async()=>{
 let calls=0,release;const pending=new Promise(resolve=>release=resolve),observation={messageLimit:{status:'known',bytes:262144},route:null};
 const f=await fixture({capabilityExchange:true,controlObservations:[()=>{calls++;return pending;},observation]});
 try{f.ds[0].requestCapabilities(ids[1]);f.deliver(ids[0],ids[1],{type:'capabilityRequest',version:1});await pause(10);assert.equal(calls,1);release(observation);await until(()=>f.frames.some(x=>x.from===ids[0]&&x.body.type==='capabilityReport')&&f.frames.some(x=>x.from===ids[0]&&x.body.type==='capabilityRequest'));assert.equal(calls,1);}finally{release(observation);f.close();}
});
test('opt-in scout reports both transfer endpoints without blocking transfer',async()=>{
 const f=await fixture({assignedRanges:true,scout:true});
 try{f.job('scouted',ids[0],ids[1],[0]);await until(()=>f.frames.some(x=>x.body.type==='done')&&f.events.filter(x=>x.type==='distributionScout'&&x.data.phase==='open').length===2);assert.deepEqual(new Set(f.events.filter(x=>x.type==='distributionScout').map(x=>x.data.role)),new Set(['source','receiver']));assert.deepEqual(f.events.filter(x=>x.type==='distributionError'),[]);}finally{f.close();}
});
test('opposite-direction jobs keep requests and binary replies on their assigned channels',async()=>{
 const requests=[];let released=false;
 const f=await fixture({onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}
  if(m.sys==='PHANTOM_FILE_REQUEST'&&!released){requests.push({data,ch});if(requests.length===2){released=true;setTimeout(()=>{for(const {data,ch}of requests)ch.other.onmessage?.({data});},0);}return false;}return true;
 }});
 try{
  f.job('a-to-b',ids[0],ids[1],[0]);f.job('b-to-a',ids[1],ids[0],[1,2,3]);
  await until(()=>f.frames.filter(f=>f.body.type==='done').length===2||f.events.some(e=>e.type==='distributionError'));
  assert.notEqual(requests[0].ch.other,requests[1].ch,'opposite jobs use distinct RTC channels');
  assert.deepEqual(f.events.filter(e=>e.type==='distributionError'),[]);
  assert.equal(f.frames.filter(f=>f.body.type==='done').length,2);
  // A source's normal channel closure must not race the receiver's verified completion.
  assert.equal(f.frames.filter(f=>f.body.type==='failed').length,0);
 }finally{f.close();}
});
test('first periodic tick does not cancel a job already received under current authority',async()=>{
 const held=[];
 const f=await fixture({intervalMs:100,onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}if(m.sys==='PHANTOM_FILE_REQUEST'){held.push({data,ch});return false;}return true;}});
 try{
  f.job('before-first-tick',ids[0],ids[1],[0]);await until(()=>held.length>0);await pause(120);
  for(const {data,ch}of held)ch.other.onmessage?.({data});
  await until(()=>f.frames.some(f=>f.body.type==='done')||f.events.some(e=>e.type==='distributionError'));
  assert.deepEqual(f.events.filter(e=>e.type==='distributionError'),[]);
  assert.equal(f.frames.filter(f=>f.body.type==='done').length,1);
 }finally{f.close();}
});
test('real source transport loss still cancels and reports the receiver job promptly',async()=>{
 let requestChannel;
 const f=await fixture({onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}if(m.sys==='PHANTOM_FILE_REQUEST'){requestChannel=ch;return false;}return true;}});
 try{
  f.job('source-lost',ids[0],ids[1],[0]);await until(()=>requestChannel);
  requestChannel.other.close();
  await until(()=>f.frames.some(f=>f.body.type==='failed'));
  assert.ok(f.frames.some(f=>f.from===ids[1]&&f.body.type==='failed'&&f.body.jobId==='source-lost'));
  assert.ok(f.events.some(e=>e.type==='distributionConnectionFailed'&&e.data.jobId==='source-lost'));
  assert.ok(f.ds.every(d=>d.snapshot().connections.length===0));
 }finally{f.close();}
});
test('assigned jobs on separate connections survive loss of a different range',async()=>{
 let first;
 const f=await fixture({assignedRanges:true,onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}if(m.sys==='PHANTOM_FILE_REQUEST'&&m.file.index===0){first=ch;return false;}return true;}});
 try{
  f.job('range-a',ids[0],ids[1],[0]);await until(()=>first);
  f.job('range-b',ids[0],ids[1],[1]);
  await until(()=>f.frames.some(x=>x.body.type==='done'&&x.body.jobId==='range-b'));
  first.close();await until(()=>f.frames.some(x=>x.body.type==='failed'&&x.body.jobId==='range-a'));
  assert.equal(f.frames.some(x=>x.body.type==='failed'&&x.body.jobId==='range-b'),false);
  assert.ok(f.events.some(x=>x.peer===ids[1]&&x.type==='distributionBatchComplete'&&x.data.jobId==='range-b'));
 }finally{f.close();}
});
test('closing a job during inventory loading prevents its later chunk requests',async()=>{
 const memory=createMemoryChunkStore();let release,entered,channel,requests=0;const gate=new Promise(r=>release=r),inside=new Promise(r=>entered=r);
 const store={...memory,async get(id,index){entered();await gate;return memory.get(id,index);}};
 const f=await fixture({receiverStore:store,onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}if(m.kind==='ready_probe')channel=ch;if(m.sys==='PHANTOM_FILE_REQUEST')requests++;return true;}});
 try{f.job('inventory-lost',ids[0],ids[1],[0]);await inside;assert.ok(channel);channel.close();await until(()=>f.frames.some(x=>x.body.type==='failed'&&x.body.jobId==='inventory-lost'));release();await pause(30);assert.equal(requests,0);assert.ok(f.ds.every(d=>d.snapshot().connections.length===0));}
 finally{release();f.close();}
});
test('a chunk queued during teardown does not block the next job to that peer',async()=>{
 let held;
 const f=await fixture({onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}if(m.sys==='PHANTOM_FILE_REQUEST'&&!held){held={data,ch};return false;}return true;}});
 try{
  f.job('retired-send',ids[0],ids[1],[0]);await until(()=>held);
  // Queue source work, then retire the link before its asynchronous chunk read finishes.
  held.ch.other.onmessage?.({data:held.data});held.ch.close();
  await until(()=>f.frames.some(f=>f.body.type==='failed'));
  const at=Date.now();f.job('next-send',ids[0],ids[1],[1]);
  await until(()=>f.frames.some(f=>f.body.type==='done'&&f.body.jobId==='next-send'));
  assert.ok(Date.now()-at<500,'next job did not wait for the retired send timeout');
 }finally{f.close();}
});
test('loss of the first application frame recovers before starting chunk requests',async()=>{
 let dropped=false;
 const f=await fixture({onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}
  if(!dropped&&(m.kind==='ready_probe'||m.sys==='PHANTOM_FILE_REQUEST')){dropped=true;return false;}return true;
 }});
 try{
  const at=Date.now();f.job('first-frame-boundary',ids[0],ids[1],[0,1,2,3]);
  await until(()=>f.frames.some(f=>f.body.type==='done')||f.events.some(e=>e.type==='distributionError'));
  assert.ok(dropped);
  assert.deepEqual(f.events.filter(e=>e.type==='distributionError'),[]);
  assert.ok(f.frames.some(f=>f.body.type==='done'&&f.body.jobId==='first-frame-boundary'));
  assert.ok(Date.now()-at<500,'readiness recovered before the chunk timeout');
 }finally{f.close();}
});

test('unanswered readiness stops at the job deadline and releases its probe timer',async()=>{
 let probes=0,requests=0;
 const f=await fixture({intervalMs:10,onSend(data){let m;try{m=JSON.parse(data);}catch{return true;}
  if(m.kind==='ready_probe')probes++;
  if(m.sys==='PHANTOM_FILE_REQUEST')requests++;
  return m.kind!=='ready';
 }});
 try{
  f.job('no-source-readiness',ids[0],ids[1],[0],160);
  await until(()=>f.frames.some(f=>f.body.type==='failed'));
  assert.equal(requests,0,'no chunk request before source acknowledgement');
  assert.ok(f.ds.every(d=>d.snapshot().connections.length===0));
  const stopped=probes;await pause(150);assert.equal(probes,stopped,'probe timer released');
 }finally{f.close();}
});
test('duplicate channel-open callbacks do not duplicate endpoint events or readiness probes',async()=>{
 let repeated=false,probes=0;
 const f=await fixture({onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}
  if(m.kind==='ready_probe'){probes++;if(!repeated){repeated=true;queueMicrotask(()=>ch.onopen?.());}}return true;
 }});
 try{
  f.job('duplicate-open',ids[0],ids[1],[0]);await until(()=>f.frames.some(f=>f.body.type==='done'));
  assert.equal(f.events.filter(e=>e.type==='distributionConnectionOpened').length,2);
  assert.equal(probes,1);
 }finally{f.close();}
});

test('peers without readiness capability transfer without the new handshake',async()=>{
 let probes=0;
 const f=await fixture({legacyProtocol:true,onSend(data){let m;try{m=JSON.parse(data);}catch{return true;}if(m.kind==='ready_probe'||m.kind==='ready'){probes++;return false;}return true;}});
 try{f.job('legacy-protocol',ids[0],ids[1],[0]);await until(()=>f.frames.some(f=>f.body.type==='done'));assert.equal(probes,0);assert.deepEqual(f.events.filter(e=>e.type==='distributionError'),[]);}finally{f.close();}
});

test('fresh file sockets wait for preparation in both directions and release exactly once',async()=>{
 const calls=[],released=[],gates=[];let payloads=0;
 const f=await fixture({reuseConnections:false,assignedRanges:true,prepareTransferTransport:(context,side)=>new Promise(resolve=>{calls.push({context,side});gates.push(()=>resolve({report:{buffer:2097152},release:reason=>{released.push({id:context.id,side,reason});return {drops:0};}}));}),onSend:data=>{if(data instanceof ArrayBuffer)payloads++;return true;}});
 try{
  f.job('fresh-forward',ids[0],ids[1],[0]);await until(()=>calls.length===2);assert.equal(payloads,0);assert.equal(calls[0].context.scope,'file_connection');
  gates.splice(0).forEach(resolve=>resolve());await until(()=>released.length===2);assert.ok(payloads>0);
  const before=payloads;f.job('fresh-reverse',ids[1],ids[0],[1]);await until(()=>calls.length===4);assert.equal(payloads,before);
  gates.splice(0).forEach(resolve=>resolve());await until(()=>released.length===4);assert.ok(payloads>before);assert.equal(new Set(released.map(r=>r.id+':'+r.side)).size,4);
  assert.equal(f.events.filter(e=>e.data.phase==='file-native-socket'&&e.data.status==='released').length,4);
 }finally{f.close();}assert.equal(released.length,4);
});
test('closing during fresh socket preparation aborts and releases a late resource',async()=>{
 const calls=[],released=[];const f=await fixture({reuseConnections:false,assignedRanges:true,prepareTransferTransport:context=>new Promise(resolve=>calls.push({context,resolve}))});
 try{f.job('fresh-cancel',ids[0],ids[1],[0]);await until(()=>calls.length===2);f.close();assert.ok(calls.every(row=>row.context.signal.aborted));for(const row of calls)row.resolve({release:()=>{released.push(row.context.id);}});await until(()=>released.length===2);}finally{f.close();}
});
test('unavailable fresh socket preparation continues the existing transfer',async()=>{
 const f=await fixture({reuseConnections:false,assignedRanges:true,prepareTransferTransport:()=>{throw Error('unsupported');}});
 try{f.job('fresh-unknown',ids[0],ids[1],[0]);await until(()=>f.frames.some(x=>x.body.type==='done'));assert.equal(f.events.filter(e=>e.data.phase==='file-native-socket'&&e.data.status==='unavailable').length,2);}finally{f.close();}
});
