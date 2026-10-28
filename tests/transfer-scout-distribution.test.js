import test from 'node:test';
import assert from 'node:assert/strict';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const ids=['1111111111111111','2222222222222222','3333333333333333'];
async function until(predicate){const end=Date.now()+2500;while(!predicate()&&Date.now()<end)await new Promise(r=>setTimeout(r,5));assert.ok(predicate(),'expected state did not arrive');}
async function fixture({thirdLeader=false,legacyLeader=false,legacyReceiver=false,dropScout=false}={}) {
 const members=ids.slice(0,thirdLeader?3:2),leader=thirdLeader?ids[2]:ids[0],admin=await createAdminIdentity();
 const callbacks=new Map(),events=[],frames=[],pcs=[];
 const Base=memoryRTC({onSend(data,ch){if(typeof data==='string'&&ch.label==='phantom-distribution'&&JSON.parse(data).sys==='PHANTOM_FILE_REQUEST')return false;if(dropScout&&data instanceof ArrayBuffer&&ch.label==='phantom-distribution-scout')return false;return true;}});
 class RTC extends Base{constructor(){super();pcs.push(this);}}
 const nodes=members.map((id,i)=>{
  let clock=0,calls=0,serial=0;
  const runtimeCrypto={subtle:crypto.subtle,getRandomValues:crypto.getRandomValues.bind(crypto),randomUUID:()=>`probe-${i}-${serial++}0`};
  return createNetworkDistribution({networkId:'scout-distribution',adminPublicKey:admin.publicKey,runtime:{crypto:runtimeCrypto,RTCPeerConnection:RTC,performance:{now(){const call=calls++;clock+=call%2?[5,10,30,31,11][Math.floor(call/2)%5]:1;return clock;}}},
   core:{getState:()=>({peerId:id,leader,mirror:ids[1],role:id===leader?'syscon1':i===1?'syscon2':'peer',state:'admitted',admitted:true}),
    onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{to,frame}){if(type!=='PHANTOM_DIST_SEND')return;frames.push(frame);if(legacyLeader&&frame.body.type==='rangeCapabilityAck')return;queueMicrotask(()=>callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));}},
   lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'scout-distribution',action,target,...fields,operationId:crypto.randomUUID()})},
   options:{intervalMs:10,jobTimeoutMs:5000,requestTimeoutMs:4000,batchSize:8,requestWindow:8,assignedRanges:true,transferLanes:3,capabilityExchange:true,transferScout:!(legacyReceiver&&i===1),scoutSampleBytes:98304},
   emit:(type,data)=>events.push({i,type,data})});
 });
 for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:members.map(hKey=>({hKey}))}});
 return {nodes,events,frames,pcs,disconnectAtSource(){callbacks.get(ids[0])({type:'PEER_CONNECTION_STATE',payload:{peerId:ids[1],state:'connecting'}});},async scout(){nodes[0].requestCapabilities(ids[1]);nodes[1].requestCapabilities(ids[0]);await until(()=>nodes[0].getCapabilities(ids[1])&&nodes[1].getCapabilities(ids[0]));await until(()=>members.filter(id=>id!==leader).every(id=>frames.some(f=>f.from===id&&f.body.type==='rangeCapability'))&&nodes[thirdLeader?2:0].snapshot().adminChecked);},
  publish:()=>nodes[0].publish(new Blob([new Uint8Array(65536)]),{chunkSize:1024,recipients:[ids[1]]}),close:()=>nodes.forEach(n=>n.close())};
}
for(const thirdLeader of [false,true])test(`a measured single-connection choice constrains the actual scheduler (independent coordinator: ${thirdLeader})`,async()=>{
 const f=await fixture({thirdLeader});try{
  await f.scout();await until(()=>f.events.filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-result').length===2);
  assert.equal(f.nodes[0].getScout(ids[1]).connections,1);assert.equal(f.nodes[0].getScout(ids[1]).enforcement,'coordinator_supported');
  const manifest=await f.publish();await until(()=>f.events.some(e=>e.type==='distributionScout'&&e.data.phase==='preflight-applied'));
  await until(()=>f.nodes[0].snapshot().connections.length===1);await new Promise(r=>setTimeout(r,100));
  const coordinator=f.nodes[thirdLeader?2:0];assert.equal(coordinator.snapshot().jobs.length,1,JSON.stringify(f.events.filter(e=>!['distributionScout','distributionProgress','distributionFileEvent'].includes(e.type))));
  assert.ok(coordinator.snapshot().jobs.every(j=>j.assigned===true&&j.asset===manifest.id));
  assert.ok(f.pcs.filter(pc=>pc.channels.some(ch=>ch.label==='phantom-distribution-scout')).every(pc=>pc.connectionState==='closed'));
  if(thirdLeader)assert.equal(f.events.some(e=>e.i===2&&e.type==='distributionProgress'),false);
 }finally{f.close();}
});
test('an older coordinator keeps the existing setup and is never reported as enforcing a measured choice',async()=>{
 const f=await fixture({thirdLeader:true,legacyLeader:true});try{
  await f.scout();await until(()=>f.events.filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-result').length===2);
  assert.equal(f.nodes[0].getScout(ids[1]).connections,1);assert.equal(f.nodes[0].getScout(ids[1]).effectiveConnections,3);
  await f.publish();await until(()=>f.nodes[2].snapshot().jobs.length===3);
  assert.equal(f.frames.some(f=>f.body.type==='inventory'&&f.body.setup),false);
  assert.equal(f.events.some(e=>e.data.phase==='preflight-applied'),false);
 }finally{f.close();}
});
test('older receivers are not probed and retain the existing parallel setup',async()=>{
 const f=await fixture({legacyReceiver:true});try{await f.scout();assert.equal(f.frames.some(f=>f.body.type==='transferScout'),false);await f.publish();await until(()=>f.nodes[0].snapshot().jobs.length===3);}finally{f.close();}
});
test('selecting a file during a stalled scout cancels it before file associations open',async()=>{
 const f=await fixture({dropScout:true});try{
  await f.scout();await until(()=>f.pcs.some(pc=>pc.channels.some(ch=>ch.label==='phantom-distribution-scout'&&ch.readyState==='open')));
  await f.publish();await until(()=>f.nodes[0].snapshot().jobs.length===3);
  assert.equal(f.nodes[0].getScout(ids[1]),null);assert.ok(f.pcs.filter(pc=>pc.channels.some(ch=>ch.label==='phantom-distribution-scout')).every(pc=>pc.connectionState==='closed'));
 }finally{f.close();}
});

test('a changed peer connection invalidates evidence and removes its scheduling restriction',async()=>{
 const f=await fixture({thirdLeader:true});try{
  await f.scout();await until(()=>f.events.filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-result').length===2);
  await f.publish();await until(()=>f.nodes[2].snapshot().jobs.length===1);
  f.disconnectAtSource();assert.equal(f.nodes[0].getScout(ids[1]),null);
  await until(()=>f.nodes[2].snapshot().jobs.length===3);
 }finally{f.close();}
});
