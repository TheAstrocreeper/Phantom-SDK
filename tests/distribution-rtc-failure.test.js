import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
for(const phase of ['constructor','offer'])test(`distribution ${phase} failure releases job and both endpoints promptly`,async()=>{
 const admin=await createAdminIdentity(),ids=['1111111111111111','2222222222222222'],callbacks=new Map(),events=[],instances=[];let sourceAttempts=0;
 class ReceiverRTC {constructor(){instances.push(this);}close(){this.closed=true;} }
 class SourceRTC extends ReceiverRTC {constructor(){sourceAttempts++;if(phase==='constructor')throw Error('RTC capacity exhausted');super();}createDataChannel(){return {label:'phantom-distribution',close(){this.closed=true;}};}async createOffer(){throw Error('Offer setup failed');}}
 const distributions=ids.map((id,index)=>{
  const core={getState:()=>({peerId:id,leader:ids[0],mirror:ids[1],role:index?'syscon2':'syscon1',state:'admitted',admitted:true}),onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{to,frame}){queueMicrotask(()=>callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));}};
  return createNetworkDistribution({core,networkId:'room',adminPublicKey:admin.publicKey,lifecycle:{issue:(action,target)=>admin.sign({networkId:'room',action,target,operationId:crypto.randomUUID()})},runtime:{crypto,RTCPeerConnection:index?ReceiverRTC:SourceRTC},options:{intervalMs:10,jobTimeoutMs:10000},emit:(type,data)=>events.push({type,data})});
 });
 try{
  for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
  await distributions[0].publish(new Blob([new Uint8Array(32768)]));
  const deadline=Date.now()+2000;while(!events.some(e=>e.type==='distributionJobFailed')&&Date.now()<deadline)await new Promise(r=>setTimeout(r,10));
  const failed=events.find(e=>e.type==='distributionJobFailed');assert.ok(failed,'failure reported without waiting for job deadline');
  assert.equal(failed.data.reason,'rtc_setup_failed');assert.equal(failed.data.retryAfterMs,10000);
  assert.equal(distributions[0].snapshot().jobs.length,0);
  assert.equal(sourceAttempts,1);
  await new Promise(r=>setTimeout(r,20));
  assert.equal(sourceAttempts,1,'failed source pair remains in cooldown');
  assert.ok(instances.every(pc=>pc.closed),'partially constructed and remote endpoints closed');
  assert.ok(instances.every(pc=>pc.ondatachannel===null&&pc.onconnectionstatechange===null&&pc.onicecandidate===null),'connection handlers released');
  assert.ok(distributions.every(d=>d.snapshot().connections.length===0));
 }finally{for(const d of distributions)d.close();}
});
