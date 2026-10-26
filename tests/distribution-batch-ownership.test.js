import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const ids=['1111111111111111','2222222222222222'];
const turn=()=>new Promise(r=>setImmediate(r));
async function until(fn){const end=Date.now()+3000;while(!fn()){if(Date.now()>end)assert.fail('expected transfer state');await new Promise(r=>setTimeout(r,2));}}
async function pair({legacy=false,holdSave=false}={}){
 const admin=await createAdminIdentity(),callbacks=new Map(),events=[],frames=[],held=[];let holdSecond=!legacy,second=false;
 let rejectSave;const stored=new Map();
 const receiverStore={load:async()=>[],save:async()=>{if(!rejectSave)await new Promise((_,reject)=>{rejectSave=reject;});},async get(id,index){return stored.get(id+index);},async put(id,index,bytes){stored.set(id+index,bytes);}};
 const RTC=memoryRTC({onSend(data,ch){let binaryHeader=false;if(typeof data==='string')try{binaryHeader=JSON.parse(data).kind==='binary';}catch{}if(second&&holdSecond&&(data instanceof ArrayBuffer||binaryHeader)){held.push({data,ch});return false;}return true;}});
 const deliver=(to,from,body)=>callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from,frame:{networkId:'batch-owner',from,to,body}}});
 const ds=ids.map((id,i)=>createNetworkDistribution({networkId:'batch-owner',adminPublicKey:admin.publicKey,runtime:{crypto,RTCPeerConnection:RTC},
  lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'batch-owner',action,target,...fields,operationId:crypto.randomUUID()})},
  core:{getState:()=>({peerId:id,leader:ids[0],mirror:ids[1],role:i?'syscon2':'syscon1',state:'admitted',admitted:true}),onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{frame}){if(legacy&&frame.body.type==='signal')frame={...frame,body:{...frame.body,reuseProtocol:1}};frames.push(frame);queueMicrotask(()=>deliver(frame.to,frame.from,frame.body));}},
  options:{intervalMs:10,batchSize:2,requestWindow:2,...(i&&holdSave?{chunkStore:receiverStore}:{})},emit(type,data){events.push({side:i,type,data});if(i&&type==='distributionBatchStarted'&&data.batch===1)second=true;}}));
 for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
 const bytes=Uint8Array.from({length:4096},(_,i)=>i%251),manifest=await ds[0].publish(new Blob([bytes]),{chunkSize:1024,recipients:[ids[1]]});
 return {ds,events,frames,manifest,bytes,deliver,get saveHeld(){return !!rejectSave;},rejectOldSave(){rejectSave(Error("late retired save"));},async second(){await until(()=>held.filter(row=>row.data instanceof ArrayBuffer).length===2);return ds[0].snapshot().jobs[0];},release(){holdSecond=false;for(const {data,ch}of held)ch.other.onmessage?.({data});},closeTransport(){held[0].ch.close();},close(){ds.forEach(d=>d.close());}};
}
test('stale, unversioned and malformed failures or closes cannot retire a reused batch',async()=>{
 const f=await pair();try{
  const job=await f.second();assert.equal(job.batch,1);
  for(const batch of [0,undefined,-1,2,1.5,'1']){
   f.deliver(ids[0],ids[1],{type:'failed',jobId:job.id,batch,reason:'old_work'});
   for(const to of ids)f.deliver(to,ids[0],{type:'close',jobId:job.id,batch});
  }
  await turn();assert.equal(f.ds[0].snapshot().jobs[0]?.batch,1);assert.ok(f.ds.every(d=>d.snapshot().connections.length===1));
  assert.equal(f.events.some(e=>e.type==='distributionJobFailed'),false);
  f.release();await until(()=>f.events.some(e=>e.side===1&&e.type==='distributionComplete'));
  const complete=f.events.find(e=>e.side===1&&e.type==='distributionComplete');assert.deepEqual(new Uint8Array(await complete.data.blob.arrayBuffer()),f.bytes);
 }finally{f.close();}
});
test('a current-batch failure still retires the live job and both links',async()=>{
 const f=await pair();try{const job=await f.second();f.deliver(ids[0],ids[1],{type:'failed',jobId:job.id,batch:1,reason:'current_failure'});
  await until(()=>f.ds.every(d=>d.snapshot().connections.length===0));assert.equal(f.ds[0].snapshot().jobs.length,0);assert.ok(f.events.some(e=>e.type==='distributionJobFailed'&&e.data.reason==='current_failure'));
 }finally{f.close();}
});
test('physical transport loss after reuse reports its current batch, not the initial batch',async()=>{
 const f=await pair();try{const job=await f.second();f.closeTransport();await until(()=>f.events.some(e=>e.type==='distributionJobFailed'));
  const failures=f.frames.filter(e=>e.body.type==='failed'&&e.body.jobId===job.id);assert.ok(failures.length);assert.ok(failures.every(e=>e.body.batch===1));
  assert.ok(f.events.some(e=>e.type==='distributionConnectionFailed'&&e.data.id===f.manifest.id&&e.data.batch===1));
 }finally{f.close();}
});
test('older reuse capability falls back to fresh jobs and still verifies the full file',async()=>{
 const f=await pair({legacy:true});try{await until(()=>f.events.some(e=>e.side===1&&e.type==='distributionComplete'));
  assert.equal(f.frames.some(e=>e.body.type==='continue'),false);assert.equal(f.events.filter(e=>e.type==='distributionJob').length,2);
  const complete=f.events.find(e=>e.side===1&&e.type==='distributionComplete');assert.deepEqual(new Uint8Array(await complete.data.blob.arrayBuffer()),f.bytes);
  assert.equal(f.events.some(e=>e.type==='distributionJobFailed'),false);
 }finally{f.close();}
});

test('a rejected storage operation from a retired job cannot fail its replacement',async()=>{
 const f=await pair({holdSave:true});try{
  await until(()=>f.saveHeld);const old=f.ds[0].snapshot().jobs[0];
  f.deliver(ids[0],ids[1],{type:'failed',jobId:old.id,batch:0,reason:'retire_for_recovery'});
  const current=await f.second();assert.notEqual(current.id,old.id);const failures=f.frames.filter(e=>e.body.type==='failed').length;
  f.rejectOldSave();await turn();await turn();
  assert.equal(f.ds[0].snapshot().jobs[0]?.id,current.id);
  assert.equal(f.frames.filter(e=>e.body.type==='failed').length,failures);
  assert.equal(f.events.some(e=>e.type==='distributionError'&&e.data.error==='late retired save'),false);
  f.release();await until(()=>f.events.some(e=>e.side===1&&e.type==='distributionComplete'));
 }finally{f.close();}
});
