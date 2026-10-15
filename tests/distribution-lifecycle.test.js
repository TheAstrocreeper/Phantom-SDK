import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const ids=['1111111111111111','2222222222222222'],leader='3333333333333333';
const pause=ms=>new Promise(r=>setTimeout(r,ms));
async function until(check){const end=Date.now()+2000;while(Date.now()<end){if(check())return;await pause(5);}assert.fail('distribution did not settle');}
async function fixture({intervalMs=1000,onSend,legacyProtocol=false}={}){
 const admin=await createAdminIdentity(),callbacks=new Map(),events=[],frames=[],RTC=memoryRTC({onSend});
 const states=ids.map(peerId=>({peerId,leader,mirror:ids[0],role:'peer',state:'admitted',admitted:true}));
 const deliver=(to,from,body)=>{if(legacyProtocol&&body.type==='signal')body={...body,readyProtocol:undefined};return callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from:leader,frame:{networkId:'room',from,to,body}}});};
 const ds=ids.map((id,i)=>createNetworkDistribution({core:{getState:()=>states[i],onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{frame}){if(type!=='PHANTOM_DIST_SEND')return;frames.push(frame);queueMicrotask(()=>deliver(frame.to,frame.from,frame.body));}},networkId:'room',adminPublicKey:admin.publicKey,lifecycle:{issue:(action,target)=>admin.sign({networkId:'room',action,target,operationId:crypto.randomUUID()})},runtime:{crypto,RTCPeerConnection:RTC},options:{intervalMs,requestTimeoutMs:1000},emit:(type,data)=>events.push({peer:id,type,data})}));
 for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:[...ids,leader].map(hKey=>({hKey}))}});
 const file=new Blob([Uint8Array.from({length:65536},(_,i)=>i%251)]),manifest=await ds[0].publish(file);
 await ds[1].publish(file);
 function job(id,source,receiver,indexes,deadlineMs=2000){const job={id,source,receiver,indexes,asset:manifest.id,needsManifest:false,deadline:Date.now()+deadlineMs};const body={type:'job',job,asset:{...manifest,proof:frames.find(f=>f.body.type==='inventory').body.proof}};for(const to of [receiver,source])deliver(to,leader,body);}
 return {ds,events,frames,states,job,close(){ds.forEach(d=>d.close());}};
}
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
