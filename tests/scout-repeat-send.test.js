import test from 'node:test';
import assert from 'node:assert/strict';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {memoryRTC} from './fixtures/memory-rtc.js';

const ids=['1111111111111111','2222222222222222'];
async function until(predicate){
  const end=Date.now()+4000;
  while(!predicate()&&Date.now()<end)await new Promise(resolve=>setTimeout(resolve,5));
  assert.ok(predicate(),'file transfer did not settle');
}

test('successive agreed files, including a reversed send, qualify and use fresh owned associations',async()=>{
  const admin=await createAdminIdentity(),callbacks=new Map(),events=[],frames=[],pcs=[];
  const Base=memoryRTC();
  class RTC extends Base{constructor(){super();pcs.push(this);}}
  const services=ids.map((id,i)=>createNetworkDistribution({
    networkId:'repeat-scout',adminPublicKey:admin.publicKey,runtime:{crypto,RTCPeerConnection:RTC},
    core:{getState:()=>({peerId:id,leader:ids[0],mirror:ids[1],role:i?'syscon2':'syscon1',state:'admitted',admitted:true}),
      onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},
      command(type,{frame}){if(type==='PHANTOM_DIST_SEND'){frames.push(frame);queueMicrotask(()=>callbacks.get(frame.to)?.({type:'PHANTOM_DIST',payload:{from:frame.from,frame}}));}}},
    lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'repeat-scout',action,target,...fields,operationId:crypto.randomUUID()})},
    options:{intervalMs:10,jobTimeoutMs:2000,requestTimeoutMs:1000,batchSize:2,requestWindow:2,
      capabilityExchange:true,transferScout:true,scoutBeforeSend:true,scoutQueueComparison:true,assignedRanges:true,maxChunkBytes:65536},
    emit:(type,data)=>events.push({i,type,data})
  }));
  try{
    for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
    for(let i=0;i<2;i++)services[i].requestCapabilities(ids[1-i]);
    await until(()=>services.every((service,i)=>service.getCapabilities(ids[1-i]))&&services[0].snapshot().adminChecked);
    const scoutIds=[];
    for(const [round,sender]of [0,0,1].entries()){
      const receiver=1-sender,source=services[sender],target=services[receiver];
      const plan=source.proposePlan(ids[receiver]);await until(()=>source.getPlan(ids[receiver])?.id===plan);
      const bytes=Uint8Array.from({length:65536*8+17},(_,i)=>(i+round)%251),file=new Blob([bytes]);
      const eventStart=events.length,frameStart=frames.length;
      const manifest=await source.prepareAgreed(file,ids[receiver]);
      const qualified=events.slice(eventStart).filter(e=>e.i===sender&&e.type==='distributionScout'&&e.data.phase==='preflight-file-workload'&&e.data.status==='complete');
      assert.ok(qualified.length>=4,'every file must compare queues on its own retained associations');
      const scoutId=qualified[0].data.id;assert.ok(!scoutIds.includes(scoutId));scoutIds.push(scoutId);
      await source.publish(file,{prepared:manifest,recipients:[ids[receiver]]});
      await until(()=>events.some(e=>e.i===receiver&&e.type==='distributionComplete'&&e.data.id===manifest.id));
      const complete=events.find(e=>e.i===receiver&&e.type==='distributionComplete'&&e.data.id===manifest.id);
      assert.deepEqual(new Uint8Array(await complete.data.blob.arrayBuffer()),bytes);
      await until(()=>services.every(s=>s.snapshot().connections.length===0)&&services[0].snapshot().jobs.length===0);
      const signals=frames.slice(frameStart).filter(f=>f.body.type==='signal'&&f.body.scoutTransport);
      assert.ok(signals.length>0,'file must claim the newly qualified associations');
      assert.ok(signals.every(f=>f.body.scoutTransport.id===scoutId),'an old scout cannot qualify new file sockets');
      const handoffs=events.slice(eventStart).filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-handoff');
      assert.ok(handoffs.some(e=>e.i===sender&&e.data.status==='claimed'));
      assert.ok(handoffs.some(e=>e.i===receiver&&e.data.status==='bound'));
      assert.ok(pcs.filter(pc=>pc.channels.some(ch=>ch.label==='phantom-distribution')).every(pc=>pc.connectionState==='closed'),'completed file sockets must be released');
    }
    assert.equal(scoutIds.length,3);
    assert.equal(events.some(e=>e.type==='distributionError'),false);
  }finally{services.forEach(service=>service.close());}
});
