import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const ids=['1111111111111111','2222222222222222'];
const turn=()=>new Promise(resolve=>setImmediate(resolve));
async function until(fn){for(let i=0;i<1500;i++){if(fn())return;await new Promise(r=>setTimeout(r,2));}assert.fail('expected transport state');}
for(const config of [{name:'reliable ordered',coalesce:true},{name:'unordered',coalesce:false,ordered:false},{name:'partially reliable',coalesce:false,maxRetransmits:0}])test(`${config.name}: retries respect queue ownership and drain`,async()=>{
 const admin=await createAdminIdentity(),callbacks=new Map(),events=[];let sourceChannel,request,headers=0,binaries=0,hold=false;
 const RTC=memoryRTC({onSend(data,ch){
  ch.ordered=config.ordered??true;ch.maxRetransmits=config.maxRetransmits??null;
  if(typeof data==='string'){
   const m=JSON.parse(data);
   if(m.kind==='message'&&m.sys==='PHANTOM_FILE_REQUEST')request={ch,data};
   if(m.kind==='binary'){sourceChannel=ch;hold=true;headers++;}
  }
  if(ch===sourceChannel&&hold){ch.bufferedAmount+=typeof data==='string'?new TextEncoder().encode(data).byteLength:data.byteLength;if(data instanceof ArrayBuffer)binaries++;return false;}
  return true;
 }});
 const ds=ids.map((id,i)=>createNetworkDistribution({networkId:'queued-retry',adminPublicKey:admin.publicKey,runtime:{crypto,RTCPeerConnection:RTC},
  lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'queued-retry',action,target,...fields,operationId:crypto.randomUUID()})},
  core:{getState:()=>({peerId:id,leader:ids[0],mirror:ids[1],role:i?'syscon2':'syscon1',state:'admitted',admitted:true}),onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{frame}){queueMicrotask(()=>callbacks.get(frame.to)?.({type:'PHANTOM_DIST',payload:{from:frame.from,frame}}));}},
  emit:(type,data)=>events.push({type,data}),options:{fileTimeline:true,intervalMs:10,transferLanes:1,maxConnections:1,batchSize:1,requestWindow:1,requestTimeoutMs:60000,jobTimeoutMs:60000}}));
 try{
  for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
  await ds[0].publish(new Blob([new Uint8Array(1024)]),{chunkSize:1024,recipients:[ids[1]]});
  await until(()=>binaries===1&&request);await turn();
  const retry=()=>request.ch.other.onmessage?.({data:request.data});
  retry();await until(()=>config.coalesce?events.some(e=>e.type==='distributionTimeline'&&e.data.stage==='retry-coalesced'):binaries===2);
  assert.equal(binaries,config.coalesce?1:2);assert.equal(headers,binaries);
  const before=binaries;sourceChannel.bufferedAmount=0;retry();await until(()=>binaries===before+1);assert.equal(headers,binaries,'drained originals remain eligible for retry');
 }finally{ds.forEach(d=>d.close());}
});
