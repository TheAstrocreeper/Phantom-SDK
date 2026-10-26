import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
test('a reused job sees newly acquired source pieces without reopening its connection',async()=>{
 const ids=['1111111111111111','2222222222222222','3333333333333333'],leader=ids[2],callbacks=new Map(),events=[],frames=[],held=[];
 const admin=await createAdminIdentity();let hold=false,inventory;
 const RTC=memoryRTC({onSend(data,ch){let header=false;if(typeof data==='string')try{header=JSON.parse(data).kind==='binary';}catch{}if(hold&&(header||data instanceof ArrayBuffer)){held.push({data,ch});return false;}return true;}});
 const ds=ids.map((id,i)=>createNetworkDistribution({networkId:'selection',adminPublicKey:admin.publicKey,runtime:{crypto,RTCPeerConnection:RTC},
  lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'selection',action,target,...fields,operationId:crypto.randomUUID()})},
  core:{getState:()=>({peerId:id,leader,mirror:ids[1],role:i===2?'syscon1':i===1?'syscon2':'peer',state:'admitted',admitted:true}),onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{frame}){
   if(id===ids[0]&&frame.body.type==='inventory'){inventory=frame.body;frame={...frame,body:{...frame.body,indexes:frame.body.indexes.filter(index=>index<4)}};}
   frames.push(frame);queueMicrotask(()=>callbacks.get(frame.to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));
  }},options:{batchSize:2,requestWindow:2,intervalMs:10},emit(type,data){events.push({side:i,type,data});if(i===1&&type==='distributionBatchStarted'&&data.batch===1)hold=true;}}));
 const until=async check=>{const end=Date.now()+3000;while(!check()){if(Date.now()>end)assert.fail('expected transfer state');await new Promise(r=>setTimeout(r,2));}};
 try{
  for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
  const bytes=Uint8Array.from({length:6144},(_,i)=>i%251);await ds[0].publish(new Blob([bytes]),{chunkSize:1024,recipients:[ids[1]]});
  await until(()=>held.filter(frame=>frame.data instanceof ArrayBuffer).length===2);
  const body={...inventory,indexes:[5,4]};callbacks.get(leader)({type:'PHANTOM_DIST',payload:{from:ids[0],frame:{networkId:'selection',from:ids[0],to:leader,body}}});
  await until(()=>frames.some(frame=>frame.body.type==='inventoryAck'&&frame.body.indexes.includes(5)));
  hold=false;for(const {data,ch}of held)ch.other.onmessage?.({data});
  await until(()=>events.some(e=>e.side===1&&e.type==='distributionComplete'));
  const complete=events.find(e=>e.side===1&&e.type==='distributionComplete');assert.deepEqual(new Uint8Array(await complete.data.blob.arrayBuffer()),bytes);
  assert.equal(events.filter(e=>e.type==='distributionJob').length,1);assert.equal(events.filter(e=>e.type==='distributionConnectionOpened').length,2);
  assert.deepEqual(events.filter(e=>e.side===1&&e.type==='distributionBatchComplete').map(e=>e.data.batch),[0,1,2]);
  assert.equal(events.some(e=>e.type==='distributionError'),false);
 }finally{ds.forEach(d=>d.close());}
});
