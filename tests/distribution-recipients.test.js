import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity,createAdminAuthorization} from '../packages/phantom-core/src/authorization.js';
import {createAdminLifecycle} from '../packages/phantom-core/src/admin-lifecycle.js';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
import {createMemoryChunkStore} from '../packages/phantom-core/src/files.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const ids=['1111111111111111','2222222222222222','3333333333333333','4444444444444444'];
async function until(check){const end=Date.now()+4000;while(Date.now()<end){if(check())return;await new Promise(r=>setTimeout(r,5));}assert.fail('distribution did not settle');}
async function fixture({holdLateReceiver=false}={}){
 const admin=await createAdminIdentity(),callbacks=new Map(),events=[],frames=[],writes=new Map(ids.map(id=>[id,0]));
 const Base=memoryRTC({onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}return !(holdLateReceiver&&ch.owner===ids[2]&&m.sys==='PHANTOM_FILE_REQUEST'&&m.file.index>0);}});
 const states=ids.map((peerId,i)=>({peerId,leader:ids[0],mirror:ids[1],role:i===0?'syscon1':i===1?'syscon2':'peer',state:'admitted',admitted:true}));
 const ds=[];
 for(const [i,id]of ids.entries()){
  const lifecycle=await createAdminLifecycle({networkId:'room',adminPublicKey:admin.publicKey,adminIdentity:i===0?admin:undefined,store:{load:async()=>[],append:async()=>{}}});
  class RTC extends Base {createDataChannel(label){const ch=super.createDataChannel(label);ch.owner=id;return ch;}fire(type,e={}){if(type==='datachannel')e.channel.owner=id;super.fire(type,e);}}
  const store=createMemoryChunkStore();
  ds.push(createNetworkDistribution({networkId:'room',adminPublicKey:admin.publicKey,lifecycle,runtime:{crypto,RTCPeerConnection:RTC},
   core:{getState:()=>states[i],onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{to,frame}){if(type!=='PHANTOM_DIST_SEND')return;frames.push(structuredClone(frame));queueMicrotask(()=>callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));}},
   options:{intervalMs:10,batchSize:1,jobTimeoutMs:1000,chunkStore:{get:store.get,async put(asset,index,bytes){writes.set(id,writes.get(id)+1);await store.put(asset,index,bytes);}}},emit:(type,data)=>events.push({peer:id,type,data})}));
 }
 function roster(live=ids){for(const id of live)callbacks.get(id)?.({type:'PRESENCE_ROSTER',payload:{list:live.map(hKey=>({hKey}))}});}
 roster();
 return {admin,ds,events,frames,writes,states,
  departPublisher(){ds[0].close();for(const [i,s]of states.entries()){s.leader=ids[1];s.mirror=ids[2];s.role=i===1?'syscon1':i===2?'syscon2':'peer';}holdLateReceiver=false;roster(ids.slice(1));},
  inject(to,body){callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from:states[ids.indexOf(to)].leader,frame:{networkId:'room',from:states[ids.indexOf(to)].leader,to,body}}});},
  close(){for(const d of ds)d.close();}};
}
const file=()=>new Blob([Uint8Array.from({length:65536},(_,i)=>i%251)]);

test('selected recipients alone receive chunks and signed policy survives publisher/leader loss',async()=>{
 const f=await fixture({holdLateReceiver:true});try{
  const manifest=await f.ds[0].publish(file(),{recipients:[ids[2],ids[1],ids[1]]});
  await until(()=>f.events.some(e=>e.peer===ids[1]&&e.type==='distributionComplete')&&f.writes.get(ids[2])>0);
  assert.equal(f.writes.get(ids[3]),0);assert.equal(f.events.some(e=>e.peer===ids[2]&&e.type==='distributionComplete'),false);
  f.departPublisher();
  await until(()=>f.events.some(e=>e.peer===ids[2]&&e.type==='distributionComplete'));
  for(const id of [ids[1],ids[2]]){const complete=f.events.find(e=>e.peer===id&&e.type==='distributionComplete');assert.deepEqual(new Uint8Array(await complete.data.blob.arrayBuffer()),new Uint8Array(await file().arrayBuffer()));}
  assert.equal(f.writes.get(ids[3]),0);
  assert.ok(f.events.some(e=>e.peer===ids[2]&&e.type==='distributionProgress'&&e.data.from===ids[1]));
  assert.ok(f.events.filter(e=>e.type==='distributionJob').every(e=>[ids[1],ids[2]].includes(e.data.receiver)));
  const proof=f.ds[1].snapshot().assets.find(a=>a.id===manifest.id).proof;assert.equal(proof.version,2);assert.deepEqual(proof.recipients,[ids[1],ids[2]]);
 }finally{f.close();}
});

test('omitted recipients keep all-member distribution and an empty list schedules no receivers',async()=>{
 for(const recipients of [undefined,[]]){const f=await fixture();try{
  await f.ds[0].publish(file(),{recipients});
  if(recipients===undefined){await until(()=>f.events.filter(e=>e.type==='distributionComplete').length===3);assert.ok(ids.slice(1).every(id=>f.writes.get(id)>0));}
  else{await new Promise(r=>setTimeout(r,100));assert.equal(f.events.some(e=>e.type==='distributionJob'),false);assert.ok(ids.slice(1).every(id=>f.writes.get(id)===0));}
 }finally{f.close();}}
});

test('transfer endpoints reject a leader-assigned job outside the signed recipient list',async()=>{
 const f=await fixture();try{
  const manifest=await f.ds[0].publish(file(),{recipients:[]}),asset=f.ds[0].snapshot().assets[0];
  const body={type:'job',asset,job:{id:'forged-recipient',source:ids[0],receiver:ids[3],asset:manifest.id,indexes:[0],needsManifest:true,deadline:Date.now()+1000}};
  f.inject(ids[0],body);f.inject(ids[3],body);await new Promise(r=>setTimeout(r,50));
  assert.equal(f.events.some(e=>e.type==='distributionConnectionOpened'&&e.data.jobId==='forged-recipient'),false);assert.equal(f.writes.get(ids[3]),0);
 }finally{f.close();}
});

test('recipient policy is signed, cannot be widened or downgraded, and old proofs stay compatible',async()=>{
 const admin=await createAdminIdentity(),policy=createAdminAuthorization({networkId:'room',adminPublicKey:admin.publicKey});
 const fields={networkId:'room',action:'content.publish',target:'asset',operationId:'op'};
 const proof=await admin.sign({...fields,recipients:[ids[1]]});assert.deepEqual((await policy.verify(proof)).recipients,[ids[1]]);
 const mutable=structuredClone(proof),verification=policy.verify(mutable);mutable.recipients.push(ids[3]);assert.deepEqual((await verification).recipients,[ids[1]],'async verification uses the signed input snapshot');
 for(const altered of [{...proof,recipients:[ids[1],ids[3]]},{...proof,recipients:undefined},{...proof,version:1}])assert.equal(await policy.verify(altered),null);
 const old=await admin.sign(fields);assert.equal(old.version,1);assert.equal((await policy.verify({...old,recipients:[ids[3]]})).recipients,undefined);
 await assert.rejects(admin.sign({...fields,recipients:['invalid']}),/Invalid publication recipients/);
});

test('invalid recipient options and conflicting republishes cannot change existing policy',async()=>{
 const f=await fixture();try{
  for(const recipients of [null,'all',['invalid'],Array(129).fill(ids[1])])await assert.rejects(f.ds[0].publish(file(),{recipients}),/Invalid publication recipients/);
  const manifest=await f.ds[0].publish(file(),{recipients:[]});
  await assert.rejects(f.ds[0].publish(file(),{recipients:[ids[3]]}),/recipients are immutable/);
  const authorization=await f.admin.sign({networkId:'room',action:'content.publish',target:manifest.id,operationId:'external',recipients:[]});
  await assert.rejects(f.ds[0].publish(file(),{authorization,recipients:[ids[3]]}),/do not match/);
  assert.deepEqual(f.ds[0].snapshot().assets[0].proof.recipients,[]);
 }finally{f.close();}
});
