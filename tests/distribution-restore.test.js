import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserPhantom,createBrowserIdentity} from '../packages/phantom-doorways/src/browser-entry.js';
import {createAdminIdentity} from '../packages/phantom-core/src/index.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {createNetworkDistribution} from '../packages/phantom-core/src/network-distribution.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const ids=['1111111111111111','2222222222222222'];
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function until(check){const end=Date.now()+6000;while(Date.now()<end){if(check())return;await sleep(5);}assert.fail('restoration did not settle');}
function store(){const chunks=new Map(),records=new Map(),local=new Map();return {
 chunks,records,storage:{getItem:k=>local.get(k)??null,setItem:(k,v)=>local.set(k,v),removeItem:k=>local.delete(k)},
 async get(id,i){return chunks.get(`${id}:${i}`)?.slice(0);},async put(id,i,bytes){chunks.set(`${id}:${i}`,bytes.slice(0));},
 async load(networkId){return structuredClone([...records.values()].filter(r=>r.proof.networkId===networkId));},
 async save(networkId,record){assert.equal(record.proof.networkId,networkId);records.set(record.manifest.id,structuredClone(record));}
};}
async function fixture(){
 let phase=0,hold=true;const members=new Map(),requests=[],stores=[store(),store()],networks=[];
 const identities=[createBrowserIdentity(),createBrowserIdentity()];
 const Base=memoryRTC({onSend(data,ch){let m;try{m=JSON.parse(data);}catch{return true;}if(m.sys==='PHANTOM_FILE_REQUEST'&&ch.owner===1){requests.push({phase,index:m.file.index});if(hold&&m.file.index>0)return false;}return true;}});
 const adapter=()=>createDoorway({discovery:true,presence:'live-sockets',open(c){queueMicrotask(c.onOpen);
  const publish=()=>{const keys=[...members.keys()].sort(),p={kind:'concierge_update',epoch:1,leader:keys[0],mirror:keys[1],peers:[...members.values()].map(m=>m.row)};for(const m of members.values())m.context.onDiscovery(p);};
  return {announce(v){members.set(c.peerId,{context:c,row:{hKey:c.peerId,from:c.peerId,bootId:v.bootId,handle:v.handle,roomJoinedAt:v.t}});publish();},reconcile:publish,sendSignal(p){queueMicrotask(()=>members.get(p.to)?.context.onSignal(p));},close(){members.delete(c.peerId);publish();}};
 }});
 const admin=await createAdminIdentity();
 async function open(index,invite){
  class RTC extends Base{createDataChannel(label){const ch=super.createDataChannel(label);ch.owner=index;return ch;}fire(type,e={}){if(type==='datachannel')e.channel.owner=index;super.fire(type,e);}}
  const sdk=createBrowserPhantom({identity:identities[index],...(invite?{}:{adminIdentity:admin}),doorways:[{type:'memory'}],adapters:{memory:adapter},distribution:{intervalMs:10,batchSize:1,chunkStore:stores[index]},
   runtime:{crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval,localStorage:stores[index].storage}});
  const n=await(invite?sdk.join(invite,{name:`Peer ${index}`,timeoutMs:5000}):sdk.create({name:`Peer ${index}`}));networks.push(n);return n;
 }
 const source=await open(0),receiver=await open(1,source.invite);await until(()=>source.getState().state==='admitted');
 return {source,receiver,stores,requests,identities,
  async restart(){const invite=source.invite;for(const n of networks)n.close();networks.length=0;phase++;hold=false;const [a,b]=await Promise.all([open(0,invite),open(1,invite)]);return {source:a,receiver:b};},
  close(){for(const n of networks)n.close();}};
}

for(const corrupt of [false,true])test(`fresh SDK instances resume a partial distribution${corrupt?' and replace corrupt cached bytes':''}`,async()=>{
 const f=await fixture();try{
  const bytes=Uint8Array.from({length:65536},(_,i)=>i%251),manifest=await f.source.distributeFile(new Blob([bytes]),{recipients:[f.receiver.getState().peerId]});
  await until(()=>f.stores[1].chunks.has(`${manifest.id}:0`));assert.ok(f.stores[1].records.has(manifest.id),'manifest and proof retained before receiving chunks');
  if(corrupt)f.stores[1].chunks.set(`${manifest.id}:0`,new Uint8Array(16384).buffer);
  const restored=await f.restart();
  await until(()=>restored.receiver.getDistributionState().assets.some(a=>a.id===manifest.id&&a.holders.some(h=>h.peer===restored.receiver.getState().peerId&&h.indexes.length===manifest.total)));
  const blob=await restored.receiver.getDistributedFile(manifest.id);assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),bytes);
  const requested=f.requests.filter(r=>r.phase===1).map(r=>r.index);assert.ok(requested.includes(1));assert.equal(requested.includes(0),corrupt,'only missing or corrupt chunks requested');
  assert.equal(restored.receiver.getDistributionState().restoration,'ready');
  assert.deepEqual(restored.source.getDistributionState().assets[0].proof.recipients,[restored.receiver.getState().peerId]);
  const saved=JSON.stringify([...f.stores[0].records.values()]);assert.equal(saved.includes(f.identities[0].peerSecret),false);assert.equal(saved.includes(f.identities[0].nostrSecret),false);
  assert.deepEqual(new Uint8Array(await(await restored.source.getDistributedFile(manifest.id)).arrayBuffer()),bytes,'publisher restores without original Blob or signer');
 }finally{f.close();}
});

test('invalid, cross-network, or revoked records are not advertised and storage load failures propagate',async()=>{
 const f=await fixture();try{
  const m=await f.source.distributeFile(new Blob(['stored content']),{recipients:[f.receiver.getState().peerId]});
  const original=structuredClone(f.stores[0].records.get(m.id));
  for(const [record,allowed]of [[{...original,manifest:{...original.manifest,name:'tampered'}},()=>true],[{...original,proof:{...original.proof,networkId:'other'}},()=>true],[original,()=>false]]){
   const events=[],distribution=createNetworkDistribution({networkId:original.proof.networkId,adminPublicKey:original.proof.publicKey,allowed,
    core:{getState:()=>({peerId:ids[0],leader:ids[0],role:'peer'}),onEvent:()=>()=>{},command(){assert.fail('invalid stored inventory sent');}},
    options:{chunkStore:{...f.stores[0],load:async()=>[record]}},emit:(type,payload)=>events.push({type,payload})});
   try{await distribution.restore();assert.deepEqual(distribution.snapshot().assets,[]);assert.ok(events.some(e=>e.type==='distributionRestoreRejected'));}finally{distribution.close();}
  }
  const broken=createNetworkDistribution({networkId:original.proof.networkId,adminPublicKey:original.proof.publicKey,core:{getState:()=>({peerId:ids[0]}),onEvent:()=>()=>{}},options:{chunkStore:{...f.stores[0],load:async()=>{throw Error('storage unavailable');}}}});
  try{await assert.rejects(broken.restore(),/storage unavailable/);assert.equal(broken.snapshot().restoration,'failed');}finally{broken.close();}
 }finally{f.close();}
});

test('metadata write failures prevent publication and receiver chunk requests',async()=>{
 for(const index of [0,1]){const f=await fixture();try{
  f.stores[index].save=async()=>{throw Error('metadata write failed');};
  if(index===0){await assert.rejects(f.source.distributeFile(new Blob(['content'])),/metadata write failed/);assert.ok(f.source.getDistributionState().assets.every(a=>a.holders.length===0));}
  else{const errors=[];f.receiver.on('distributionError',e=>errors.push(e));await f.source.distributeFile(new Blob(['content']));await until(()=>errors.some(e=>/metadata write failed/.test(e.error)));assert.equal(f.requests.length,0);assert.equal(f.stores[1].chunks.size,0);}
 }finally{f.close();}}
});

test('closing during storage load cannot install or advertise restored records',async()=>{
 let resolveLoad;const events=[];
 const distribution=createNetworkDistribution({networkId:'room',core:{getState:()=>({peerId:ids[0],role:'peer'}),onEvent:()=>()=>{},command(){assert.fail('closed instance sent inventory');}},
  options:{chunkStore:{get:async()=>null,put:async()=>{},save:async()=>{},load:()=>new Promise(resolve=>{resolveLoad=resolve;})}},emit:(type,payload)=>events.push({type,payload})});
 const restoring=distribution.restore();assert.equal(distribution.restore(),restoring);distribution.close();resolveLoad([]);
 await assert.rejects(restoring,/Distribution closed/);assert.deepEqual(distribution.snapshot().assets,[]);assert.equal(events.some(e=>e.type==='distributionRestored'),false);
});

test('browser identity reuse is explicit and malformed seeds fail before discovery',()=>{
 const identity=createBrowserIdentity();assert.equal(identity.version,1);assert.match(identity.peerSecret,/^[0-9a-f]{64}$/);assert.match(identity.nostrSecret,/^[0-9a-f]{64}$/);
 for(const seed of [{}, {...identity,version:2}, {...identity,peerSecret:'not a seed'}])assert.throws(()=>createBrowserPhantom({identity:seed}),/Invalid browser identity seed/);
});
