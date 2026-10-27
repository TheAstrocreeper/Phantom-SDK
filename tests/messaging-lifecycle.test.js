import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createPhantom} from '../packages/phantom-core/src/index.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
async function until(check){const end=Date.now()+4000;while(Date.now()<end){if(check())return;await new Promise(r=>setTimeout(r,5));}assert.fail('messaging did not settle');}
async function fixture(messaging='automatic',{dropFirstOffer=false,dropEveryOffer=false}={}){
 let dropped=false;const offers=[];
 const members=new Map(),networks=[],cores=[],instances=[],events=[],Base=memoryRTC({onSend:data=>{let m;try{m=JSON.parse(data);}catch{return true;}if(m.msg?.type==='rtc_offer'&&m.msg.pipe==='chat'){offers.push(m.msg.chatAttemptId);if(dropEveryOffer||(dropFirstOffer&&!dropped)){dropped=true;return false;}}return true;}});
 class RTC extends Base {constructor(config){super(config);instances.push(this);}}
 const adapter=()=>createDoorway({discovery:true,presence:'live-sockets',open(c){queueMicrotask(c.onOpen);
  const publish=()=>{const keys=[...members.keys()].sort(),value={kind:'concierge_update',epoch:1,leader:keys[0],mirror:keys[1],peers:[...members.values()].map(m=>m.row)};for(const m of members.values())m.context.onDiscovery(value);};
  return {announce(v){members.set(c.peerId,{context:c,row:{hKey:c.peerId,from:c.peerId,bootId:v.bootId,handle:v.handle,roomJoinedAt:v.t}});publish();},reconcile:publish,sendSignal(p){queueMicrotask(()=>members.get(p.to)?.context.onSignal(p));},close(){members.delete(c.peerId);publish();}};
 }});
 async function open(index,invite){const values=new Map(),storage={getItem:k=>values.get(k)??null,setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)};
  const sdk=createBrowserPhantom({messaging,doorways:[{type:'memory'}],adapters:{memory:adapter},runtime:{crypto,RTCPeerConnection:RTC,setTimeout:(fn,ms,...args)=>setTimeout(fn,(dropFirstOffer||dropEveryOffer)&&ms===20000?100:ms,...args),clearTimeout,setInterval,clearInterval,localStorage:storage},
   coreFactory:config=>{const core=createPhantom(config);cores.push(core);return {...core,join:options=>core.join({...options,identity:{version:1,id:String(index+1).repeat(64)}})};}});
  const options={name:`Peer ${index}`,timeoutMs:3000,onEvent:e=>events.push(e)};
  const n=await(invite?sdk.join(invite,options):sdk.create(options));networks.push(n);return n;
 }
 try{const a=await open(0),b=await open(1,a.invite);await until(()=>networks.every(n=>n.getState().state==='admitted'));return {a,b,cores,instances,events,networks,offers,close(){for(const n of networks)n.close();}};}catch(e){for(const n of networks)n.close();throw e;}
}

test('automatic messaging establishes and restores ready channels without application retry or acceptance',async()=>{
 const f=await fixture();try{
  await until(()=>f.networks.every(n=>n.getPeerConnections()[0]?.state==='ready'));
  const changes=[],received=[];f.a.on('peerConnection',p=>changes.push(p));f.b.on('message',p=>received.push(p.text));
  f.a.send(f.b.getState().peerId,'before loss');await until(()=>received.length===1);
  const old=f.a.getPeerConnections()[0].attemptId;
  const channel=f.instances.flatMap(pc=>pc.channels).find(c=>c.label==='six7'&&c.readyState==='open');assert.ok(channel);channel.close();
  await until(()=>changes.some(c=>c.state!=='ready'));
  await until(()=>f.networks.every(n=>n.getPeerConnections()[0]?.state==='ready')&&f.a.getPeerConnections()[0].attemptId!==old);
  f.a.send(f.b.getState().peerId,'after recovery');await until(()=>received.length===2);assert.deepEqual(received,['before loss','after recovery']);
  assert.ok(changes.some(c=>c.state==='ready'));assert.equal(f.events.filter(e=>e.type==='VEIL_INVITE_RECEIVED').length,1,'repair preserves the relationship cycle');
  const copy=f.a.getPeerConnections();copy[0].state='corrupted';assert.equal(f.a.getPeerConnections()[0].state,'ready');
 }finally{f.close();}
 assert.deepEqual(f.a.getPeerConnections(),[]);
});

test('scout reads the actual ready peer connection and refuses removed peers',async()=>{
 const f=await fixture();try{
  await until(()=>f.networks.every(n=>n.getPeerConnections()[0]?.state==='ready'));
  const peer=f.b.getState().peerId,observation=await f.cores[0].scoutPeer(peer);
  assert.equal(observation.messageLimit.bytes,262144);assert.equal(observation.channel.state,'open');
  assert.equal(await f.cores[0].scoutPeer('ffffffffffffffff'),null);
 }finally{f.close();}
});

test('manual messaging keeps invitation consent and never auto-accepts',async()=>{
 const f=await fixture('manual');try{
  assert.ok(f.networks.every(n=>n.getPeerConnections()[0]?.state==='idle'));
  f.a.invitePeer(f.b.getState().peerId);await until(()=>f.events.some(e=>e.type==='VEIL_INVITE_RECEIVED'));
  assert.equal(f.instances.some(pc=>pc.channels.some(c=>c.label==='six7')),false);
  f.b.acceptPeer(f.a.getState().peerId);await until(()=>f.networks.every(n=>n.getPeerConnections()[0]?.state==='ready'));
 }finally{f.close();}
});

test('unanswered messaging offer uses the existing bounded successor attempt',async()=>{
 const f=await fixture('automatic',{dropFirstOffer:true});try{
  await until(()=>f.networks.every(n=>n.getPeerConnections()[0]?.state==='ready'));
  assert.ok(new Set(f.offers).size>=2,'a new native attempt replaced the unanswered offer');
  const received=[];f.b.on('message',m=>received.push(m.text));f.a.send(f.b.getState().peerId,'retry succeeded');await until(()=>received.length===1);assert.equal(received[0],'retry succeeded');
 }finally{f.close();}
});

test('unreachable messaging reports failed after bounded attempts instead of endless dialing',async()=>{
 const f=await fixture('automatic',{dropEveryOffer:true});try{
  const changes=[];f.a.on('peerConnection',p=>changes.push(p));
  await until(()=>f.a.getPeerConnections()[0]?.state==='failed');
  assert.equal(new Set(f.offers).size,2);assert.ok(changes.some(p=>p.state==='failed'));
  await new Promise(r=>setTimeout(r,150));assert.equal(new Set(f.offers).size,2);
 }finally{f.close();}
});

test('a reentrant block cannot be followed by a stale ready notification',async()=>{
 const f=await fixture();try{
  await until(()=>f.networks.every(n=>n.getPeerConnections()[0]?.state==='ready'));
  let blocked=false;const seen=[];
  f.cores[0].onEvent(e=>{if(e.type==='PEER_CONNECTION_STATE'&&e.payload.state==='ready'&&!blocked){blocked=true;f.cores[0].command('BLOCK_PEER',{to:f.b.getState().peerId});}});
  f.cores[0].onEvent(e=>{if(e.type==='PEER_CONNECTION_STATE')seen.push(e.payload.state);});
  f.instances.flatMap(pc=>pc.channels).find(c=>c.label==='six7'&&c.readyState==='open').close();
  await until(()=>blocked&&seen.includes('blocked'));
  assert.equal(seen.slice(seen.lastIndexOf('blocked')+1).includes('ready'),false);
 }finally{f.close();}
});
