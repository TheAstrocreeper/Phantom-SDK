import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createPhantom,createAdminIdentity} from '../packages/phantom-core/src/index.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {memoryRTC} from './fixtures/memory-rtc.js';

const defaultCore={iceServers:[{urls:'stun:stun.l.google.com:19302'}]};
const turn={iceServers:[{urls:['turn:relay.example:3478?transport=tcp'],username:'test-turn-user',credential:'test-turn-secret'}],iceTransportPolicy:'relay'};
async function until(check){const deadline=Date.now()+4000;while(Date.now()<deadline){if(check())return;await new Promise(r=>setTimeout(r,5));}assert.fail('peer operation did not settle');}

for(const [name,configuration,distributionConfiguration] of [
 ['custom TURN on all connections',turn,{iceServers:[{urls:'stun:overridden.example'}]}],
 ['explicit empty configuration',{},undefined],
 ['omitted configuration preserves defaults',undefined,undefined],
 ['legacy distribution-only configuration',undefined,{iceServers:[{urls:'stun:legacy.example'}]}],
])test(name,async()=>{
 const members=new Map(),packets=[],events=[],instances=[],networks=[],completed=[];
 const BaseRTC=memoryRTC();
 class RTC extends BaseRTC {constructor(config){super();this.configuration=structuredClone(config);instances.push(this);}}
 const adapter=()=>createDoorway({discovery:true,presence:'live-sockets',open(c){queueMicrotask(c.onOpen);
  const publish=()=>{const keys=[...members.keys()].sort(),value={kind:'concierge_update',epoch:1,leader:keys[0],mirror:keys[1],peers:[...members.values()].map(m=>m.row)};for(const m of members.values())m.context.onDiscovery(value);};
  return {announce(value){members.set(c.peerId,{context:c,row:{hKey:c.peerId,from:c.peerId,bootId:value.bootId,handle:value.handle,roomJoinedAt:value.t}});publish();},reconcile:publish,
   sendSignal(packet){packets.push(packet);queueMicrotask(()=>members.get(packet.to)?.context.onSignal(packet));},close(){members.delete(c.peerId);publish();}};
 }});
 const admin=await createAdminIdentity();
 async function open(index,invite){const data=new Map(),storage={getItem:k=>data.get(k)??null,setItem:(k,v)=>data.set(k,v),removeItem:k=>data.delete(k)};
  const identity={version:1,id:String(index+1).repeat(64)};let network;
  const sdk=createBrowserPhantom({adminIdentity:invite?undefined:admin,rtcConfiguration:configuration,
   doorways:[{type:'memory'}],adapters:{memory:adapter},distribution:{intervalMs:10,...(distributionConfiguration?{rtcConfiguration:distributionConfiguration}:{})},
   runtime:{crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval,localStorage:storage},
   coreFactory:config=>{const core=createPhantom(config);return {...core,join:options=>core.join({...options,identity})};}});
  const options={name:`Peer ${index}`,timeoutMs:3000,onEvent:e=>{events.push(e);if(e.type==='VEIL_INVITE_RECEIVED')network?.acceptPeer(e.payload.from);}};
  network=await(invite?sdk.join(invite,options):sdk.create(options));networks.push(network);
  network.on('distributionComplete',p=>completed.push(p));return network;
 }
 try{
  const a=await open(0),b=await open(1,a.invite);await until(()=>networks.every(n=>n.getState().state==='admitted'));
  a.invitePeer(b.getState().peerId);await until(()=>events.filter(e=>e.type==='P2P_CHAT_READY').length>=2);
  const messages=[];b.on('message',m=>messages.push(m));a.send(b.getState().peerId,'configuration test');await until(()=>messages.length===1);
  await a.distributeFile(new Blob(['verified payload']));await until(()=>completed.length===1);assert.equal(await completed[0].blob.text(),'verified payload');
  for(const [label,expected] of [['six7-sys',configuration??defaultCore],['six7',configuration??defaultCore],['phantom-distribution',configuration??distributionConfiguration??{}]]){
   const connections=instances.filter(pc=>pc.channels.some(c=>c.label===label));assert.ok(connections.length>=2,`${label}: both endpoints constructed`);
   for(const pc of connections)assert.deepEqual(pc.configuration,expected,`${label}: constructor receives configuration`);
  }
  const exported=JSON.stringify({invites:networks.map(n=>n.invite),snapshots:networks.map(n=>n.snapshot()),events,packets});
  assert.equal(exported.includes('test-turn-user'),false);assert.equal(exported.includes('test-turn-secret'),false);assert.equal(exported.includes('turn:relay.example'),false);
 }finally{for(const n of networks)n.close();}
});

test('invalid top-level configuration fails before discovery opens',()=>{
 for(const rtcConfiguration of [null,[],false,'turn:relay.example'])assert.throws(()=>createBrowserPhantom({rtcConfiguration}),/rtcConfiguration must be an object/);
});
