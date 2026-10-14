import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const turn=()=>new Promise(r=>setTimeout(r,20));
async function wait(check){const end=Date.now()+65000;while(Date.now()<end){if(check())return;await turn();}throw Error('Recovery condition timed out');}
test('full SDK verifies sparse bridge identities when doorway signal delivery is blocked',{timeout:90000},async()=>{
 const members=new Map(),networks=[],events=[],connections=new Map();let paused=false,serial=0,epoch=0,one,two;
 function roster(){const keys=[...members.keys()].sort(),leader=keys.includes(one)?one:keys.includes(two)?two:keys[0],mirror=keys.includes(two)&&two!==leader?two:keys.find(k=>k!==leader);if(one!==leader||two!==mirror)epoch++;one=leader;two=mirror;return {kind:'concierge_update',leader,mirror,epoch,peers:[...members.values()].map(m=>m.row)};}
 function announce(){const hint=roster();for(const m of members.values())m.context.onDiscovery(hint);}
 const adapter=({identity})=>createDoorway({discovery:true,presence:'live-sockets',open(context){queueMicrotask(context.onOpen);return {announce(value){members.set(context.peerId,{context,row:{hex:identity.publicKey,hKey:context.peerId,from:context.peerId,handle:value.handle,bootId:value.bootId,roomJoinedAt:value.t}});announce();},reconcile(){context.onDiscovery(roster());},sendSignal(packet){if(!paused){const target=members.get(packet.to);if(target)queueMicrotask(()=>target.context.onSignal(packet));}},close(){members.delete(context.peerId);announce();}};}});
 const RTC=memoryRTC({emitCandidates:true});
 async function peer(invite){serial++;const pcs=[];class ObservedRTC extends RTC{constructor(){super();pcs.push(this);}}
  const sdk=createBrowserPhantom({doorways:[{type:'memory'}],adapters:{memory:adapter},runtime:{crypto,RTCPeerConnection:ObservedRTC,setTimeout,clearTimeout,setInterval,clearInterval}});
  const options={name:'peer-'+serial,timeoutMs:65000,onEvent:e=>events.push(e)};const n=await(invite?sdk.join(invite,options):sdk.create(options));networks.push(n);connections.set(n,pcs);return n;
 }
 try{
  const seed=await peer();for(let i=0;i<3;i++)await peer(seed.invite);await wait(()=>networks.every(n=>n.getState().state==='admitted'));const ordinary=networks.filter(n=>n.getState().role==='peer'),[bridge,requester]=ordinary,pairs=[[bridge,seed],[requester,bridge]];assert.ok(bridge&&requester);
  for(const [a,b] of pairs){a.invitePeer(b.getState().peerId);
   await wait(()=>{b.acceptPeer(a.getState().peerId);return a.snapshot().chatPairs.some(p=>p.hKey===b.getState().peerId&&p.chatState==='ready'&&p.dcState==='open')&&b.snapshot().chatPairs.some(p=>p.hKey===a.getState().peerId&&p.chatState==='ready'&&p.dcState==='open');});
  }
  const start=events.length;paused=true;const system=connections.get(requester).find(pc=>pc.connectionState==='connected'&&pc.channels.some(ch=>ch.label==='six7-sys'));assert.ok(system);system.close();
  await wait(()=>events.slice(start).some(e=>e.type==='SYS_SIGNAL_BRIDGE'&&e.payload.action==='received'&&e.payload.to===seed.getState().peerId));
  assert.ok(events.slice(start).some(e=>e.type==='SYS_SIGNAL_BRIDGE'&&e.payload.action==='forwarded'));
  await wait(()=>events.slice(start).some(e=>e.type==='SYS_SIGNAL_BRIDGE'&&e.payload.action==='received'&&e.payload.signalType==='rtc_answer'&&e.payload.to===requester.getState().peerId)&&requester.getState().state==='admitted'&&connections.get(requester).some(pc=>pc!==system&&pc.connectionState==='connected'&&pc.channels.some(ch=>ch.label==='six7-sys')));
 }catch(error){console.error('Bridge diagnostics',events.filter(e=>e.type==='SYS_SIGNAL_BRIDGE').map(e=>e.payload));console.error(JSON.stringify(networks.map(n=>({state:n.getState(),pipes:n.snapshot().chatPairs,recovery:n.snapshot().gossipRecovery})),null,2));throw error;}finally{for(const n of networks)n.close();}
});
