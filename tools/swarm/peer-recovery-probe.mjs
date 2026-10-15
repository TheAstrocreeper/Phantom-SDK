import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserPhantom} from '../../packages/phantom-doorways/src/browser-entry.js';
import {createPhantom} from '../../packages/phantom-core/src/index.js';
import {createDoorway} from '../../packages/phantom-doorways/src/interface.js';
import {memoryRTC} from '../../tests/fixtures/memory-rtc.js';
const turn=()=>new Promise(r=>setTimeout(r,20));
async function wait(check){const end=Date.now()+65000;while(Date.now()<end){if(check())return;await turn();}throw Error('Recovery condition timed out');}
test('accepted survivor pipes carry SYS recovery during a complete doorway blackout',{timeout:90000},async()=>{
 const members=new Map(),networks=[],events=[];let paused=false,serial=0,epoch=0,one,two;
 function roster(){const keys=[...members.keys()].sort(),leader=keys.includes(one)?one:keys.includes(two)?two:keys[0],mirror=keys.includes(two)&&two!==leader?two:keys.find(k=>k!==leader);if(one!==leader||two!==mirror)epoch++;one=leader;two=mirror;return {kind:'concierge_update',leader,mirror,epoch,peers:[...members.values()].map(m=>m.row)};}
 function announce(){const hint=roster();if(!paused)for(const m of members.values())m.context.onDiscovery(hint);}
 const adapter=()=>createDoorway({discovery:true,presence:'live-sockets',open(context){queueMicrotask(context.onOpen);return {announce(value){members.set(context.peerId,{context,row:{hKey:context.peerId,from:context.peerId,handle:value.handle,bootId:value.bootId,roomJoinedAt:value.t}});announce();},reconcile(){if(!paused)context.onDiscovery(roster());},sendSignal(packet){if(!paused){const target=members.get(packet.to);if(target)queueMicrotask(()=>target.context.onSignal(packet));}},close(){members.delete(context.peerId);announce();}};}});
 const RTC=memoryRTC({emitCandidates:true});
 async function peer(invite){const id=(++serial).toString(16).padStart(8,'0')+'deadbeef'+'0'.repeat(48);
  const sdk=createBrowserPhantom({coreFactory:config=>{const core=createPhantom(config);return {...core,join:options=>core.join({...options,identity:{version:1,id}})};},doorways:[{type:'memory'}],adapters:{memory:adapter},runtime:{crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval}});
  const options={name:'peer-'+serial,timeoutMs:65000,onEvent:e=>events.push(e)};const n=await(invite?sdk.join(invite,options):sdk.create(options));networks.push(n);return n;
 }
 try{
  const seed=await peer();for(let i=0;i<3;i++)await peer(seed.invite);
  console.log('peer recovery: initial peers joined');const survivors=networks.slice(1);await wait(()=>networks.every(n=>n.getState().state==='admitted'));
  for(let i=0;i<3;i++)for(let j=i+1;j<3;j++){
   const a=survivors[i],b=survivors[j];a.invitePeer(b.getState().peerId);
   await wait(()=>{b.acceptPeer(a.getState().peerId);return a.snapshot().chatPairs.some(p=>p.hKey===b.getState().peerId&&p.chatState==='ready'&&p.dcState==='open')&&b.snapshot().chatPairs.some(p=>p.hKey===a.getState().peerId&&p.chatState==='ready'&&p.dcState==='open');});
  }
  console.log('peer recovery: survivor peer mesh ready');const departed=seed.getState().peerId,start=events.length;paused=true;seed.close();
  await wait(()=>survivors.every(n=>n.getState().state==='admitted'&&n.getState().leader!==departed)&&new Set(survivors.map(n=>n.getState().leader)).size===1&&new Set(survivors.map(n=>n.getState().mirror)).size===1);
  assert.ok(events.slice(start).some(e=>e.type==='SNAPSHOT'&&e.payload.reason==='sys_recovery_signal_received_existing_peer_pipe'));
 }catch(error){console.error(JSON.stringify(networks.map(n=>({state:n.getState(),pipes:n.snapshot().chatPairs,recovery:n.snapshot().gossipRecovery})),null,2));throw error;}finally{for(const n of networks)n.close();}
});
