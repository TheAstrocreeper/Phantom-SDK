import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {createPhantom} from '../packages/phantom-core/src/index.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
function doorwayBus(){
 const members=new Map();let one,two,epoch=0,routeHint;
 function hint(){const keys=[...members.keys()].sort();const leader=keys.includes(one)?one:keys.includes(two)?two:keys[0];const mirror=keys.includes(two)&&two!==leader?two:keys.find(k=>k!==leader);if(leader!==one||mirror!==two)epoch++;one=leader;two=mirror;return {kind:'concierge_update',epoch,leader:routeHint??one,mirror:routeHint===two?keys.find(k=>k!==routeHint):two,peers:[...members.values()].map(m=>m.row)};}
 function publish(){const value=hint();for(const m of members.values())m.context.onDiscovery(value);}
 const factory=()=>createDoorway({discovery:true,presence:'live-sockets',open(context){queueMicrotask(context.onOpen);return {announce(value){members.set(context.peerId,{context,row:{hKey:context.peerId,from:context.peerId,handle:value.handle,bootId:value.bootId,roomJoinedAt:value.t,endpointId:crypto.randomUUID()}});publish();},reconcile(){context.onDiscovery(hint());},sendSignal(packet){const target=members.get(packet.to);if(target)queueMicrotask(()=>target.context.onSignal(packet));else if(packet.type==='rtc_offer'&&packet.sysCarrier)queueMicrotask(()=>context.onSignal({type:'rtc_undeliverable',signalType:'rtc_offer',target:packet.to,sysCarrier:true,epoch:packet.epoch}));},close(){if(members.get(context.peerId)?.context===context){members.delete(context.peerId);publish();}}};}});
 factory.setRouteHint=key=>{routeHint=key;publish();};return factory;
}
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function wait(check,label){const deadline=Date.now()+65000;while(Date.now()<deadline){if(check())return;await sleep(20);}throw Error('Timed out: '+label);}
test('publisher/leader loss converges with early recovery timers before verified distribution', {timeout:180000},async()=>{
 let mirrorFault=null,faultFired=false,identitySerial=0;
 // Exercise timer wakeups before the wall-clock grace deadline on both elections.
 const recoveryTimer=(fn,ms,...args)=>setTimeout(fn,ms===10000?ms-5:ms,...args);
 const adapter=doorwayBus(),RTC=memoryRTC({emitCandidates:true,onSend:data=>{let packet;try{packet=JSON.parse(data);}catch{return true;}if(mirrorFault&&packet.pendingAuthority?.admission){const departed=mirrorFault;mirrorFault=null;faultFired=true;queueMicrotask(()=>departed.close());return false;}return true;}}),admin=await createAdminIdentity(),networks=[],files=new Map(),events=[],cores=[];
 async function peer(name,invite){const values=new Map(),storage={getItem:k=>values.get(k)??null,setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)};
  const id=(++identitySerial).toString(16).padStart(8,'0')+'deadbeef'+'0'.repeat(48);
  const sdk=createBrowserPhantom({coreFactory:config=>{const core=createPhantom(config),join=core.join;cores.push(core);return {...core,join:options=>join({...options,identity:{version:1,id}})};},...(invite?{}:{adminIdentity:admin}),doorways:[{type:'memory'}],adapters:{memory:adapter},distribution:{},runtime:{crypto,RTCPeerConnection:RTC,setTimeout:recoveryTimer,clearTimeout,setInterval,clearInterval,localStorage:storage}});
  const n=await(invite?sdk.join(invite,{name,timeoutMs:65000,onEvent:e=>events.push({...e,observedAt:Date.now(),observer:name})}):sdk.create({name,onEvent:e=>events.push({...e,observedAt:Date.now(),observer:name})}));networks.push(n);n.on('distributionComplete',data=>files.set(n,data));return n;
 }
 try{
  const seed=await peer('seed'),a=await peer('a',seed.invite),b=await peer('b',seed.invite),c=await peer('c',seed.invite);
  await wait(()=>networks.every(n=>n.getState().state==='admitted'),'initial admission');
  console.log('engine swarm: initial admission');
  const bytes=Uint8Array.from({length:32768},(_,i)=>i%251),manifest=await seed.distributeFile(new Blob([bytes]));
  await wait(()=>[a,b,c].every(n=>files.get(n)?.id===manifest.id),'initial distribution');
  console.log('engine swarm: initial file verified');
  seed.close();const live=[a,b,c],oldLeader=seed.getState().peerId;
  // A developer may receive a new join while existing members recover.
  const joining=peer('late',seed.invite);joining.catch(()=>{});
  await wait(()=>live.every(n=>n.getState().state==='admitted'&&n.getState().leader!==oldLeader)&&new Set(live.map(n=>n.getState().leader)).size===1,'survivors agree on successor');
  console.log('engine swarm: first successor agreed');
  const resumed=events.filter(e=>e.type==='SYS_HANDOFF_OFFER'&&e.payload.action==='resumed');
  assert.ok(resumed.length>0,'leader commit resumed retained offers');
  for(const event of resumed){
    const held=events.find(e=>e.type==='SYS_HANDOFF_OFFER'&&e.payload.action==='held'&&e.payload.peer===event.payload.peer&&e.payload.attemptId===event.payload.attemptId);
    assert.ok(held,'resumption matches the retained attempt');
    assert.ok(event.observedAt-held.observedAt<15000,'resumption happened before the caller retry deadline');
  }
  assert.ok(events.some(e=>e.type==='ICE_TRACE'&&e.payload.stage==='buffered_handoff'),'ICE retained alongside handoff offer');
  assert.ok(RTC.appliedCandidates.length>0);
  const buffered=events.find(e=>e.type==='ICE_TRACE'&&e.payload.stage==='buffered_handoff');
  assert.ok(events.some(e=>e.type==='ICE_TRACE'&&e.observer===buffered.observer&&
    e.payload.key===buffered.payload.key&&['applied','applied_from_buffer'].includes(e.payload.stage)),
    'retained ICE was applied after the retained offer resumed');
  const late=await joining;await wait(()=>files.get(late)?.id===manifest.id,'late distribution');assert.deepEqual(new Uint8Array(await files.get(late).blob.arrayBuffer()),bytes);
  console.log('engine swarm: late file verified');
  const current=[a,b,c,late],leaderId=a.getState().leader;
  const leader=current.find(n=>n.getState().peerId===leaderId);assert.ok(leader);leader.close();
  const remaining=current.filter(n=>n!==leader);
  await wait(()=>remaining.every(n=>n.getState().state==='admitted'&&![oldLeader,leaderId].includes(n.getState().leader))&&new Set(remaining.map(n=>n.getState().leader)).size===1,'second leader replacement');
  console.log('engine swarm: second successor agreed');
  assert.ok(events.some(e=>e.type==='SYS_RECOVERY_TIMER'&&e.payload.action==='rearmed'),
    'early timer wakeup retained the recovery deadline');
  adapter.setRouteHint(remaining.find(n=>n.getState().peerId!==remaining[0].getState().leader).getState().peerId);
  const finalPeer=await peer('after-second-election',seed.invite);await wait(()=>files.get(finalPeer)?.id===manifest.id,'distribution after second election');assert.deepEqual(new Uint8Array(await files.get(finalPeer).blob.arrayBuffer()),bytes);
  const redirect=events.find(e=>e.type==='SYS_BOOTSTRAP_ROUTE'&&e.observer==='after-second-election'&&e.payload.reason==='peer_route_hint');
  assert.ok(redirect,'fresh join followed immediate peer routing advice');
  assert.equal(events.some(e=>e.type==='SYS_SIGNAL_TIMEOUT'&&e.observer==='after-second-election'),false,
    'fresh join did not wait for the unanswered-offer deadline');
  // A surviving transport peer can publish with an explicit admin-signed proof;
  // elected leadership alone must never supply the missing signing authority.
  const publisher=remaining[0],nextBytes=Uint8Array.from({length:32768},(_,i)=>(i+17)%251),file=new Blob([nextBytes]),settings={name:'authorized-survivor.bin'};
  await assert.rejects(publisher.distributeFile(file,settings),/Admin seed required/);
  const prepared=await publisher.publishFile(file,settings);
  const authorization=await admin.sign({networkId:publisher.networkId,action:'content.publish',target:prepared.id,operationId:crypto.randomUUID()});
  const fresh=await publisher.distributeFile(file,{...settings,authorization});assert.equal(fresh.id,prepared.id);
  const receivers=[...remaining.filter(n=>n!==publisher),finalPeer];
  await wait(()=>receivers.every(n=>files.get(n)?.id===fresh.id),'authorized survivor publishes after original admin peer exits');
  for(const n of receivers)assert.deepEqual(new Uint8Array(await files.get(n).blob.arrayBuffer()),nextBytes);
  console.log('engine swarm: authorized survivor publication verified');
  const active=[...remaining,finalPeer],steadyLeader=active[0].getState().leader;
  const departingMirror=active.find(n=>n.getState().peerId===active[0].getState().mirror);assert.ok(departingMirror);
  mirrorFault=departingMirror;
  const interruptedJoin=peer('admission-interrupted-by-mirror-loss',seed.invite);interruptedJoin.catch(()=>{});
  await wait(()=>faultFired,'mirror loss interrupts an already staged admission');
  const secondJoin=peer('concurrent-after-mirror-loss',seed.invite);secondJoin.catch(()=>{});
  const [interrupted,concurrent]=await Promise.all([interruptedJoin,secondJoin]);
  const survivors=[...active.filter(n=>n!==departingMirror),interrupted,concurrent];
  await wait(()=>survivors.every(n=>n.getState().state==='admitted'&&n.getState().leader===steadyLeader&&n.getState().mirror!==departingMirror.getState().peerId),'interrupted and concurrent admissions commit after mirror replacement');
  await wait(()=>[interrupted,concurrent].every(n=>files.get(n)?.id===fresh.id),'new peers download after interrupted admission');
  for(const n of [interrupted,concurrent])assert.deepEqual(new Uint8Array(await files.get(n).blob.arrayBuffer()),nextBytes);
  console.log('engine swarm: staged admission survives mirror replacement');
 }catch(error){error.message+='\nCORE '+JSON.stringify(cores.map(core=>({state:core.getState(),error:String(core.snapshot().concierge?.lastErr??'').slice(0,500),sysError:String(core.snapshot().concierge?.sysLastErr??'').slice(0,500),recovery:core.snapshot().gossipRecovery})))+'\n'+JSON.stringify(networks.map(n=>({state:n.getState(),distribution:n.getDistributionState(),sysError:String(n.snapshot().concierge?.sysLastErr??'').slice(0,300)})));throw error;}finally{for(const n of networks)n.close();}
});
