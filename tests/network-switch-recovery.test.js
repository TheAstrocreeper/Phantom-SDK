import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {Socket,rtc} from './fixtures/native-network.js';
import {memoryDoorway} from './fixtures/memory-doorway.js';
import {CandidateRTC} from './fixtures/candidate-rtc.js';
import {ManualClock} from './fixtures/manual-clock.js';
async function scenario(oneSided,outage=90000,custom=false){
 const memory=custom?memoryDoorway({copies:custom===3?3:1}):null;
 const previousPcs=new Set(rtc.keys());
 const clock=new ManualClock(),actualNow=Date.now;Date.now=()=>clock.now;
 let paused=false;const sockets=[],networks=[],events=[];
 class BaseSocket extends Socket{constructor(url){super(url);sockets.push(this);}send(raw){if(!(paused&&(!oneSided||this.phone)))super.send(raw);}receive(packet){if(!(paused&&(!oneSided||this.phone)))super.receive(packet);}}
 class PhoneSocket extends BaseSocket{phone=true;}
 class DualIceRTC extends CandidateRTC{async setLocalDescription(d){await super.setLocalDescription(d);this.onicecandidate?.({candidate:{candidate:this.id,sdpMLineIndex:1}});}}
 class DelayedOpenRTC extends CandidateRTC{asymmetricOpen=true;}
 const runtime={crypto,RTCPeerConnection:custom===4?DelayedOpenRTC:custom===5?DualIceRTC:CandidateRTC,setTimeout:clock.setTimeout,clearTimeout:clock.clearTimeout,setInterval:clock.setInterval,clearInterval:clock.clearInterval};
 const live=n=>n.snapshot().chatPairs?.some(p=>p.dcState==='open');
 async function until(fn,label){for(let i=0;i<100&&!fn();i++)await clock.settle();assert.ok(fn(),label+JSON.stringify(networks.map(n=>({state:n.getState().state,attempt:n.snapshot().concierge.sysAttempt?.state,fault:n.snapshot().concierge.sysAttempt?.faultReason,resources:n.snapshot().recoveryResources,chat:n.snapshot().chatPairs?.map(p=>({pc:p.pcState,state:p.chatState,fault:p.chatFault}))})))+JSON.stringify(events.slice(-25)));}
 async function transfer(bytes){const [sender,receiver]=networks;let offer;const off=receiver.on('fileOffer',row=>{offer=row;});try{const m=await sender.publishFile(new Blob([bytes]),{chunkSize:1024});await sender.shareFile(receiver.getState().peerId,m.id);await until(()=>offer,'file offer');const blob=await receiver.requestFile(m.id,{peers:[sender.getState().peerId]});assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),bytes);}finally{off();}}
 try{
  for(let i=0;i<2;i++){const sdk=createBrowserPhantom({transportDiagnostics:true,WebSocket:i?PhoneSocket:BaseSocket,runtime,...(custom?{doorways:[{type:'memory'}],adapters:{memory:({identity})=>memory.register(identity)}}:{relays:['wss://nostr.fixture/']}),onDoorwayStatus:s=>{if(s.detail?.type==='peer_available')events.push('available_for_'+i);}});const n=i?await sdk.join(networks[0].invite,{name:'Phone',timeoutMs:5000}):await sdk.create({name:'PC'});networks.push(n);n.on('event',e=>{if(e.type==='VEIL_INVITE_RECEIVED')n.acceptPeer(e.payload.from);if(e.type==='SNAPSHOT')events.push(e.payload.reason);});if(i===0)await clock.advance(1);}
  await until(()=>networks.every(n=>n.getState().state==='admitted'),'admission');assert.ok(networks.every(n=>n.snapshot().rtcDiagnostics.events.some(e=>e.event==='candidate_received')));assert.ok(networks.every(n=>n.snapshot().rtcDiagnostics.events.some(e=>e.event==='remote_answer_applied'||e.event==='remote_offer_applied')));assert.equal(networks[0].getState().role,'syscon1');networks[0].invitePeer(networks[1].getState().peerId);await until(()=>networks.every(live),'initial channels');await transfer(new Uint8Array(2048).fill(7));
  paused=true;memory?.pause();
  // In the asymmetric case PC signaling remains online and offers get relay OKs.
  for(const sock of [...sockets])if(!oneSided||sock.phone)sock.close();
  for(const pc of [...rtc.values()])if(pc.connectionState==='connected'){pc.connectionState='failed';pc.iceConnectionState='failed';pc.onconnectionstatechange?.();pc.oniceconnectionstatechange?.();}
  await clock.settle();await clock.advance(outage);
  for(const n of networks){assert.equal(n.snapshot().concierge.membershipAudit.topology.committedCount,2,'no inferred departure after silence');assert.equal(n.getState().state,'recovering');assert.ok(n.snapshot().recoveryResources.queuedSignals<=1024);}
  assert.ok(!events.includes('sys_replacement_terminal_exit_recorded'));
  assert.ok(!events.includes('committed_member_remote_incarnation_transition'),'same-boot transport attempts are not peer reincarnations');
  if(oneSided&&outage>=20000)assert.ok(events.includes('sys_birth_response_deadline_exhausted'),'PC exhausts attempts while phone is offline');
  paused=false;memory?.resume();for(const sock of [...sockets])if(!oneSided||sock.phone)sock.close();await clock.settle();
  await until(()=>networks.every(n=>n.getState().state==='admitted')&&networks.every(live),'channels recover on peer availability');
  if(oneSided&&outage>=20000)assert.ok(events.includes('sys_birth_rearmed_peer_availability'));
  await transfer(new Uint8Array(3072).fill(11));
  for(const n of networks)assert.ok(!n.snapshot().rtcDiagnostics.events.some(e=>e.errorMessage?.includes('meta is not defined')),'queued candidate tracing cannot throw or stop ICE flush');
  if(custom===5)assert.ok(networks.some(n=>n.snapshot().rtcDiagnostics.events.some(e=>e.event==='candidate_queue_flushed'&&e.pendingCandidates>=2&&e.appliedCandidates===e.pendingCandidates&&e.failedCandidates===0)),'all queued candidates are applied');
  if(custom===3)for(const [id,pc]of rtc)if(!previousPcs.has(id))assert.ok((pc.offerApplications||0)<=1,'duplicate delivery must not renegotiate the same attempt');
  for(const n of networks){assert.equal(n.snapshot().concierge.membershipAudit.topology.committedCount,2);assert.equal(n.snapshot().recoveryResources.queuedSignals,0);assert.equal(n.snapshot().recoveryResources.earlyIceCandidates,0);}
 }finally{networks.forEach(n=>n.close());await clock.settle();assert.equal(clock.tasks.size,0,'dispose removes all owned timer tasks');Date.now=actualNow;}
}
test('discovery silence cannot commit departure; both peers recover after 90 seconds',()=>scenario(false));
test('PC exhausts retries before phone returns; availability rearms only the SYS owner',()=>scenario(true));

test('phone availability arriving during the second attempt does not get lost',()=>scenario(true,15000));

test('developer doorway recovers with early ICE and opaque availability IDs',()=>scenario(false,90000,true));

test('duplicate relay offers apply once per transport attempt',()=>scenario(false,90000,3));

test('admission received before local channel-open is applied after transport proof',()=>scenario(false,90000,4));

test('multiple candidates queued before SDP all survive the diagnostic flush',()=>scenario(false,90000,5));
