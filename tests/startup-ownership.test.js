import test from 'node:test';
import assert from 'node:assert/strict';
import {webcrypto} from 'node:crypto';
import {createPhantom} from '../packages/phantom-core/src/index.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {withNativeEntry} from '../packages/phantom-doorways/src/native-entry.js';
const leader='c7dae1bca9eea49c',mirror='c44bcafa56a10013',ordinary='eeeeeeeeeeeeeeee',room='a'.repeat(64);
const applied=[],instances=[];
class RTC {
 constructor(){instances.push(this);this.connectionState='new';this.iceConnectionState='new';this.signalingState='stable';}
 createDataChannel(label){return {label,readyState:'connecting',bufferedAmount:0,close(){},send(){}};}
 async getStats(){assert.notEqual(this.connectionState,'closed');return new Map([['pair',{id:'pair',type:'candidate-pair',state:'in-progress',requestsSent:2,responsesReceived:0}]]);}
 async addIceCandidate(c){assert.notEqual(this.connectionState,'closed');applied.push(c.candidate);}
 async createOffer(){return {type:'offer',sdp:'stub-offer'};}
 async createAnswer(){return {type:'answer',sdp:'stub-answer'};}
 async setLocalDescription(s){this.localDescription=s;this.signalingState=s.type==='offer'?'have-local-offer':'stable';}
 async setRemoteDescription(s){this.remoteDescription=s;this.signalingState=s.type==='offer'?'have-remote-offer':'stable';}
 close(){this.connectionState='closed';}
}
async function settle(){for(let i=0;i<10;i++)await new Promise(setImmediate);}
async function fixture(self,timers={}){
 const packets=[];let wire;
 const peers=[leader,mirror,...(self===ordinary?[ordinary]:[])].map((hKey,i)=>({hKey,handle:'Peer',bootId:hKey,roomJoinedAt:i+1}));
 const raw=createDoorway({discovery:true,open(c){wire=c;queueMicrotask(()=>c.onOpen());return {announce(){c.onDiscovery({kind:'concierge_route',roomId:room,epoch:2,leader,mirror,peers});},reconcile(){},sendSignal(p){packets.push(p)},close(){}};}});
 const core=createPhantom({doorway:withNativeEntry(raw),runtime:{crypto:webcrypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval,...timers}});
 await core.join({roomId:room,handle:'@Peer',identityHash:self+'0'.repeat(48)});await settle();
 return {core,packets,get wire(){return wire;}};
}
test('candidate mirror waits for the leader without promoting itself',async()=>{
 const f=await fixture(mirror);try{
  assert.equal(f.packets.filter(p=>p.type==='rtc_offer').length,0);
  const s=f.core.snapshot();assert.equal(s.concierge.localRoleCandidate,'peer');assert.equal(s.concierge.syscon2,null);
  assert.equal(s.concierge.sysLastErr,'sys_preflight_acceptor_waiting_for_owner');
 }finally{f.core.dispose();}
});
test('leader alone opens the candidate mirror edge, which answers the offer',async()=>{
 const a=await fixture(leader),b=await fixture(mirror);try{
  const offers=a.packets.filter(p=>p.type==='rtc_offer'&&p.to===mirror);assert.equal(offers.length,1);
  b.wire.onSignal(offers[0]);await settle();
  assert.equal(b.packets.filter(p=>p.type==='rtc_offer').length,0);
  assert.equal(b.packets.filter(p=>p.type==='rtc_answer'&&p.to===leader).length,1);
 }finally{a.core.dispose();b.core.dispose();}
});
test('ordinary peers still open their own admission tether',async()=>{
 const f=await fixture(ordinary);try{assert(f.packets.some(p=>p.type==='rtc_offer'&&p.to===leader));}finally{f.core.dispose();}
});

test('queued ICE survives the incoming owner offer',async()=>{
 const a=await fixture(leader),b=await fixture(mirror);try{
  const offer=a.packets.find(p=>p.type==='rtc_offer'&&p.to===mirror);
  const candidate={candidate:'candidate:1 1 UDP 1 192.0.2.1 1234 typ host',sdpMid:'0',sdpMLineIndex:0};
  b.wire.onSignal(offer);
  b.wire.onSignal({...offer,type:'rtc_ice',sdp:undefined,cand:candidate});
  await settle();
  assert(applied.includes(candidate.candidate));
 }finally{a.core.dispose();b.core.dispose();}
});

test('replacement offer keeps new ICE and rejects ICE from the retired attempt',async()=>{
 const a=await fixture(leader),b=await fixture(mirror);try{
  const offer=a.packets.find(p=>p.type==='rtc_offer'&&p.to===mirror);
  b.wire.onSignal(offer);await settle();
  const next={...offer,sysAttemptId:offer.sysAttemptId+'-replacement'};
  const candidate=port=>({candidate:`candidate:1 1 UDP 1 192.0.2.1 ${port} typ host`,sdpMid:'0',sdpMLineIndex:0});
  const old=candidate(1235),fresh=candidate(1236);
  b.wire.onSignal(next);
  b.wire.onSignal({...offer,type:'rtc_ice',sdp:undefined,cand:old});
  b.wire.onSignal({...next,type:'rtc_ice',sdp:undefined,cand:fresh});
  await settle();assert(!applied.includes(old.candidate));assert(applied.includes(fresh.candidate));
 }finally{a.core.dispose();b.core.dispose();}
});
test('a non-owner offer cannot replace an existing SYS connection',async()=>{
 const a=await fixture(leader),b=await fixture(mirror);try{
  const offer=a.packets.find(p=>p.type==='rtc_offer'&&p.to===mirror);
  b.wire.onSignal(offer);await settle();
  const before=instances.length,closed=instances.filter(p=>p.connectionState==='closed').length;
  // The mirror cannot open the leader-owned edge, including with a new boot.
  a.wire.onSignal({...offer,from:mirror,to:leader,bootId:'rogue-boot',sysAttemptId:'rogue-attempt'});
  await settle();assert.equal(instances.length,before);assert.equal(instances.filter(p=>p.connectionState==='closed').length,closed);
 }finally{a.core.dispose();b.core.dispose();}
});

test('core snapshot preserves native state and stats after SDK teardown',async()=>{
 const f=await fixture(leader);f.core.dispose();await settle();
 const records=f.core.snapshot().rtcDiagnostics.connections;
 assert(records.length>0);assert(records.every(r=>r.retired));
 const r=records.find(r=>r.pipe==='sys');assert.equal(r.state.connectionState,'new');assert(r.lastReason.startsWith('before_close:'));assert.equal(r.stats.rows[0].responsesReceived,0);assert.equal(r.statsError,undefined);
});

function clock(){
 let time=0,serial=0;const scheduled=new Map(),intervals=new Set();
 return {timers:{setTimeout(fn,ms){const id=++serial;scheduled.set(id,{fn,at:time+ms});return id;},clearTimeout:id=>scheduled.delete(id),setInterval(){const id=++serial;intervals.add(id);return id;},clearInterval:id=>intervals.delete(id)},
 advance(ms){time+=ms;let count=0;for(;;){const due=[...scheduled].find(([,v])=>v.at<=time);if(!due)return;assert(++count<1000,'timer loop');scheduled.delete(due[0]);due[1].fn();}},pending:()=>scheduled.size+intervals.size};
}
test('elapsed time does not retire a checking SYS birth; native failure initiates recovery',async()=>{
 const c=clock(),before=instances.length,f=await fixture(leader,c.timers);
 try{
  const original=instances[before];original.connectionState='connecting';original.iceConnectionState='checking';
  c.advance(11000);await settle();c.advance(11000);await settle();c.advance(11000);await settle();
  assert.equal(original.connectionState,'connecting');assert.equal(f.packets.filter(p=>p.type==='rtc_offer').length,1);
  original.connectionState='failed';original.onconnectionstatechange();await settle();
  assert.equal(original.connectionState,'closed');assert(f.packets.filter(p=>p.type==='rtc_offer').length>1);
 }finally{f.core.dispose();}
 assert.equal(c.pending(),0,'disposal clears scheduled work');
});
