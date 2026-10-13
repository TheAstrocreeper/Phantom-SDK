import test from 'node:test';
import assert from 'node:assert/strict';
import {webcrypto} from 'node:crypto';
import {createPhantom} from '../packages/phantom-core/src/index.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {withNativeEntry} from '../packages/phantom-doorways/src/native-entry.js';
const leader='c7dae1bca9eea49c',mirror='c44bcafa56a10013',ordinary='eeeeeeeeeeeeeeee',room='a'.repeat(64);
class RTC {
 constructor(){this.connectionState='new';this.iceConnectionState='new';this.signalingState='stable';}
 createDataChannel(label){return {label,readyState:'connecting',bufferedAmount:0,close(){},send(){}};}
 async createOffer(){return {type:'offer',sdp:'stub-offer'};}
 async createAnswer(){return {type:'answer',sdp:'stub-answer'};}
 async setLocalDescription(s){this.localDescription=s;this.signalingState=s.type==='offer'?'have-local-offer':'stable';}
 async setRemoteDescription(s){this.remoteDescription=s;this.signalingState=s.type==='offer'?'have-remote-offer':'stable';}
 close(){this.connectionState='closed';}
}
async function settle(){for(let i=0;i<10;i++)await new Promise(setImmediate);}
async function fixture(self){
 const packets=[];let wire;
 const peers=[leader,mirror,...(self===ordinary?[ordinary]:[])].map((hKey,i)=>({hKey,handle:'Peer',bootId:hKey,roomJoinedAt:i+1}));
 const raw=createDoorway({discovery:true,open(c){wire=c;queueMicrotask(()=>c.onOpen());return {announce(){c.onDiscovery({kind:'concierge_route',roomId:room,epoch:2,leader,mirror,peers});},reconcile(){},sendSignal(p){packets.push(p)},close(){}};}});
 const core=createPhantom({doorway:withNativeEntry(raw),runtime:{crypto:webcrypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval}});
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
