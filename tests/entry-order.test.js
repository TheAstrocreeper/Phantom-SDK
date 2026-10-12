import test from 'node:test';import assert from 'node:assert/strict';
import {withNativeEntry} from '../packages/phantom-doorways/src/native-entry.js';
import {createCompositeDoorway} from '../packages/phantom-doorways/src/composite.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {createPhantom} from '../packages/phantom-core/src/index.js';
const authority={room:'network',from:'astro',syscon1:'astro',syscon2:'nova',epoch:3,rosterSeq:1,roster:[{hKey:'astro'},{hKey:'nova'}]};
function fixture(){let c;const sent=[];let opens=0,closes=0;return {sent,get c(){return c;},get opens(){return opens;},get closes(){return closes;},doorway:createDoorway({discovery:true,open(context){opens++;c=context;return{announce(){},reconcile(){},sendSignal:p=>sent.push(p),close(){closes++;}};}})};}
const context={roomId:'network',peerId:'echo',onOpen(){},onSignal(){},onDiscovery(){}};
test('ordered composition opens fallback only after primary loss and fences late packets',()=>{
 const a=fixture(),b=fixture(),hints=[];
 const s=createCompositeDoorway({doorways:[a.doorway,b.doorway],mode:'failover'}).open({...context,onDiscovery:h=>hints.push(h)});
 assert.equal(a.opens,1);assert.equal(b.opens,0);a.c.onOpen();s.announce({peerId:'echo'});a.c.onClose({reason:'lost'});
 assert.equal(b.opens,1);b.c.onOpen();a.c.onDiscovery({leader:'stale'});assert.equal(hints.length,0);
 b.c.onDiscovery({leader:'astro'});assert.equal(hints.length,1);s.close();
});
test('open primary without admission advances, admitted primary stays selected',async()=>{
 for(const admitted of [false,true]){
  const a=fixture(),b=fixture();const s=createCompositeDoorway({doorways:[a.doorway,b.doorway],mode:'failover',admissionTimeoutMs:5}).open(context);
  a.c.onOpen();if(admitted)s.observeNetworkState({state:'admitted'});
  await new Promise(r=>setTimeout(r,15));assert.equal(b.opens,admitted?0:1);s.close();
 }
});
test('real core initiates native ordinary admission despite doorway provisional mirror',async()=>{
 const f=fixture();let offers=0;
 class RTC {
  signalingState='stable';connectionState='new';iceConnectionState='new';
  createDataChannel(){return{readyState:'connecting',send(){},close(){}};}
  async createOffer(){offers++;return{type:'offer',sdp:'fixture'};}
  async setLocalDescription(d){this.localDescription=d;this.signalingState='have-local-offer';}
  close(){this.signalingState='closed';}
 }
 const intro=withNativeEntry(f.doorway);
 const core=createPhantom({doorway:intro,runtime:{crypto:globalThis.crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval}});
 try{
  const self=await core.join({roomId:'network',name:'Echo'});await new Promise(r=>setImmediate(r));f.c.onOpen();
  const a='a'.repeat(16),n='b'.repeat(16);
  f.c.onDiscovery({kind:'concierge_route',roomId:'network',leader:a,mirror:self.hKey,epoch:2,peers:[{hKey:a},{hKey:self.hKey}]});
  for(let i=0;i<20;i++)await Promise.resolve();
  assert.equal(core.snapshot().concierge.role,'peer');assert.ok(offers>0,'Echo owns and starts its ordinary peer tether');
  assert.ok(f.sent.some(p=>p.type==='rtc_offer'&&p.to===a));
 }finally{core.dispose();}
});
