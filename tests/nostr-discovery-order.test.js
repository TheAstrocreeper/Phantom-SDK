import test from 'node:test';
import assert from 'node:assert/strict';
import {createNostrDoorway} from '../packages/phantom-doorways/src/nostr-doorway.js';
const self='1'.repeat(64),remote='2'.repeat(64),room='room';
function fixture(){let callbacks;const signals=[],discovery=[],statuses=[],order=[];
 const doorway=createNostrDoorway({identity:{publicKey:self},relays:['wss://relay.example'],busFactory:config=>{callbacks=config;return {announce(){},close(){},sendSignal(){}};}});
 const session=doorway.open({roomId:room,peerId:self.slice(0,16),onOpen(){},onClose(){},onError(){},onStatus:s=>statuses.push(s),onDiscovery:p=>{discovery.push(p);order.push('discovery');},onSignal:p=>{signals.push(p);order.push('signal');}});
 session.announce({roomId:room,peerId:self.slice(0,16),bootId:'self',t:1});
 const control={type:'rtc_offer',from:remote.slice(0,16),to:self.slice(0,16),videoId:room,bootId:'remote',sysAttemptId:'attempt'};
 const announcement={s7pPublicKey:remote,row:{hKey:remote.slice(0,16),hex:remote,bootId:'remote',roomJoinedAt:2}};
 return {session,callbacks,signals,discovery,statuses,order,control,announcement};
}
test('authenticated Nostr offer preceding discovery resumes after roster projection',()=>{
 const f=fixture();try{
  f.callbacks.onSignal({s7pPublicKey:remote,control:f.control});assert.equal(f.signals.length,0);assert.ok(f.statuses.some(s=>s.type==='signal_deferred'));
  f.callbacks.onAnnouncement(f.announcement);assert.deepEqual(f.signals,[f.control]);assert.equal(f.order.at(-2),'discovery');assert.equal(f.order.at(-1),'signal');
 }finally{f.session.close();}
});
test('out-of-order queue preserves bounds and rejects wrong incarnation or room',()=>{
 const f=fixture();try{
  f.callbacks.onSignal({s7pPublicKey:remote,control:{...f.control,videoId:'wrong'}});
  for(let i=0;i<20;i++)f.callbacks.onSignal({s7pPublicKey:remote,control:{...f.control,bootId:'old',sysAttemptId:String(i)}});
  assert.equal(f.statuses.filter(s=>s.type==='signal_deferred').length,16);assert.equal(f.statuses.filter(s=>s.reason==='discovery_queue_limit').length,4);
  f.callbacks.onAnnouncement(f.announcement);assert.equal(f.signals.length,0);assert.equal(f.statuses.filter(s=>s.reason==='sender_incarnation_mismatch').length,16);
 }finally{f.session.close();}
});
test('closing Nostr doorway clears signals awaiting discovery',()=>{
 const f=fixture();f.callbacks.onSignal({s7pPublicKey:remote,control:f.control});f.session.close();f.callbacks.onAnnouncement(f.announcement);assert.equal(f.signals.length,0);
});
test('expired signal cannot revive when discovery finally arrives',()=>{
 const f=fixture(),originalNow=Date.now;
 try{f.callbacks.onSignal({s7pPublicKey:remote,control:f.control});const future=originalNow()+16000;Date.now=()=>future;f.callbacks.onAnnouncement(f.announcement);assert.equal(f.signals.length,0);assert.ok(f.statuses.some(s=>s.reason==='discovery_wait_expired'));}
 finally{Date.now=originalNow;f.session.close();}
});
