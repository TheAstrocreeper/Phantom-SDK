import test from 'node:test';
import assert from 'node:assert/strict';
import {withNativeEntry} from '../packages/phantom-doorways/src/native-entry.js';
import {createNostrDoorway} from '../packages/phantom-doorways/src/nostr-doorway.js';
const self='1'.repeat(64),remote='2'.repeat(64),room='room';
function fixture(native=false){let callbacks;const signals=[],discovery=[],statuses=[],order=[];
 const raw=createNostrDoorway({identity:{publicKey:self},relays:['wss://relay.example'],busFactory:config=>{callbacks=config;return {announce(){},close(){},sendSignal(){}};}});
 const doorway=native?withNativeEntry(raw):raw;
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
test('an older relay announcement cannot replace a rejoined device incarnation',()=>{
 const f=fixture();try{
  f.callbacks.onAnnouncement(f.announcement);
  const fresh={...f.announcement,row:{...f.announcement.row,bootId:'fresh',roomJoinedAt:3}};
  f.callbacks.onAnnouncement(fresh);
  f.callbacks.onAnnouncement(f.announcement);
  const current={...f.control,bootId:'fresh'};
  f.callbacks.onSignal({s7pPublicKey:remote,control:current});
  assert.deepEqual(f.signals,[current]);
  assert.equal(f.discovery.at(-1).peers.find(p=>p.hKey===remote.slice(0,16)).bootId,'fresh');
 }finally{f.session.close();}
});
test('a delayed live announcement cannot resurrect a departed incarnation',()=>{
 const f=fixture();try{
  f.callbacks.onAnnouncement(f.announcement);
  f.callbacks.onAnnouncement({...f.announcement,departed:true});
  f.callbacks.onAnnouncement(f.announcement);
  assert.equal(f.discovery.at(-1).peers.some(p=>p.hKey===remote.slice(0,16)),false);
 }finally{f.session.close();}
});
test('fresh rejoin survives an old departure and repeated old relay replay',()=>{
 const f=fixture();try{
  f.callbacks.onAnnouncement(f.announcement);
  f.callbacks.onAnnouncement({...f.announcement,departed:true});
  const fresh={...f.announcement,row:{...f.announcement.row,bootId:'fresh',roomJoinedAt:3}};
  f.callbacks.onAnnouncement(fresh);
  f.callbacks.onAnnouncement({...f.announcement,departed:true});
  f.callbacks.onAnnouncement(f.announcement);
  const current={...f.control,bootId:'fresh'};
  f.callbacks.onSignal({s7pPublicKey:remote,control:current});
  assert.deepEqual(f.signals,[current]);
  assert.equal(f.discovery.at(-1).peers.find(p=>p.hKey===remote.slice(0,16)).bootId,'fresh');
 }finally{f.session.close();}
});

test('committed no-mirror authority is not replaced by an uncommitted second-device guess',()=>{
 const f=fixture(true);try{
  const announcement={...f.announcement,row:{...f.announcement.row,roomJoinedAt:0}};
  f.callbacks.onAnnouncement(announcement);
  const key=announcement.row.hKey;
  f.callbacks.onAnnouncement({...announcement,authorityHint:{room:'room',from:key,syscon1:key,syscon2:null,epoch:20,rosterSeq:1,roster:[{hKey:key}]}});
  const route=f.discovery.at(-1);
  assert.equal(route.leader,key);assert.equal(route.mirror,null);
  assert.notEqual(route.extensions?.bootstrapAdmission,true,'ordinary fresh peer must not wait for a guessed mirror-owner offer');
 }finally{f.session.close();}
});
