import test from 'node:test';
import assert from 'node:assert/strict';
import {createNostrIdentity} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {openNostrBus,openNostrRelaySet} from '../packages/phantom-doorways/src/nostr.js';
const turn=()=>new Promise(resolve=>setImmediate(resolve));
test('signaling queued before any relay opens is flushed through the production relay set',async()=>{
 const Socket=sockets(),identity=createNostrIdentity(),remote=createNostrIdentity(),statuses=[];
 const set=openNostrRelaySet({roomId:'room',identity,relays:['wss://relay.example'],WebSocket:Socket,onStatus:s=>statuses.push(s)});
 try{
  const messageId=set.sendSignal({nostrKey:remote.nostrKey,s7pPublicKey:remote.publicKey},{type:'rtc_offer',sysAttemptId:'pending'});
  assert.ok(statuses.some(s=>s.type==='signal_queued'&&s.messageId===messageId));
  assert.equal(Socket.all[0].sent.length,0);await turn();
  const packets=Socket.all[0].sent.filter(p=>p[0]==='EVENT');assert.equal(packets.length,1);
  const payload=JSON.parse(packets[0][1].content);assert.equal(payload.messageId,messageId);assert.equal(payload.control.type,'rtc_offer');
 }finally{set.close();}
});
function sockets({auto=true}={}){
 return class Socket {
  static all=[];
  constructor(url){this.url=url;this.readyState=0;this.sent=[];this.constructor.all.push(this);if(auto)queueMicrotask(()=>this.open());}
  open(){this.readyState=1;this.onopen?.();}
  send(raw){this.sent.push(JSON.parse(raw));}
  receive(packet){this.onmessage?.({data:JSON.stringify(packet)});}
  close(){this.readyState=3;this.onclose?.();}
 };
}
test('first relay rejection cannot drop an offer while another relay is opening',async()=>{
 const Socket=sockets({auto:false}),identity=createNostrIdentity(),remote=createNostrIdentity();
 const set=openNostrRelaySet({roomId:'room',identity,relays:['wss://bad.example','wss://good.example'],WebSocket:Socket});
 try{
  set.announce({row:{bootId:'a'}});const [bad,good]=Socket.all;bad.open();
  bad.receive(['OK',bad.sent.find(p=>p[0]==='EVENT')[1].id,false,'restricted']);await turn();
  set.sendSignal({nostrKey:remote.nostrKey,s7pPublicKey:remote.publicKey},{type:'rtc_offer',sysAttemptId:'original-attempt'});
  good.open();const sent=good.sent.filter(p=>p[0]==='EVENT').map(p=>JSON.parse(p[1].content));
  assert.equal(sent[0].type,'ANNOUNCEMENT','discovery must precede queued signaling on the new relay');
  assert.equal(sent.filter(p=>p.control?.type==='rtc_offer').length,1);assert.equal(sent.find(p=>p.control)?.control.sysAttemptId,'original-attempt');
  assert.equal(bad.sent.filter(p=>p[0]==='EVENT').length,1);
 }finally{set.close();}
});
test('opening relay signaling queue remains bounded and close cancels it',()=>{
 const Socket=sockets({auto:false}),identity=createNostrIdentity(),remote=createNostrIdentity();
 const set=openNostrRelaySet({roomId:'room',identity,relays:['wss://relay.example'],WebSocket:Socket});
 const target={nostrKey:remote.nostrKey,s7pPublicKey:remote.publicKey};
 try{for(let i=0;i<256;i++)set.sendSignal(target,{type:'rtc_ice',index:i});assert.throws(()=>set.sendSignal(target,{}),/Too many/);set.close();Socket.all[0].open();assert.equal(Socket.all[0].sent.length,0);assert.throws(()=>set.sendSignal(target,{}),/closed/);}finally{set.close();}
});
test('unchanged Nostr announcements coalesce; changed discovery publishes immediately',async()=>{
 const Socket=sockets(),identity=createNostrIdentity(),bus=openNostrBus({roomId:'room',identity,relays:['wss://relay.example'],WebSocket:Socket,onStatus:s=>{if(s.type==='open')bus.announce({row:{bootId:'a'}});}});
 try{
  bus.announce({row:{bootId:'a'}});await turn();const socket=Socket.all[0];
  for(let i=0;i<100;i++)bus.announce({row:{bootId:'a'}});
  assert.equal(socket.sent.filter(p=>p[0]==='EVENT').length,1);
  bus.announce({row:{bootId:'b'}});assert.equal(socket.sent.filter(p=>p[0]==='EVENT').length,2);
 }finally{bus.close();}
});
test('explicit rejection stops session writes but preserves verified incoming discovery',async()=>{
 const Socket=sockets(),identity=createNostrIdentity(),remote=createNostrIdentity(),statuses=[],received=[];
 const bus=openNostrBus({roomId:'room',identity,relays:['wss://relay.example'],WebSocket:Socket,onStatus:s=>statuses.push(s),onAnnouncement:p=>received.push(p)});
 try{
  bus.announce({row:{bootId:'a'}});await turn();const socket=Socket.all[0],event=socket.sent.find(p=>p[0]==='EVENT')[1];
  socket.receive(['OK',event.id,false,'restricted']);await turn();
  for(let i=0;i<20;i++)bus.announce({row:{bootId:String(i)}});
  assert.throws(()=>bus.sendSignal({nostrKey:remote.nostrKey,s7pPublicKey:remote.publicKey},{}),/rejected/);
  assert.equal(socket.sent.filter(p=>p[0]==='EVENT').length,1);
  const subscription=socket.sent.find(p=>p[0]==='REQ')[1];socket.receive(['EVENT',subscription,remote.sign('room',{type:'ANNOUNCEMENT',row:{bootId:'remote'}})]);await turn();
  assert.equal(received.length,1);assert.equal(socket.readyState,1);assert.ok(statuses.some(s=>s.type==='write_blocked'));
 }finally{bus.close();}
});
test('relay set stops rejected path while remaining writer carries signaling',async()=>{
 const Socket=sockets(),identity=createNostrIdentity(),remote=createNostrIdentity(),statuses=[];
 const set=openNostrRelaySet({roomId:'room',identity,relays:['wss://bad.example','wss://good.example'],WebSocket:Socket,onStatus:s=>statuses.push(s)});
 try{
  set.announce({row:{bootId:'a'}});await turn();const [bad,good]=Socket.all;
  const event=bad.sent.find(p=>p[0]==='EVENT')[1];bad.receive(['OK',event.id,false,'restricted']);await turn();
  const count=bad.sent.length;set.sendSignal({nostrKey:remote.nostrKey,s7pPublicKey:remote.publicKey},{type:'rtc_offer'});
  assert.equal(bad.sent.length,count);assert.ok(good.sent.some(p=>p[0]==='EVENT'&&JSON.parse(p[1].content).type==='SIGNAL'));
  assert.equal(statuses.some(s=>s.type==='exhausted'),false);
  const last=good.sent.at(-1)[1];good.receive(['OK',last.id,false,'restricted']);await turn();
  assert.ok(statuses.some(s=>s.type==='exhausted'&&s.reason==='all_publication_paths_rejected'));
  assert.throws(()=>set.sendSignal({},{}),/No open/);
 }finally{set.close();}
});
test('event-specific rejections preserve later writes and expose machine-readable categories',async()=>{
 for(const category of ['mute','invalid','pow','error','unknown']){
  const Socket=sockets(),identity=createNostrIdentity(),statuses=[];
  const bus=openNostrBus({roomId:'room',identity,relays:['wss://relay.example'],WebSocket:Socket,onStatus:s=>statuses.push(s)});
  try{bus.announce({row:{bootId:'a'}});await turn();const socket=Socket.all[0];
   socket.receive(['OK',socket.sent.find(p=>p[0]==='EVENT')[1].id,false,`${category}: test`]);await turn();
   assert.doesNotThrow(()=>bus.sendSignal({nostrKey:identity.nostrKey,s7pPublicKey:identity.publicKey},{type:'test'}));
   assert.equal(statuses.some(s=>s.type==='write_blocked'),false);
   assert.ok(statuses.some(s=>s.type==='publish'&&s.rejectionCategory===category&&s.rejectionScope==='event'));
  }finally{bus.close();}
 }
});
test('rate limit pauses writes, keeps ACK/read processing, then permits fresh messages without replay',async()=>{
 const Socket=sockets(),identity=createNostrIdentity(),statuses=[];
 const bus=openNostrBus({roomId:'room',identity,relays:['wss://relay.example'],publicationCooldownMs:30,WebSocket:Socket,onStatus:s=>statuses.push(s)});
 try{bus.announce({row:{bootId:'a'}});await turn();const socket=Socket.all[0],target={nostrKey:identity.nostrKey,s7pPublicKey:identity.publicKey};
  const second=bus.sendSignal(target,{type:'test'});
  socket.receive(['OK',socket.sent.find(p=>p[0]==='EVENT')[1].id,false,'rate-limited: slow down']);await turn();
  assert.throws(()=>bus.sendSignal(target,{}),/temporarily/);
  socket.receive(['OK',second,true,'']);await turn();assert.ok(statuses.some(s=>s.eventId===second&&s.accepted));
  assert.equal(socket.readyState,1);assert.equal(statuses.some(s=>s.type==='write_blocked'),false);
  await new Promise(r=>setTimeout(r,45));assert.ok(statuses.some(s=>s.type==='publication_resumed'));
  assert.equal(socket.sent.filter(p=>p[0]==='EVENT').length,2,'cooldown does not replay rejected messages');
  bus.sendSignal(target,{type:'fresh'});assert.equal(socket.sent.filter(p=>p[0]==='EVENT').length,3);
 }finally{bus.close();}
});
test('AUTH challenge alone does not block writes or claim authentication is required',async()=>{
 const Socket=sockets(),identity=createNostrIdentity(),statuses=[];
 const bus=openNostrBus({roomId:'room',identity,relays:['wss://relay.example'],WebSocket:Socket,onStatus:s=>statuses.push(s)});
 try{await turn();const socket=Socket.all[0];socket.receive(['AUTH','challenge']);await turn();
  assert.ok(statuses.some(s=>s.type==='authentication_challenge'));assert.equal(statuses.some(s=>s.type==='authentication_required'),false);
  assert.doesNotThrow(()=>bus.announce({row:{bootId:'a'}}));
  const event=socket.sent.find(p=>p[0]==='EVENT')[1];socket.receive(['OK',event.id,false,'auth-required: authenticate']);await turn();
  assert.ok(statuses.some(s=>s.type==='write_blocked'&&s.category==='auth-required'));
 }finally{bus.close();}
});
test('paused relay set queues fresh work and resumes without replaying rejected signaling',async()=>{
 const Socket=sockets(),identity=createNostrIdentity(),statuses=[];
 const set=openNostrRelaySet({roomId:'room',identity,relays:['wss://relay.example'],publicationCooldownMs:30,WebSocket:Socket,onStatus:s=>statuses.push(s)});
 try{await turn();const socket=Socket.all[0],target={nostrKey:identity.nostrKey,s7pPublicKey:identity.publicKey};
  const rejected=set.sendSignal(target,{type:'old'}),event=socket.sent.find(p=>p[0]==='EVENT')[1];socket.receive(['OK',event.id,false,'rate-limited: pause']);await turn();
  const fresh=set.sendSignal(target,{type:'fresh'});assert.equal(socket.sent.filter(p=>p[0]==='EVENT').length,1);
  assert.equal(statuses.some(s=>s.type==='exhausted'),false);
  await new Promise(r=>setTimeout(r,45));const messages=socket.sent.filter(p=>p[0]==='EVENT').map(p=>JSON.parse(p[1].content).messageId);
  assert.deepEqual(messages,[rejected,fresh]);
 }finally{set.close();}
});
test('closing from rejection callback cannot install a cooldown timer afterwards',async()=>{
 const Socket=sockets(),identity=createNostrIdentity(),statuses=[];
 const bus=openNostrBus({roomId:'room',identity,relays:['wss://relay.example'],WebSocket:Socket,onStatus:s=>{statuses.push(s);if(s.type==='publish'&&!s.accepted)bus.close();}});
 bus.announce({row:{bootId:'a'}});await turn();const socket=Socket.all[0];
 socket.receive(['OK',socket.sent.find(p=>p[0]==='EVENT')[1].id,false,'rate-limited: pause']);await turn();
 assert.equal(socket.readyState,3);assert.equal(statuses.some(s=>s.type==='publication_paused'),false);
});
test('expired queued signaling is not submitted when a relay finally opens',async()=>{
 const Socket=sockets({auto:false}),identity=createNostrIdentity(),statuses=[];
 const set=openNostrRelaySet({roomId:'room',identity,relays:['wss://relay.example'],publicationLifetimeMs:25,WebSocket:Socket,onStatus:s=>statuses.push(s)});
 try{const id=set.sendSignal({nostrKey:identity.nostrKey,s7pPublicKey:identity.publicKey},{type:'rtc_offer',sysAttemptId:'old'});
  await new Promise(r=>setTimeout(r,40));Socket.all[0].open();await turn();
  assert.equal(Socket.all[0].sent.filter(p=>p[0]==='EVENT').length,0);assert.ok(statuses.some(s=>s.type==='signal_expired'&&s.messageId===id));
 }finally{set.close();}
});
test('cooldown resume refreshes discovery before fresh queued signals',async()=>{
 for(const change of [true,false]){
 const Socket=sockets(),identity=createNostrIdentity();
 const set=openNostrRelaySet({roomId:'room',identity,relays:['wss://relay.example'],publicationCooldownMs:30,WebSocket:Socket});
 try{set.announce({row:{bootId:'before'}});await turn();const socket=Socket.all[0];
  socket.receive(['OK',socket.sent.find(p=>p[0]==='EVENT')[1].id,false,'rate-limited: pause']);await turn();
  if(change)set.announce({row:{bootId:'after'}});set.sendSignal({nostrKey:identity.nostrKey,s7pPublicKey:identity.publicKey},{type:'rtc_offer'});
  await new Promise(r=>setTimeout(r,45));const packets=socket.sent.filter(p=>p[0]==='EVENT').map(p=>JSON.parse(p[1].content));
  assert.deepEqual(packets.map(p=>p.type),['ANNOUNCEMENT','ANNOUNCEMENT','SIGNAL']);assert.equal(packets[1].row.bootId,change?'after':'before');
 }finally{set.close();}
 }
});
test('cooldown with only expired work does not create a discovery retry loop',async()=>{
 const Socket=sockets(),identity=createNostrIdentity(),statuses=[];
 const set=openNostrRelaySet({roomId:'room',identity,relays:['wss://relay.example'],publicationCooldownMs:60,publicationLifetimeMs:20,WebSocket:Socket,onStatus:s=>statuses.push(s)});
 try{set.announce({row:{bootId:'a'}});await turn();const socket=Socket.all[0];socket.receive(['OK',socket.sent.find(p=>p[0]==='EVENT')[1].id,false,'rate-limited: pause']);await turn();
  set.sendSignal({nostrKey:identity.nostrKey,s7pPublicKey:identity.publicKey},{type:'rtc_offer'});await new Promise(r=>setTimeout(r,85));
  assert.equal(socket.sent.filter(p=>p[0]==='EVENT').length,1);assert.ok(statuses.some(s=>s.type==='signal_expired'));assert.ok(statuses.some(s=>s.type==='publication_resumed'));
 }finally{set.close();}
});
