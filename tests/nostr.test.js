import test from 'node:test';import assert from 'node:assert/strict';
import {createNostrIdentity,verifyNostrEnvelope} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {openNostrBus,openNostrRelaySet} from '../packages/phantom-doorways/src/nostr.js';
test('real Schnorr envelope and Ed25519 binding reject tampering and wrong room',()=>{
 const a=createNostrIdentity(),event=a.sign('room',{type:'ANNOUNCEMENT',roomJoinedAt:42});
 assert.equal(verifyNostrEnvelope(event,'room').roomJoinedAt,42);
 assert.equal(verifyNostrEnvelope({...event,content:event.content+' '},'room'),null);
 assert.equal(verifyNostrEnvelope(event,'other'),null);
 assert.equal(verifyNostrEnvelope(event,'room',{now:Date.now()+120000}),null);
});
test('live bus preserves control objects, checks target and ignores duplicate events after EOSE',async()=>{
 class Socket{constructor(){this.readyState=1;Socket.instance=this;}send(data){this.sent??=[];this.sent.push(JSON.parse(data));}close(){this.readyState=3;}}
 const a=createNostrIdentity(),b=createNostrIdentity();let sync=0,signals=[];
 const bus=openNostrBus({roomId:'room',identity:b,relays:['ws://test'],WebSocket:Socket,onSync:()=>sync++,onSignal:p=>signals.push(p)});
 const s=Socket.instance;s.onopen();const sub=s.sent[0][1];
 s.onmessage({data:JSON.stringify(['EOSE',sub])});
 const control={type:'rtc_offer',sdp:{type:'offer',sdp:'v=0'},relationshipCycleId:'cycle',bootId:'boot'};
 const e=a.sign('room',{type:'SIGNAL',targetNostrKey:b.nostrKey,targetS7pKey:b.publicKey,control});
 for(let i=0;i<2;i++)s.onmessage({data:JSON.stringify(['EVENT',sub,e])});
 s.onmessage({data:JSON.stringify(['EVENT',sub,a.sign('room',{type:'SIGNAL',targetNostrKey:a.nostrKey,targetS7pKey:a.publicKey,control})])});
 await new Promise(resolve=>setImmediate(resolve));assert.equal(sync,1);assert.equal(signals.length,1);assert.deepEqual(signals[0].control,control);bus.close();
});
test('Nostr doorway rejects forged core authors and keeps nested controls intact',async()=>{
 const {createNostrDoorway}=await import('../packages/phantom-doorways/src/nostr-doorway.js');
 const a=createNostrIdentity(),b=createNostrIdentity();let callbacks,sent,received;
 const d=createNostrDoorway({identity:a,relays:['ws://test'],busFactory:c=>{callbacks=c;return{announce(){},sendSignal(t,p){sent=p;},close(){}};}});
 const s=d.open({roomId:'room',peerId:a.publicKey.slice(0,16),onOpen(){},onDiscovery(){},onSignal:p=>received=p,onClose(){},onError:e=>{throw e;}});
 s.announce({roomId:'room',peerId:a.publicKey.slice(0,16),bootId:'a',t:1});
 callbacks.onAnnouncement({nostrKey:b.nostrKey,s7pPublicKey:b.publicKey,row:{hKey:b.publicKey.slice(0,16),hex:b.publicKey,bootId:'b',roomJoinedAt:2}});
 const packet={from:b.publicKey.slice(0,16),to:a.publicKey.slice(0,16),videoId:'room',type:'rtc_ice',cand:{candidate:'native'}};
 callbacks.onSignal({s7pPublicKey:b.publicKey,control:{...packet,from:'forged'}});assert.equal(received,undefined);
 callbacks.onSignal({s7pPublicKey:b.publicKey,control:packet});assert.deepEqual(received,packet);
 const out={...packet,from:a.publicKey.slice(0,16),to:b.publicKey.slice(0,16)};s.sendSignal(out);assert.deepEqual(sent,out);s.close();
});

test('shared relay set publishes on both surfaces and delivers a logical signal once',async()=>{
 class Socket{static all=[];constructor(){this.readyState=1;this.sent=[];Socket.all.push(this);}send(s){this.sent.push(JSON.parse(s));}close(){this.readyState=3;}}
 const a=createNostrIdentity(),b=createNostrIdentity(),received=[];
 const bus=openNostrRelaySet({roomId:'room',identity:b,relays:['ws://one','ws://two'],WebSocket:Socket,onSignal:p=>received.push(p)});
 for(const s of Socket.all)s.onopen();
 bus.sendSignal({nostrKey:a.nostrKey,s7pPublicKey:a.publicKey},{type:'rtc_offer'});
 const published=Socket.all.map(s=>s.sent.find(p=>p[0]==='EVENT')[1]);
 assert.equal(JSON.parse(published[0].content).messageId,JSON.parse(published[1].content).messageId);
 for(const s of Socket.all){const event=a.sign('room',{type:'SIGNAL',messageId:'same-message',targetNostrKey:b.nostrKey,targetS7pKey:b.publicKey,control:{type:'rtc_answer'}});s.onmessage({data:JSON.stringify(['EVENT',s.sent[0][1],event])});}
 await new Promise(r=>setImmediate(r));assert.equal(received.length,1);bus.close();
});

test('departure waits for publication response before closing the socket',async()=>{
 class Socket{constructor(){this.readyState=1;this.sent=[];Socket.instance=this;}send(s){this.sent.push(JSON.parse(s));}close(){this.readyState=3;}}
 const bus=openNostrBus({roomId:'room',identity:createNostrIdentity(),relays:['ws://one'],WebSocket:Socket});
 const s=Socket.instance;s.onopen();bus.depart({row:{bootId:'boot'}});
 assert.equal(s.readyState,1);const event=s.sent.at(-1)[1];assert.equal(JSON.parse(event.content).departed,true);
 s.onmessage({data:JSON.stringify(['OK',event.id,true,''])});await new Promise(r=>setImmediate(r));assert.equal(s.readyState,3);
});

test('relay history completion does not republish the cached doorway announcement',async()=>{
 const {createNostrDoorway}=await import('../packages/phantom-doorways/src/nostr-doorway.js');
 const identity=createNostrIdentity();let callbacks,publications=0,rosters=0;
 const d=createNostrDoorway({identity,relays:['ws://one'],busFactory:c=>{callbacks=c;return{announce(){publications++;},sendSignal(){},close(){}};}});
 const session=d.open({roomId:'room',peerId:identity.publicKey.slice(0,16),onOpen(){},onDiscovery(){rosters++;},onSignal(){},onClose(){},onError(e){throw e;}});
 session.announce({roomId:'room',peerId:identity.publicKey.slice(0,16),bootId:'boot',t:1});
 assert.equal(publications,1);callbacks.onSync();callbacks.onSync();assert.equal(publications,1);assert.equal(rosters,3);session.close();
});

test('committed leader publishes updated doorway hint; another peer cannot publish it',async()=>{
 const {createNostrDoorway}=await import('../packages/phantom-doorways/src/nostr-doorway.js');
 const identity=createNostrIdentity(),key=identity.publicKey.slice(0,16);let published;
 const d=createNostrDoorway({identity,relays:['ws://one'],busFactory:()=>({announce(p){published=p;},sendSignal(){},close(){}})});
 const s=d.open({roomId:'room',peerId:key,onOpen(){},onDiscovery(){},onSignal(){},onClose(){},onError(e){throw e;}});
 s.announce({roomId:'room',peerId:key,bootId:'boot',t:1});
 const state={room:'room',from:key,syscon1:key,syscon2:null,epoch:4,rosterSeq:1,roster:[{hKey:key}]};
 s.publishAuthority(state);assert.deepEqual(published.authorityHint,state);
 assert.throws(()=>s.publishAuthority({...state,from:'rogue'}));s.close();
});

test('unaccepted setup crosses to a newly opened relay; accepted setup is not replayed',async()=>{
 class Socket{static all=[];constructor(){this.readyState=0;this.sent=[];Socket.all.push(this);}send(s){this.sent.push(JSON.parse(s));}close(){this.readyState=3;}}
 const identity=createNostrIdentity(),target=createNostrIdentity(),statuses=[];
 const bus=openNostrRelaySet({roomId:'room',identity,relays:['ws://one','ws://two','ws://three'],WebSocket:Socket,onStatus:s=>statuses.push(s)});
 const [one,two,three]=Socket.all;one.readyState=1;one.onopen();
 const control={type:'rtc_offer',bootId:'boot',sysAttemptId:'attempt',sdp:{type:'offer',sdp:'native'}};
 const id=bus.sendSignal({nostrKey:target.nostrKey,s7pPublicKey:target.publicKey},control);
 const first=one.sent.find(p=>p[0]==='EVENT')[1];
 one.onmessage({data:JSON.stringify(['OK',first.id,false,'blocked'])});await new Promise(r=>setImmediate(r));
 two.readyState=1;two.onopen();const second=two.sent.find(p=>p[0]==='EVENT')[1];
 assert.deepEqual(JSON.parse(second.content).control,control);assert.equal(JSON.parse(second.content).messageId,id);
 assert.equal(statuses.find(s=>s.type==='publish').url,'ws://one');
 two.onmessage({data:JSON.stringify(['OK',second.id,true,''])});await new Promise(r=>setImmediate(r));
 three.readyState=1;three.onopen();assert.equal(three.sent.filter(p=>p[0]==='EVENT').length,0);
 bus.close();assert.throws(()=>bus.sendSignal({},control),/closed/);
});

test('new relay receives cached presence before retained setup',async()=>{
 class Socket{static all=[];constructor(){this.readyState=0;this.sent=[];Socket.all.push(this);}send(s){this.sent.push(JSON.parse(s));}close(){}}
 const identity=createNostrIdentity(),target=createNostrIdentity();
 const bus=openNostrRelaySet({roomId:'room',identity,relays:['ws://one','ws://two'],WebSocket:Socket});
 const [one,two]=Socket.all;bus.announce({row:{bootId:'boot'}});one.readyState=1;one.onopen();
 bus.sendSignal({nostrKey:target.nostrKey,s7pPublicKey:target.publicKey},{type:'rtc_offer'});
 two.readyState=1;two.onopen();assert.deepEqual(two.sent.filter(p=>p[0]==='EVENT').map(p=>JSON.parse(p[1].content).type),['ANNOUNCEMENT','SIGNAL']);bus.close();
});
