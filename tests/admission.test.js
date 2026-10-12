import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdmissionCheckpoint} from '../packages/phantom-core/src/admission.js';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createNostrDoorway} from '../packages/phantom-doorways/src/nostr-doorway.js';
import {createNostrIdentity,verifyNostrEnvelope} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {withAdmissionDoorway} from '../packages/phantom-doorways/src/admission-doorway.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {encodeInvite} from '../packages/phantom-core/src/invite.js';
const room='a'.repeat(64), otherRoom='b'.repeat(64);
const publicKey='c'.repeat(64);
const invite=encodeInvite({networkId:room,relays:['wss://relay.example']});
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const proof=(identity,c,kind,payload)=>identity.signPeerProof({networkId:room,credential:c,kind,payload,issuedAt:Date.now(),nonce:crypto.randomUUID()});
const credential=(key=publicKey,networkId=room)=>({peerPublicKey:key,networkId,expiresAt:Date.now()+60000});
const verify=({networkId,peerPublicKey,credential:c})=>c.networkId===networkId&&c.peerPublicKey===peerPublicKey;
const denied={authorize:()=>null,verify};

for(const operation of ['join','create'])test(`${operation}: denied authorization produces no discovery or socket traffic`,async()=>{
 let probes=0,sockets=0,cores=0;
 const sdk=createBrowserPhantom({admission:denied,relayFinder:()=>{probes++;return {relays:['wss://relay.example']};},WebSocket:class{constructor(){sockets++;}},coreFactory:()=>{cores++;throw Error('must not initialize');}});
 await assert.rejects(sdk[operation](...(operation==='join'?[invite]:[])),{code:'PHANTOM_ADMISSION_DENIED'});
 assert.deepEqual([probes,sockets,cores],[0,0,0]);
 await assert.rejects(async()=>sdk.refreshRelays(),{code:'PHANTOM_ADMISSION_DENIED'});
});

test('missing verifier cannot silently select open mode',()=>{
 assert.throws(()=>createBrowserPhantom({admission:{authorize:()=>true}}),/verify/);
});

test('credentials are scoped to network, identity, expiry, and strict approval',async()=>{
 const grant=credential();let clock=grant.expiresAt-60000;
 const checkpoint=createAdmissionCheckpoint({policy:{authorize:()=>grant,verify},networkId:room,peerPublicKey:publicKey,now:()=>clock});
 await checkpoint.authorize();assert.equal(checkpoint.allows(publicKey.slice(0,16)),true);
 assert.equal(await checkpoint.verify('d'.repeat(64),credential()),false);
 assert.equal(await checkpoint.verify(publicKey,credential(publicKey,otherRoom)),false);
 clock=grant.expiresAt+1;assert.equal(checkpoint.allows(publicKey.slice(0,16)),false);
 assert.equal(await checkpoint.verify(publicKey,grant),false);
 const truthy=createAdmissionCheckpoint({policy:{authorize:()=>credential(),verify:()=>({allowed:true})},networkId:room,peerPublicKey:publicKey});
 await assert.rejects(truthy.authorize(),{code:'PHANTOM_ADMISSION_DENIED'});
});

test('throws, timeout and abort fail closed',async()=>{
 for(const authorize of [()=>{throw Error('denied');},()=>new Promise(()=>{})]){
  const checkpoint=createAdmissionCheckpoint({policy:{authorize,verify},networkId:room,peerPublicKey:publicKey,timeoutMs:10});
  await assert.rejects(checkpoint.authorize());
 }
 const controller=new AbortController();controller.abort();
 const checkpoint=createAdmissionCheckpoint({policy:{authorize:()=>credential(),verify},networkId:room,peerPublicKey:publicKey});
 await assert.rejects(checkpoint.authorize({signal:controller.signal}));
});

function fixture(access){
 const identity=createNostrIdentity();let busConfig,announcements=[],signals=[],hints=[],received=[];
 const doorway=createNostrDoorway({identity,relays:['wss://relay.example'],admission:access,busFactory:config=>{
  busConfig=config;return {announce:p=>announcements.push(p),sendSignal:(...p)=>signals.push(p),close(){},depart(){}};
 }});
 const session=doorway.open({roomId:room,peerId:identity.publicKey.slice(0,16),onOpen(){},onDiscovery:h=>hints.push(h),onSignal:p=>received.push(p),onClose(){},onError:e=>{throw e;}});
 return {identity,session,busConfig,announcements,signals,hints,received};
}
function announcement(identity,c){
 const payload={roomId:room,peerId:identity.publicKey.slice(0,16),handle:'Peer',bootId:'boot',t:Date.now()};
 const row={hKey:payload.peerId,hex:identity.publicKey,bootId:payload.bootId,roomJoinedAt:payload.t,...(c?{admissionProof:proof(identity,c,'announce',payload)}:{})};
 return verifyNostrEnvelope(identity.sign(room,{type:'ANNOUNCEMENT',row}),room);
}

test('Nostr admits only verified signed identities; copied credentials and bare signals stay out',async()=>{
 // The real signed-envelope verifier supplies the public key used by admission.
 const local=createNostrIdentity();
 const checkpoint=createAdmissionCheckpoint({policy:{authorize:()=>credential(local.publicKey),verify},networkId:room,peerPublicKey:local.publicKey});
 const c=await checkpoint.authorize();
 let busConfig,hints=[],received=[],outbound=[];
 const rawDoorway=createNostrDoorway({identity:local,relays:['wss://relay.example'],busFactory:config=>{busConfig=config;return {announce:p=>outbound.push(p),sendSignal(){},close(){},depart(){}};}});
 const doorway=withAdmissionDoorway(rawDoorway,{identity:local,checkpoint,credential:c});
 const session=doorway.open({roomId:room,peerId:local.publicKey.slice(0,16),onOpen(){},onDiscovery:h=>hints.push(h),onSignal:p=>received.push(p),onClose(){},onError:e=>{throw e;}});
 session.announce({roomId:room,peerId:local.publicKey.slice(0,16),bootId:'local',t:Date.now()});
 assert.deepEqual(outbound[0].row.admissionProof.credential,c);
 const approved=createNostrIdentity(),rogue=createNostrIdentity();
 busConfig.onAnnouncement(announcement(rogue,credential(approved.publicKey)));
 busConfig.onAnnouncement(announcement(rogue,undefined));
 await tick();assert.equal(hints.at(-1).peers.length,1);
 assert.equal(checkpoint.allows(rogue.publicKey.slice(0,16)),false);
 busConfig.onAnnouncement(announcement(approved,credential(approved.publicKey)));
 await tick();assert.equal(hints.at(-1).peers.length,2);
 const control={type:'rtc_offer',videoId:room,from:rogue.publicKey.slice(0,16),to:local.publicKey.slice(0,16)};
 const packet=verifyNostrEnvelope(rogue.sign(room,{type:'SIGNAL',control:{...control,admissionProof:proof(rogue,credential(approved.publicKey),'signal',control)}}),room);
 busConfig.onSignal(packet);await tick();assert.equal(received.length,0);
 const goodControl={...control,from:approved.publicKey.slice(0,16)};
 const good=verifyNostrEnvelope(approved.sign(room,{type:'SIGNAL',control:{...goodControl,admissionProof:proof(approved,credential(approved.publicKey),'signal',goodControl)}}),room);
 busConfig.onSignal(good);await tick();assert.equal(received.length,1);
 session.close();
});

test('open Nostr networks remain compatible',()=>{
 const f=fixture();f.session.announce({roomId:room,peerId:f.identity.publicKey.slice(0,16),bootId:'local',t:Date.now()});
 const remote=createNostrIdentity();f.busConfig.onAnnouncement(announcement(remote));
 assert.equal(f.hints.at(-1).peers.length,2);f.session.close();
});


test('real admin signature cannot be copied to another identity or changed to extend expiry',async()=>{
 const {createAdminIdentity,importAdminIdentity}=await import('../packages/phantom-core/src/authorization.js');
 const {createAdmissionVerifier}=await import('../packages/phantom-core/src/admission.js');
 const admin=await createAdminIdentity();
 const restored=await importAdminIdentity(await admin.exportSeed());
 const c=await restored.signAdmission({networkId:room,peerPublicKey:publicKey,expiresAt:Date.now()+60000});
 const verifier=createAdmissionVerifier({authorityPublicKey:admin.publicKey});
 assert.equal(await verifier({networkId:room,peerPublicKey:publicKey,credential:c}),true);
 assert.equal(await verifier({networkId:otherRoom,peerPublicKey:publicKey,credential:c}),false);
 assert.equal(await verifier({networkId:room,peerPublicKey:'d'.repeat(64),credential:c}),false);
 assert.equal(await verifier({networkId:room,peerPublicKey:publicKey,credential:{...c,expiresAt:c.expiresAt+60000}}),false);
 const impostor=await createAdminIdentity();
 assert.equal(await verifier({networkId:room,peerPublicKey:publicKey,credential:await impostor.signAdmission(c)}),false);
});

test('authorized create boots the real core and relay bus only after approval',async()=>{
 const {createAdminIdentity}=await import('../packages/phantom-core/src/authorization.js');
 const {createAdmissionVerifier}=await import('../packages/phantom-core/src/admission.js');
 const admin=await createAdminIdentity();const order=[],sent=[];
 class Relay {
  constructor(){order.push('socket');this.readyState=0;queueMicrotask(()=>{this.readyState=1;this.onopen?.();});}
  send(raw){sent.push(JSON.parse(raw));}
  close(){this.readyState=3;}
 }
 const sdk=createBrowserPhantom({relays:['wss://relay.example'],WebSocket:Relay,admission:{
  async authorize({networkId,peerPublicKey}){order.push('authorize');return admin.signAdmission({networkId,peerPublicKey,expiresAt:Date.now()+60000});},
  verify:createAdmissionVerifier({authorityPublicKey:admin.publicKey}),
 }});
 const network=await sdk.create({name:'Source'});
 await tick();await tick();
 assert.equal(order[0],'authorize');assert.ok(order.includes('socket'));
 const events=sent.filter(p=>p[0]==='EVENT');assert.ok(events.length>0);
 const payload=verifyNostrEnvelope(events[0][1],network.networkId);
 assert.equal(payload.row.admissionProof.credential.peerPublicKey,payload.s7pPublicKey);
 assert.equal(payload.row.admissionProof.credential.networkId,network.networkId);
 network.close();
});

for(const type of ['websocket','custom'])for(const operation of ['create','join'])test(`${type} ${operation}: denial never constructs a socket or calls the adapter factory`,async()=>{
 let sockets=0,factories=0,opens=0;
 const spec=type==='websocket'?{type,url:'wss://private.example'}:{type};
 const custom=()=>{factories++;return createDoorway({discovery:true,open(){opens++;throw Error('must not open');}});};
 const sdk=createBrowserPhantom({doorways:[spec],adapters:{custom},admission:denied,WebSocket:class{constructor(){sockets++;}}});
 const link=encodeInvite({networkId:room,doorways:[spec]});
 await assert.rejects(sdk[operation](...(operation==='join'?[link]:[])));
 assert.deepEqual([sockets,factories,opens],[0,0,0]);
});

test('authorized private WebSocket starts the real core with a portable signed announcement',async()=>{
 const order=[],sent=[];
 class Server {
  constructor(){order.push('socket');queueMicrotask(()=>{this.onopen?.();this.onmessage?.({data:JSON.stringify({type:'hello',connectionChallenge:'private-challenge'})});});}
  send(raw){sent.push(JSON.parse(raw));}
  close(){}
 }
 const sdk=createBrowserPhantom({doorways:[{type:'websocket',url:'wss://private.example'}],WebSocket:Server,admission:{authorize:({networkId,peerPublicKey})=>{order.push('authorize');return credential(peerPublicKey,networkId);},verify}});
 const network=await sdk.create({name:'Private source'});await tick();await tick();
 assert.deepEqual(order,['authorize','socket']);
 const announcement=sent.find(p=>p.type==='concierge_connect');assert.ok(announcement.admissionProof);
 assert.equal(announcement.admissionProof.networkId,network.networkId);
 assert.equal(announcement.admissionProof.payload.peerId,announcement.from);
 network.close();
});

test('authorized custom adapter receives verified local context and signed packets',async()=>{
 let opened,announcement;
 const sdk=createBrowserPhantom({doorways:[{type:'custom'}],adapters:{custom:()=>createDoorway({discovery:true,open(context){
  opened=context;queueMicrotask(()=>context.onOpen());return {announce:p=>{announcement=p;},reconcile(){},sendSignal(){},close(){}};
 }})},admission:{authorize:({networkId,peerPublicKey})=>credential(peerPublicKey,networkId),verify}});
 const network=await sdk.create({name:'Custom source'});await tick();await tick();
 assert.equal(opened.authorization.networkId,network.networkId);
 assert.equal(opened.authorization.peerPublicKey,announcement.admissionProof.peerPublicKey);
 assert.equal(announcement.admissionProof.kind,'announce');network.close();
});

test('common gate rejects unauthorized rows and tampered/replayed private-server signaling',async()=>{
 const local=createNostrIdentity(),remote=createNostrIdentity(),rogue=createNostrIdentity();
 const checkpoint=createAdmissionCheckpoint({policy:{authorize:()=>credential(local.publicKey),verify},networkId:room,peerPublicKey:local.publicKey});
 const c=await checkpoint.authorize();let wire,hints=[],received=[],sent=[];
 const raw=createDoorway({discovery:true,presence:'live-sockets',open(context){wire=context;return {announce(){},reconcile(){},sendSignal:p=>sent.push(p),close(){}};}});
 const protectedDoorway=withAdmissionDoorway(raw,{checkpoint,credential:c,identity:local});
 const session=protectedDoorway.open({roomId:room,peerId:local.publicKey.slice(0,16),onDiscovery:p=>hints.push(p),onSignal:p=>received.push(p),onError:e=>{throw e;}});
 session.announce({roomId:room,peerId:local.publicKey.slice(0,16),bootId:'local',t:Date.now()});
 wire.onDiscovery({roomId:room,kind:'concierge_route',leader:rogue.publicKey.slice(0,16),peers:[announcement(remote,credential(remote.publicKey)).row,announcement(rogue,credential(remote.publicKey)).row]});
 await tick();assert.equal(hints.at(-1).peers.length,1);assert.equal(hints.at(-1).leader,remote.publicKey.slice(0,16));
 const payload={type:'rtc_offer',from:remote.publicKey.slice(0,16),to:local.publicKey.slice(0,16),videoId:room,sdp:{type:'offer',sdp:'original'}};
 const packet={...payload,admissionProof:proof(remote,credential(remote.publicKey),'signal',payload)};
 wire.onSignal({...packet,sdp:{type:'offer',sdp:'modified'}});await tick();assert.equal(received.length,0);
 wire.onSignal(packet);wire.onSignal(packet);await tick();assert.equal(received.length,1);
 wire.onSignal({...payload,admissionProof:undefined});await tick();assert.equal(received.length,1);
 session.sendSignal({...payload,from:local.publicKey.slice(0,16),to:remote.publicKey.slice(0,16)});assert.ok(sent[0].admissionProof);
 session.close();wire.onSignal(packet);await tick();assert.equal(received.length,1);
});

test('common gate checks permission again before adapter.open and each send',async()=>{
 let clock=Date.now(),opens=0,sends=0;
 const local=createNostrIdentity();const c={...credential(local.publicKey),expiresAt:clock+1000};
 const checkpoint=createAdmissionCheckpoint({policy:{authorize:()=>c,verify},networkId:room,peerPublicKey:local.publicKey,now:()=>clock});
 await checkpoint.authorize();
 const raw=createDoorway({discovery:true,open(){opens++;return {announce(){sends++;},reconcile(){sends++;},sendSignal(){sends++;},close(){}};}});
 const gated=withAdmissionDoorway(raw,{checkpoint,credential:c,identity:local,now:()=>clock});
 const session=gated.open({roomId:room,peerId:local.publicKey.slice(0,16)});
 clock+=1001;
 assert.throws(()=>session.announce({roomId:room,peerId:local.publicKey.slice(0,16)}),{code:'PHANTOM_ADMISSION_DENIED'});
 assert.throws(()=>gated.open({roomId:room,peerId:local.publicKey.slice(0,16)}),{code:'PHANTOM_ADMISSION_DENIED'});
 assert.deepEqual([opens,sends],[1,0]);session.close();
});

test('raw custom adapter carries the same admission proof without Nostr',async()=>{
 const local=createNostrIdentity(),remote=createNostrIdentity();
 const checkpoint=createAdmissionCheckpoint({policy:{authorize:()=>credential(local.publicKey),verify},networkId:room,peerPublicKey:local.publicKey});
 const c=await checkpoint.authorize();let wire,sent=[],hints=[];
 const adapter=createDoorway({open(context){wire=context;return {send:p=>sent.push(p),close(){}};}});
 const gated=withAdmissionDoorway(adapter,{checkpoint,credential:c,identity:local});
 const session=gated.open({roomId:room,peerId:local.publicKey.slice(0,16),onDiscovery:h=>hints.push(h),onSignal(){}});
 session.announce({roomId:room,peerId:local.publicKey.slice(0,16),bootId:'local',t:Date.now()});
 assert.equal(sent[0].type,'concierge_connect');assert.ok(sent[0].admissionProof);
 wire.onPacket({type:'concierge_route',videoId:room,syscon1:remote.publicKey.slice(0,16),list:[announcement(remote,credential(remote.publicKey)).row]});
 await tick();assert.equal(hints[0].peers.length,1);session.close();
});
