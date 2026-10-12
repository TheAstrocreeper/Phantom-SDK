import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdmissionCheckpoint} from '../packages/phantom-core/src/admission.js';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createNostrDoorway} from '../packages/phantom-doorways/src/nostr-doorway.js';
import {createNostrIdentity,verifyNostrEnvelope} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {encodeInvite} from '../packages/phantom-core/src/invite.js';
const room='a'.repeat(64), otherRoom='b'.repeat(64);
const publicKey='c'.repeat(64);
const invite=encodeInvite({networkId:room,relays:['wss://relay.example']});
const tick=()=>new Promise(resolve=>setImmediate(resolve));
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
 let clock=Date.now();
 const checkpoint=createAdmissionCheckpoint({policy:{authorize:()=>credential(),verify},networkId:room,peerPublicKey:publicKey,now:()=>clock});
 await checkpoint.authorize();assert.equal(checkpoint.allows(publicKey.slice(0,16)),true);
 assert.equal(await checkpoint.verify('d'.repeat(64),credential()),false);
 assert.equal(await checkpoint.verify(publicKey,credential(publicKey,otherRoom)),false);
 clock+=60001;assert.equal(checkpoint.allows(publicKey.slice(0,16)),false);
 assert.equal(await checkpoint.verify(publicKey,credential()),false);
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
 return verifyNostrEnvelope(identity.sign(room,{type:'ANNOUNCEMENT',admissionCredential:c,row:{hKey:identity.publicKey.slice(0,16),hex:identity.publicKey,bootId:'boot',roomJoinedAt:Date.now()}}),room);
}

test('Nostr admits only verified signed identities; copied credentials and bare signals stay out',async()=>{
 // The real signed-envelope verifier supplies the public key used by admission.
 const local=createNostrIdentity();
 const checkpoint=createAdmissionCheckpoint({policy:{authorize:()=>credential(local.publicKey),verify},networkId:room,peerPublicKey:local.publicKey});
 const c=await checkpoint.authorize();
 let busConfig,hints=[],received=[],outbound=[];
 const doorway=createNostrDoorway({identity:local,relays:['wss://relay.example'],admission:{checkpoint,credential:c},busFactory:config=>{busConfig=config;return {announce:p=>outbound.push(p),sendSignal(){},close(){},depart(){}};}});
 const session=doorway.open({roomId:room,peerId:local.publicKey.slice(0,16),onOpen(){},onDiscovery:h=>hints.push(h),onSignal:p=>received.push(p),onClose(){},onError:e=>{throw e;}});
 session.announce({roomId:room,peerId:local.publicKey.slice(0,16),bootId:'local',t:Date.now()});
 assert.deepEqual(outbound[0].admissionCredential,c);
 const approved=createNostrIdentity(),rogue=createNostrIdentity();
 busConfig.onAnnouncement(announcement(rogue,credential(approved.publicKey)));
 busConfig.onAnnouncement(announcement(rogue,undefined));
 await tick();assert.equal(hints.at(-1).peers.length,1);
 assert.equal(checkpoint.allows(rogue.publicKey.slice(0,16)),false);
 busConfig.onAnnouncement(announcement(approved,credential(approved.publicKey)));
 await tick();assert.equal(hints.at(-1).peers.length,2);
 const control={type:'rtc_offer',videoId:room,from:rogue.publicKey.slice(0,16),to:local.publicKey.slice(0,16)};
 const packet=verifyNostrEnvelope(rogue.sign(room,{type:'SIGNAL',control,admissionCredential:credential(approved.publicKey)}),room);
 busConfig.onSignal(packet);await tick();assert.equal(received.length,0);
 const good=verifyNostrEnvelope(approved.sign(room,{type:'SIGNAL',control:{...control,from:approved.publicKey.slice(0,16)},admissionCredential:credential(approved.publicKey)}),room);
 busConfig.onSignal(good);await tick();assert.equal(received.length,1);
 session.close();
});

test('open Nostr networks remain compatible',()=>{
 const f=fixture();f.session.announce({roomId:room,peerId:f.identity.publicKey.slice(0,16),bootId:'local',t:Date.now()});
 const remote=createNostrIdentity();f.busConfig.onAnnouncement(announcement(remote));
 assert.equal(f.hints.at(-1).peers.length,2);f.session.close();
});

test('unsupported adapters fail closed before relay discovery',async()=>{
 let probes=0;
 const sdk=createBrowserPhantom({doorways:[{type:'websocket',url:'wss://example.com'}],admission:{authorize:({networkId,peerPublicKey})=>credential(peerPublicKey,networkId),verify},relayFinder:()=>{probes++;}});
 await assert.rejects(sdk.create(),/requires a Nostr doorway/);assert.equal(probes,0);
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
 assert.equal(payload.admissionCredential.peerPublicKey,payload.s7pPublicKey);
 assert.equal(payload.admissionCredential.networkId,network.networkId);
 network.close();
});
