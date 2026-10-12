import test from 'node:test';
import assert from 'node:assert/strict';
import worker,{PhantomDoorway} from '../templates/cloudflare-doorway/src/index.js';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createNostrIdentity} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {verifyPeerProof} from '../packages/phantom-doorways/src/peer-proof.js';
const room='a'.repeat(64);
class Socket {
 readyState=1;sent=[];
 constructor(hKey,endpointId=crypto.randomUUID()){this.a={hKey,networkId:room,endpointId,connectedAt:Date.now(),joinDeadline:Date.now()+10000,joined:false};}
 serializeAttachment(a){assert.ok(new TextEncoder().encode(JSON.stringify(a)).length<2048);this.a=structuredClone(a);}
 deserializeAttachment(){return structuredClone(this.a);}
 send(raw){this.sent.push(JSON.parse(raw));}
 close(code,reason){this.readyState=3;this.closed={code,reason};}
}
function state(){
 const values=new Map(),sockets=[];
 return {values,sockets,storage:{
  async get(key){return Array.isArray(key)?new Map(key.filter(k=>values.has(k)).map(k=>[k,structuredClone(values.get(k))])):structuredClone(values.get(key));},
  async put(key,value){if(typeof key==='string')values.set(key,structuredClone(value));else for(const [k,v]of Object.entries(key))values.set(k,structuredClone(v));},
  async delete(key){return values.delete(key);},async setAlarm(at){values.set('alarm',at);},async deleteAlarm(){values.delete('alarm');}
 },blockConcurrencyWhile:fn=>Promise.resolve().then(fn),getWebSockets:()=>sockets,acceptWebSocket:s=>sockets.push(s)};
}
async function fixture(){const admin=await createAdminIdentity(),storage=state();const env={ADMISSION_MODE:'restricted',ADMISSION_AUTHORITY_PUBLIC_KEY:admin.publicKey};const instance=new PhantomDoorway(storage,env);await instance.ready;return {admin,storage,env,instance};}
async function joinPacket(admin,identity,socket,{padding}={}){
 const c=await admin.signAdmission({networkId:room,peerPublicKey:identity.publicKey,expiresAt:Date.now()+60000});
 const payload={roomId:room,peerId:identity.publicKey.slice(0,16),handle:'Peer',bootId:'boot-'+socket.a.endpointId,t:Date.now(),connectionChallenge:socket.a.endpointId,...(padding?{padding}:{})};
 const proof=identity.signPeerProof({networkId:room,kind:'announce',credential:c,payload,issuedAt:Date.now(),nonce:crypto.randomUUID()});
 const {roomId,peerId,...fields}=payload;return {...fields,type:'concierge_connect',videoId:roomId,from:peerId,admissionProof:proof};
}
async function add(f,identity,options){const s=new Socket(identity.publicKey.slice(0,16));f.storage.sockets.push(s);const p=await joinPacket(f.admin,identity,s,options);await f.instance.webSocketMessage(s,JSON.stringify(p));return {s,p};}

test('worker rejects old admin/HTTP exit routes and invalid IDs before DO lookup',async()=>{
 let lookups=0;const env={SWITCHBOARD:{idFromName(){lookups++;}},ADMISSION_MODE:'open'};
 for(const path of ['/admin/token?to=abcd','/doorway/exit?v='+room]){
  const response=await worker.fetch(new Request('https://relay.example'+path,{method:'POST'}),env);assert.equal(response.status,404);
 }
 const response=await worker.fetch(new Request('https://relay.example/ws?v=bad&to=bad',{headers:{Upgrade:'websocket'}}),env);
 assert.equal(response.status,400);assert.equal(lookups,0);
});

test('restricted worker refuses incomplete configuration and disallowed origins',async()=>{
 const request=new Request(`https://relay.example/ws?v=${room}&to=${'b'.repeat(16)}`,{headers:{Upgrade:'websocket',Origin:'https://unwanted.example'}});
 assert.equal((await worker.fetch(request,{ALLOWED_ORIGINS:'https://allowed.example'})).status,403);
 assert.equal((await worker.fetch(request,{})).status,503);
});

test('roster proofs survive hibernation and signals are forwarded without modification',async()=>{
 const f=await fixture(),alice=createNostrIdentity(),bob=createNostrIdentity();
 const a=await add(f,alice),b=await add(f,bob);
 assert.equal(f.instance.peers.size,2);
 const roster=b.s.sent.find(p=>p.type==='concierge_route');
 assert.equal(roster.list[0].admissionProof.kind,'announce');
 const woke=new PhantomDoorway(f.storage,f.env);await woke.ready;f.instance=woke;
 assert.deepEqual(woke.peers.get(a.s.a.hKey).admissionProof,a.p.admissionProof);
 const payload={type:'rtc_offer',videoId:room,from:a.s.a.hKey,to:b.s.a.hKey,sdp:{type:'offer',sdp:'exact fields'}};
 const packet={...payload,admissionProof:alice.signPeerProof({networkId:room,kind:'signal',credential:a.p.admissionProof.credential,payload,issuedAt:Date.now(),nonce:crypto.randomUUID()})};
 await woke.webSocketMessage(a.s,JSON.stringify(packet));assert.deepEqual(b.s.sent.at(-1),packet);
 await woke.webSocketMessage(a.s,JSON.stringify({...packet,sdp:'altered'}));assert.deepEqual(b.s.sent.at(-1),packet);
});

test('copied grants and replayed announcements cannot join or replace an existing socket',async()=>{
 const f=await fixture(),alice=createNostrIdentity(),rogue=createNostrIdentity();const a=await add(f,alice);
 const impersonator=new Socket(a.s.a.hKey);f.storage.sockets.push(impersonator);
 await f.instance.webSocketMessage(impersonator,JSON.stringify(a.p));
 assert.equal(impersonator.closed.reason,'challenge_mismatch');assert.equal(f.instance.sockets.get(a.s.a.hKey),a.s);assert.equal(a.s.readyState,1);
 const r=new Socket(rogue.publicKey.slice(0,16));f.storage.sockets.push(r);
 const payload={roomId:room,peerId:r.a.hKey,bootId:'rogue',handle:'Peer',t:Date.now(),connectionChallenge:r.a.endpointId};
 const p={type:'concierge_connect',videoId:room,from:r.a.hKey,handle:payload.handle,bootId:payload.bootId,t:payload.t,connectionChallenge:payload.connectionChallenge,
  admissionProof:rogue.signPeerProof({networkId:room,kind:'announce',credential:a.p.admissionProof.credential,payload,issuedAt:Date.now(),nonce:crypto.randomUUID()})};
 await f.instance.webSocketMessage(r,JSON.stringify(p));assert.equal(r.closed.reason,'admission_denied');assert.equal(f.instance.peers.size,1);
});

test('valid replacements retain ownership when old close events arrive',async()=>{
 const f=await fixture(),alice=createNostrIdentity();const old=await add(f,alice),replacement=await add(f,alice);
 assert.equal(old.s.closed.reason,'doorway_replaced');
 await f.instance.webSocketClose(old.s);
 assert.equal(f.instance.sockets.get(replacement.s.a.hKey),replacement.s);assert.equal(f.instance.pending.size,0);
 assert.equal(f.instance.peers.get(replacement.s.a.hKey).endpointId,replacement.s.a.endpointId);
});

test('disconnected peer lease survives wake and is removed after its grace alarm',async()=>{
 const f=await fixture(),alice=createNostrIdentity();const a=await add(f,alice);
 a.s.readyState=3;await f.instance.webSocketClose(a.s);assert.equal(f.instance.peers.size,1);
 const pending=f.storage.values.get('pending');pending[0][1].dueAt=Date.now()-1;f.storage.values.set('pending',pending);
 const woke=new PhantomDoorway(f.storage,f.env);await woke.ready;assert.equal(woke.peers.size,1);
 await woke.alarm();assert.equal(woke.peers.size,0);assert.equal(woke.pending.size,0);
 assert.equal(f.storage.values.has('peer:'+a.s.a.endpointId),false);
});

test('large proofs use durable records rather than oversized WebSocket attachments',async()=>{
 const f=await fixture();const {s,p}=await add(f,createNostrIdentity(),{padding:'x'.repeat(5000)});
 assert.ok(JSON.stringify(p.admissionProof).length>2048);assert.ok(JSON.stringify(s.a).length<2048);
 const woke=new PhantomDoorway(f.storage,f.env);await woke.ready;
 assert.deepEqual(woke.peers.get(s.a.hKey).admissionProof,p.admissionProof);
 assert.equal(verifyPeerProof(p.admissionProof,{networkId:room,kind:'announce'}),true);
});

test('unjoined and stale sockets cannot route signaling or remove other peers',async()=>{
 const f=await fixture(),alice=createNostrIdentity(),bob=createNostrIdentity();const a=await add(f,alice),b=await add(f,bob);
 const unjoined=new Socket(a.s.a.hKey);f.storage.sockets.push(unjoined);const before=b.s.sent.length;
 await f.instance.webSocketMessage(unjoined,JSON.stringify({type:'rtc_offer',videoId:room,from:a.s.a.hKey,to:b.s.a.hKey}));assert.equal(b.s.sent.length,before);
 await f.instance.webSocketMessage(unjoined,JSON.stringify({type:'doorway_exit',videoId:room,from:a.s.a.hKey}));assert.equal(f.instance.peers.size,2);
});

test('a close event that wakes a hibernated object still creates the departure lease',async()=>{
 const f=await fixture();const a=await add(f,createNostrIdentity());a.s.readyState=3;
 const woke=new PhantomDoorway(f.storage,f.env);await woke.ready;
 assert.equal(woke.sockets.size,0);await woke.webSocketClose(a.s);
 assert.equal(woke.pending.get(a.s.a.hKey).endpointId,a.s.a.endpointId);
 assert.equal(woke.peers.size,1);
});
