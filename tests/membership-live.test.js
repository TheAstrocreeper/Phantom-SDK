import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createMembershipAuthority,createMemoryMembershipStore} from '../packages/phantom-core/src/membership.js';
import {createMembershipAdmission} from '../packages/phantom-core/src/membership-admission.js';
import {createAdmissionCheckpoint} from '../packages/phantom-core/src/admission.js';
import {createBrowserPhantom,createBrowserIdentity} from '../packages/phantom-doorways/src/browser-entry.js';
import {createNostrIdentity} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {verifyPeerProof} from '../packages/phantom-doorways/src/peer-proof.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const room='a'.repeat(64),sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function wait(check,label){const end=Date.now()+10000;while(Date.now()<end){if(check())return;await sleep(10);}throw Error('Timed out: '+label);}
const digest=async token=>Buffer.from(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(token))).toString('hex');
function bus(){
 const members=new Map();let epoch=0,one,two;
 function hint(){const keys=[...members.keys()].sort(),leader=keys.includes(one)?one:keys[0],mirror=keys.includes(two)&&two!==leader?two:keys.find(k=>k!==leader);if(leader!==one||mirror!==two)epoch++;one=leader;two=mirror;return {kind:'concierge_update',epoch,leader,mirror,peers:[...members.values()].map(m=>m.row)};}
 function publish(){const h=hint();for(const m of members.values())m.context.onDiscovery(h);}
 return ()=>createDoorway({discovery:true,presence:'live-sockets',open(context){queueMicrotask(context.onOpen);return {
  announce(v){members.set(context.peerId,{context,row:{hKey:context.peerId,hex:context.authorization.peerPublicKey,from:context.peerId,handle:v.handle,bootId:v.bootId,roomJoinedAt:v.t,admissionProof:v.admissionProof}});publish();},
  reconcile(){context.onDiscovery(hint());},sendSignal(p){queueMicrotask(()=>members.get(p.to)?.context.onSignal(p));},
  close(){if(members.get(context.peerId)?.context===context){members.delete(context.peerId);publish();}}
 };}});
}
async function fixture(){
 const admin=await createAdminIdentity(),store=createMemoryMembershipStore();let authority;
 const verifyPeerRequest=({networkId,action,inviteId,membershipId,request})=>request.proof?.peerPublicKey===request.peerPublicKey&&verifyPeerProof(request.proof,{networkId,kind:action,payload:{action,...(inviteId?{inviteId}:{}),...(membershipId?{membershipId}:{})}});
 async function get(networkId){if(!authority)authority=await createMembershipAuthority({networkId,adminIdentity:admin,store,verifyPeerRequest});return authority;}
 function request(identity,networkId,action,fields={}){return {peerPublicKey:identity.publicKey,...(fields.membershipId?{membershipId:fields.membershipId}:{}),proof:identity.signPeerProof({networkId,kind:action,credential:null,payload:{action,...fields},issuedAt:Date.now(),nonce:crypto.randomUUID()})};}
 async function grant(identity,networkId){const a=await get(networkId),invite=await a.issueInvite();return a.redeem({token:invite.token,request:request(identity,networkId,'membership.redeem',{inviteId:await digest(invite.token)})});}
 function policy(identity,extra={}) {return createMembershipAdmission({authorityPublicKey:admin.publicKey,
  authorize:({networkId})=>grant(identity,networkId),readRevocations:()=>authority.getRevocations(),
  subscribeRevocations:({onRevocation})=>authority.onRevocation(onRevocation),
  leave:({request})=>authority.leave({request}),...extra});}
 return {admin,get,grant,policy,request,authority:()=>authority};
}

test('signed revocation invalidates cached admission; forged/cross-network records cannot remove peers',async()=>{
 const f=await fixture(),identity=createNostrIdentity(),c=await f.grant(identity,room),p=f.policy(identity,{authorize:()=>c});
 const checkpoint=createAdmissionCheckpoint({policy:p,networkId:room,peerPublicKey:identity.publicKey});
 try{
  await checkpoint.authorize();assert.equal(checkpoint.allows(identity.publicKey.slice(0,16)),true);
  const rogue=await createAdminIdentity(),fake=await rogue.signMembershipRevocation({networkId:room,peerPublicKey:identity.publicKey,membershipId:c.membershipId,reason:'removed'});
  await assert.rejects(p.applyRevocation(fake));assert.equal(checkpoint.allows(identity.publicKey.slice(0,16)),true);
  const wrong=await f.admin.signMembershipRevocation({...fake,networkId:'b'.repeat(64)});
  await p.applyRevocation(wrong);assert.equal(checkpoint.allows(identity.publicKey.slice(0,16)),true);
  await f.authority().remove(identity.publicKey,c.membershipId);
  await wait(()=>!checkpoint.allows(identity.publicKey.slice(0,16)),'cache invalidation');
  assert.equal(await checkpoint.verify(identity.publicKey,c),false);
 }finally{checkpoint.close();}
});

test('authority silence expires the membership freshness lease and closes subscriptions',async()=>{
 const f=await fixture(),identity=createNostrIdentity(),c=await f.grant(identity,room);let offline=false,active=0;
 const p=f.policy(identity,{authorize:()=>c,pollIntervalMs:10,maxAgeMs:30,
  readRevocations:()=>offline?new Promise(()=>{}):f.authority().getRevocations(),
  subscribeRevocations:()=>{active++;return ()=>active--;}});
 const checkpoint=createAdmissionCheckpoint({policy:p,networkId:room,peerPublicKey:identity.publicKey});let stale=false;
 checkpoint.onChange(e=>{if(e.type==='stale')stale=true;});
 try{await checkpoint.authorize();offline=true;await wait(()=>stale,'stale lease notification');assert.equal(checkpoint.allows(identity.publicKey.slice(0,16)),false);}
 finally{checkpoint.close();assert.equal(active,0);}
});

test('live SDK peers stop messaging and active distribution on Remove; fresh invite reuses identity and Leave closes locally', {timeout:30000},async()=>{
 let heldBinary=0,holdBinary=true;
 const f=await fixture(),adapter=bus(),RTC=memoryRTC({onSend:(data,channel)=>{if(holdBinary&&channel.label==='phantom-distribution'&&data instanceof ArrayBuffer){heldBinary++;return false;}return true;}}),networks=[],identities=[],credentials=new Map(),received=[];
 async function peer(name,invite,saved,blind=false){
  const seed=saved??createBrowserIdentity(),identity=createNostrIdentity(seed);identities.push(identity);
  const pcs=[];class PeerRTC extends RTC{constructor(){super();pcs.push(this);}}
  const admission=f.policy(identity,{...(blind?{subscribeRevocations:()=>()=>{},pollIntervalMs:10000,maxAgeMs:30000}:{}),authorize:async({networkId})=>{const c=await f.grant(identity,networkId);credentials.set(identity.publicKey,c);return c;}});
  const values=new Map(),storage={getItem:k=>values.get(k)??null,setItem:(k,v)=>values.set(k,v)};
  const sdk=createBrowserPhantom({identity:seed,admission,...(invite?{adminPublicKey:f.admin.publicKey}:{adminIdentity:f.admin}),distribution:{intervalMs:10},messaging:'automatic',doorways:[{type:'memory'}],adapters:{memory:adapter},runtime:{crypto,localStorage:storage,RTCPeerConnection:PeerRTC,setTimeout,clearTimeout,setInterval,clearInterval}});
  const n=await(invite?sdk.join(invite,{name,timeoutMs:10000}):sdk.create({name}));networks.push(n);n.on('message',m=>received.push({name,...m}));return {n,seed,identity,pcs};
 }
 try{
  const a=await peer('a'),b=await peer('b',a.n.invite,undefined,true);
  await wait(()=>a.n.getPeerConnections().some(p=>p.state==='ready')&&b.n.getPeerConnections().some(p=>p.state==='ready'),'automatic chat');
  b.n.send(a.n.getState().peerId,'before remove');await wait(()=>received.some(m=>m.text==='before remove'),'message before remove');
  await a.n.distributeFile(new Blob([new Uint8Array(65536)]));
  await wait(()=>heldBinary>0,'active distribution transfer');
  assert.ok(a.n.getDistributionState().connections.length>0);
  const old=credentials.get(b.identity.publicKey);
  const oldChannel=a.pcs.flatMap(pc=>pc.channels).find(dc=>dc.label==='six7'&&dc.readyState==='open');assert.ok(oldChannel);const oldHandler=oldChannel.onmessage;
  await f.authority().remove(b.identity.publicKey,old.membershipId);
  await wait(()=>!a.n.getPeerConnections().some(p=>p.peerId===b.identity.publicKey.slice(0,16)&&p.state==='ready'),'survivor channel closes');
  assert.equal(a.n.getDistributionState().connections.length,0,'distribution channels close on removal');
  assert.equal(a.n.getDistributionState().jobs.some(j=>j.source===b.identity.publicKey.slice(0,16)||j.receiver===b.identity.publicKey.slice(0,16)),false,'revoked peer jobs are removed');
  assert.equal(oldChannel.readyState,'closed','survivor closes the channel without waiting for the removed device to cooperate');
  assert.notEqual(b.n.getState().state,'closed','removed device has not received revocation');
  await oldHandler({data:JSON.stringify({t:'MSG',text:'revoked old channel'})});await sleep(10);assert.equal(received.some(m=>m.text==='revoked old channel'),false);
  b.n.close();
  assert.equal(await f.authority().verify({networkId:a.n.networkId,peerPublicKey:b.identity.publicKey,credential:old}),false);
  holdBinary=false;
  const rejoined=await peer('b-again',a.n.invite,b.seed);
  assert.notEqual(credentials.get(b.identity.publicKey).membershipId,old.membershipId);
  await wait(()=>rejoined.n.getPeerConnections().some(p=>p.state==='ready'),'fresh membership reconnects');
  await oldHandler({data:JSON.stringify({t:'MSG',text:'stale old generation'})});await sleep(10);assert.equal(received.some(m=>m.text==='stale old generation'),false);
  rejoined.n.send(a.n.getState().peerId,'after reinvite');await wait(()=>received.some(m=>m.text==='after reinvite'),'message after reinvite');
  await rejoined.n.leave();await wait(()=>rejoined.n.getState().state==='closed','leave closes local peer');
 }finally{for(const n of networks)n.close();}
});

test('membership credential expiry emits loss without waiting for another packet',async()=>{
 const f=await fixture(),identity=createNostrIdentity();await f.get(room);
 const c=await f.admin.signMembership({networkId:room,peerPublicKey:identity.publicKey,membershipId:crypto.randomUUID(),expiresAt:Date.now()+120});
 const p=f.policy(identity,{authorize:()=>c}),checkpoint=createAdmissionCheckpoint({policy:p,networkId:room,peerPublicKey:identity.publicKey});let expired=false;
 checkpoint.onChange(e=>{if(e.type==='expired')expired=true;});
 try{await checkpoint.authorize();await wait(()=>expired,'membership expiry');assert.equal(checkpoint.allows(identity.publicKey.slice(0,16)),false);}
 finally{checkpoint.close();}
});

test('a source read queued before removal cannot send after the peer gets fresh access',async()=>{
 const {createFileDistribution}=await import('../packages/phantom-core/src/files.js');
 const callbacks=new Set(),commands=[],peer='b'.repeat(16),data=new Uint8Array(2048);let block=false,reading=false,release;
 const pending=new Promise(resolve=>release=resolve);
 const file={size:data.length,slice:(start,end)=>({async arrayBuffer(){if(block){reading=true;await pending;}return data.slice(start,end).buffer;}})};
 const core={onEvent:fn=>{callbacks.add(fn);return ()=>callbacks.delete(fn);},command(type,payload){commands.push({type,payload});if(type==='SEND_FILE_BIN_CHUNK')for(const fn of callbacks)fn({type:'FILE_BIN_CHUNK_SENT',payload:{...payload,ok:true}});}};
 let allowed=true;const files=createFileDistribution({core,canUsePeer:()=>allowed});
 const emit=event=>{for(const fn of callbacks)fn(event);};
 try{
  const manifest=await files.publish(file);block=true;
  const request={type:'INCOMING_MSG',payload:{from:peer,sys:'PHANTOM_FILE_REQUEST',file:{id:manifest.id,index:0}}};
  emit(request);emit(request);await wait(()=>reading,'blocked source read');
  allowed=false;files.dropPeer(peer);allowed=true;block=false;release();await sleep(20);
  assert.equal(commands.filter(c=>c.type==='SEND_FILE_BIN_CHUNK').length,0);
  emit(request);await wait(()=>commands.some(c=>c.type==='SEND_FILE_BIN_CHUNK'),'fresh request sends');
  assert.equal(commands.filter(c=>c.type==='SEND_FILE_BIN_CHUNK').length,1);
 }finally{files.close();release();}
});
