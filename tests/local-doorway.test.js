import test from 'node:test';
import assert from 'node:assert/strict';
import {createLocalDoorways,browserLocalTransport} from '@phantom-local/doorways/local';
const localInvitation=(base,secret,hostKey,code)=>JSON.stringify({endpoint:'http://192.168.0.214:42087',secret,hostKey,code,token:'A'.repeat(32)});
const parseLocalInvitation=JSON.parse;
import {createNostrIdentity} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createMembershipAuthority,createMemoryMembershipStore} from '../packages/phantom-core/src/membership.js';
import {createMembershipPairingHost,connectMembershipPairing,verifyMembershipPeerRequest} from '../packages/phantom-doorways/src/membership-pairing.js';
import {createRequire} from 'node:module';
import {createLanPairingHost,createLanPairingClient} from '@phantom-local/doorways/local';
import {encodeInvite} from '../packages/phantom-core/src/invite.js';
const secret='ab'.repeat(32), base='L2:192.168.0.214:42087:'+'A'.repeat(32);
function pipes(){
 const listeners=[null,null],queued=[[],[]],sent=[[],[]],onSend=[null,null];
 return {sent,onSend,inject(side,value){listeners[side]?.(value);},transports:[0,1].map(side=>({
  send:async value=>{sent[side].push(value);onSend[side]?.(value);const other=1-side;if(listeners[other])listeners[other](value);else queued[other].push(value);},
  listen(receive){listeners[side]=receive;for(const value of queued[side].splice(0))receive(value);return ()=>{listeners[side]=null;};},
 }))};
}
test('real PAKE bootstrap, membership enrollment, signed revocation and leave work over encrypted local doorways', {timeout:6000},async()=>{
 const host=createNostrIdentity(),guest=createNostrIdentity(),admin=await createAdminIdentity(),networkId='12'.repeat(32),errors=[];
 const wire=pipes();
 const hostHub=await createLocalDoorways({identity:host,secret,hostKey:host.publicKey,host:true,transport:wire.transports[0],onError:e=>errors.push(e)});
 let guestHub;
 const authority=await createMembershipAuthority({networkId,adminIdentity:admin,store:createMemoryMembershipStore(),verifyPeerRequest:verifyMembershipPeerRequest});
 const inviter=createMembershipPairingHost({identity:host,authority,invite:encodeInvite({networkId,adminPublicKey:admin.publicKey,doorways:[{type:'lan'}]}),doorwayFactory:()=>hostHub.doorway(),onError:e=>errors.push(e)});
 let control;
 try{
  const invitation=await inviter.newInvite();
  const pake=createRequire(import.meta.url)('spake2-wasm'),route='http://192.168.0.214:42087',password='2W9X7';
  const h=createLanPairingHost({route,password,hostPublicKey:host.publicKey,invitation:localInvitation(base,secret,host.publicKey,invitation.code),pake});
  const g=createLanPairingClient({route,password,hostPublicKey:host.publicKey,peerPublicKey:guest.publicKey,pake});
  let recovered;
  try {
   const answer=await g.answer(h.begin(guest.publicKey,g.hello));
   recovered=parseLocalInvitation(await g.open(await h.finish(guest.publicKey,g.hello,answer)));
  } finally {h.close();g.close();}
  guestHub=await createLocalDoorways({identity:guest,secret:recovered.secret,hostKey:recovered.hostKey,host:false,transport:wire.transports[1],onError:e=>errors.push(e)});
  control=await connectMembershipPairing({identity:guest,code:recovered.code,doorwayFactory:()=>guestHub.doorway(),onError:e=>errors.push(e)});
  const credential=await control.admission.authorize({networkId,peerPublicKey:guest.publicKey});
  assert.equal(await authority.verify({networkId,peerPublicKey:guest.publicKey,credential}),true);
  assert.equal(await control.admission.verify({networkId,peerPublicKey:guest.publicKey,credential}),true);
  assert.ok(wire.sent.flat().length>6);
  for(const message of wire.sent.flat()){assert.match(message,/^[A-Za-z0-9+/=]+$/);assert.ok(!message.includes('membership'));assert.ok(!message.includes(credential.membershipId));}
  await control.releaseMembership();
  assert.equal(await authority.verify({networkId,peerPublicKey:guest.publicKey,credential}),false);
  assert.equal(errors.length,0,errors.map(e=>e.message).join(';'));
 }finally{control?.close();inviter.close();hostHub.close();guestHub?.close();}
});
test('ciphertext replay closes the local channel instead of processing another signal',{timeout:5000},async()=>{
 const host=createNostrIdentity(),guest=createNostrIdentity(),wire=pipes();
 const first=new Promise(resolve=>{wire.onSend[0]=resolve;});
 let rejectReplay;const rejected=new Promise(resolve=>{rejectReplay=resolve;});
 const h=await createLocalDoorways({identity:host,secret,hostKey:host.publicKey,host:true,transport:wire.transports[0]});
 const g=await createLocalDoorways({identity:guest,secret,hostKey:host.publicKey,host:false,transport:wire.transports[1],onError:rejectReplay});
 const session=h.doorway().open({roomId:'room',peerId:host.publicKey.slice(0,16),onOpen(){},onDiscovery(){},onSignal(){}});
 session.announce({roomId:'room',peerId:host.publicKey.slice(0,16),bootId:'one',t:Date.now()});
 g.doorway().open({roomId:'room',peerId:guest.publicKey.slice(0,16),onOpen(){},onDiscovery(){},onSignal(){}});
 const original=await first;
 wire.inject(1,original);
 assert.match((await rejected).message,/authentication/);
 h.close();g.close();
});
test('browser transport stops on failure and never follows a redirect or retries remotely',async()=>{
 let failed, requests=[];
 const transport=browserLocalTransport(parseLocalInvitation(localInvitation(base,secret,'12'.repeat(32),'P1-1234')),{fetch:async(url,options)=>{requests.push({url,options});return new Response('',{status:403});}});
 await new Promise(resolve=>transport.listen(()=>assert.fail('unexpected receive'),error=>{failed=error;resolve();}));
 assert.equal(requests.length,1);assert.match(failed.message,/403/);
 assert.equal(requests[0].url,'http://192.168.0.214:42087/receive');
 assert.equal(requests[0].options.redirect,'error');assert.equal(requests[0].options.credentials,'omit');assert.equal(requests[0].options.targetAddressSpace,'local');
 transport.close();
});

test('public HTTP transport rejects destinations and capabilities before any request',()=>{
  let calls=0;const fetch=()=>{calls++;throw Error('unexpected request');};
  for(const endpoint of ['http://8.8.8.8:42087','http://localhost:42087','http://192.168.0.214:80','http://192.168.0.214:42087/','http://192.168.0.214:42087@evil.test','http://192.168.0.256:42087'])assert.throws(()=>browserLocalTransport({endpoint,token:'A'.repeat(32)},{fetch}));
  assert.throws(()=>browserLocalTransport({endpoint:'http://192.168.0.214:42087',token:'bad'},{fetch}));
  assert.equal(calls,0);
});
test('host identity mismatch and malformed shared secrets allocate no transport listener',async()=>{
  const identity=createNostrIdentity();let calls=0;
  const transport={send(){},listen(){calls++;}};
  await assert.rejects(createLocalDoorways({identity,secret:'bad',hostKey:identity.publicKey,host:true,transport}));
  await assert.rejects(createLocalDoorways({identity,secret,hostKey:'f'.repeat(64),host:true,transport}));
  assert.equal(calls,0);
});

test('a guest joining after PIN expiry receives fresh ordered signaling',{timeout:6000},async(t)=>{
 const realNow=Date.now;let now=realNow();
 t.mock.method(Date,'now',()=>now);
 const host=createNostrIdentity(),guest=createNostrIdentity(),wire=pipes(),errors=[];
 const h=await createLocalDoorways({identity:host,secret,hostKey:host.publicKey,host:true,transport:wire.transports[0],onError:e=>errors.push(e)});
 let g;
 const context=identity=>({roomId:'waiting-room',peerId:identity.publicKey.slice(0,16),onOpen(){},onDiscovery(){},onSignal(){}});
 try{
  const first=h.doorway().open(context(host));
  first.announce({roomId:'waiting-room',peerId:host.publicKey.slice(0,16),bootId:'one',t:now});
  // Allow the original implementation to queue its startup envelopes.
  await new Promise(resolve=>setTimeout(resolve,30));
  now+=240001;
  first.close();
  const next=h.doorway().open(context(host));
  next.announce({roomId:'waiting-room',peerId:host.publicKey.slice(0,16),bootId:'two',t:now});
  g=await createLocalDoorways({identity:guest,secret,hostKey:host.publicKey,host:false,transport:wire.transports[1],onError:e=>errors.push(e)});
  let received;
  const joined=new Promise(resolve=>{received=resolve;});
  const guestChannel=g.doorway().open({...context(guest),onSignal:received});
  guestChannel.announce({roomId:'waiting-room',peerId:guest.publicKey.slice(0,16),bootId:'guest',t:now});
  next.sendSignal({from:host.publicKey.slice(0,16),to:guest.publicKey.slice(0,16),videoId:'waiting-room',data:'fresh'});
  const signal=await Promise.race([joined,new Promise((_,reject)=>setTimeout(()=>reject(Error(errors.map(e=>e.message).join(';')||'No signal')),1000))]);
  assert.equal(signal.data,'fresh');assert.equal(errors.length,0);
 }finally{h.close();g?.close();}
});

test('local adapter supports private HTTPS without weakening destination validation',()=>{
  const fetch=()=>{throw Error('must not connect during validation');};
  const transport=browserLocalTransport({endpoint:'https://192.168.0.214:42087',token:'A'.repeat(32)},{fetch});
  transport.close();
  for(const endpoint of ['https://8.8.8.8:42087','https://localhost:42087','https://192.168.0.214:443','https://192.168.0.214:42087/path','https://user@192.168.0.214:42087'])
    assert.throws(()=>browserLocalTransport({endpoint,token:'A'.repeat(32)},{fetch}));
});
