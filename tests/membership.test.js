import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createAdmissionVerifier} from '../packages/phantom-core/src/admission.js';
import {createMembershipAuthority,createMemoryMembershipStore} from '../packages/phantom-core/src/membership.js';
import {createNostrIdentity} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {verifyPeerProof} from '../packages/phantom-doorways/src/peer-proof.js';
const room='a'.repeat(64), other='b'.repeat(64);
const hex=b=>Buffer.from(b).toString('hex');
const digest=async token=>hex(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(token)));
async function fixture(store=createMemoryMembershipStore()) {
 const admin=await createAdminIdentity();let clock=Date.now();
 const options={networkId:room,adminIdentity:admin,store,now:()=>clock,
  verifyPeerRequest:({networkId,action,inviteId,membershipId,request})=>verifyPeerProof(request.proof,{networkId,kind:action,
   payload:{action,...(inviteId?{inviteId}:{}),...(membershipId?{membershipId}:{})},now:clock}) && request.proof.peerPublicKey===request.peerPublicKey &&
   (action!=='membership.leave'||request.proof.payload.membershipId===request.membershipId)};
 const authority=await createMembershipAuthority(options);
 const peer=createNostrIdentity();
 function request(identity,action,fields={}) {
  return {peerPublicKey:identity.publicKey,...(fields.membershipId?{membershipId:fields.membershipId}:{}),
   proof:identity.signPeerProof({networkId:room,kind:action,credential:null,payload:{action,...fields},issuedAt:clock,nonce:crypto.randomUUID()})};
 }
 async function redeem(token,identity=peer) {return authority.redeem({token,request:request(identity,'membership.redeem',{inviteId:await digest(token)})});}
 const verify=c=>authority.verify({networkId:room,peerPublicKey:c.peerPublicKey,credential:c});
 return {admin,authority,peer,request,redeem,verify,options,advance:ms=>clock+=ms};
}

test('one invitation has one winner under concurrent claims; winner retries and reconnects',async()=>{
 const f=await fixture(), second=createNostrIdentity();const invite=await f.authority.issueInvite();
 const outcomes=await Promise.allSettled([f.redeem(invite.token),f.redeem(invite.token,second)]);
 assert.deepEqual(outcomes.map(r=>r.status),['fulfilled','rejected']);
 const grant=outcomes[0].value;
 assert.equal(await f.verify(grant),true);
 assert.deepEqual(await f.redeem(invite.token),grant);
 assert.deepEqual(await f.authority.reconnect({request:f.request(f.peer,'membership.reconnect')}),grant);
});

test('remove blocks old grant, invite, and reconnect; fresh invite creates a new generation',async()=>{
 const f=await fixture(), invite=await f.authority.issueInvite(), old=await f.redeem(invite.token);
 await assert.rejects(f.authority.remove(f.peer.publicKey,'incorrect'));
 assert.equal(await f.verify(old),true);
 await f.authority.remove(f.peer.publicKey,old.membershipId);
 assert.equal(await f.verify(old),false);
 await assert.rejects(f.redeem(invite.token));
 await assert.rejects(f.authority.reconnect({request:f.request(f.peer,'membership.reconnect')}));
 const fresh=await f.authority.issueInvite(), current=await f.redeem(fresh.token);
 assert.notEqual(current.membershipId,old.membershipId);
 assert.equal(await f.verify(current),true);assert.equal(await f.verify(old),false);
 await assert.rejects(f.redeem(invite.token));
 await assert.rejects(f.authority.remove(f.peer.publicKey,old.membershipId));
});

test('signed leave affects only its membership generation; replay cannot remove a reinvited member',async()=>{
 const f=await fixture(), invite=await f.authority.issueInvite(), old=await f.redeem(invite.token);
 const leave=f.request(f.peer,'membership.leave',{membershipId:old.membershipId});
 await f.authority.leave({request:leave});assert.equal(await f.verify(old),false);
 const fresh=await f.authority.issueInvite(), current=await f.redeem(fresh.token);
 await assert.rejects(f.authority.leave({request:leave}));assert.equal(await f.verify(current),true);
});

test('invitation expiry and membership expiry are independent',async()=>{
 const f=await fixture(), invite=await f.authority.issueInvite({ttlMs:10,membershipTtlMs:100});
 const grant=await f.redeem(invite.token);f.advance(11);
 assert.equal(await f.verify(grant),true);
 assert.deepEqual(await f.redeem(invite.token),grant);
 f.advance(100);assert.equal(await f.verify(grant),false);await assert.rejects(f.redeem(invite.token));
 const expired=await f.authority.issueInvite({ttlMs:1});f.advance(2);await assert.rejects(f.redeem(expired.token));
});

test('forgery, token substitution, identity substitution, scope and grant tampering fail closed',async()=>{
 const f=await fixture(), invite=await f.authority.issueInvite(), second=await f.authority.issueInvite(), rogue=createNostrIdentity();
 const request=f.request(f.peer,'membership.redeem',{inviteId:await digest(invite.token)});
 await assert.rejects(f.authority.redeem({token:second.token,request}));
 await assert.rejects(f.authority.redeem({token:invite.token,request:{...request,peerPublicKey:rogue.publicKey}}));
 await assert.rejects(f.authority.redeem({token:invite.token,request:{peerPublicKey:f.peer.publicKey}}));
 const grant=await f.redeem(invite.token);
 assert.equal(await f.verify({...grant,membershipId:crypto.randomUUID()}),false);
 assert.equal(await f.verify({...grant,expiresAt:grant.expiresAt+1}),false);
 assert.equal(await f.authority.verify({networkId:other,peerPublicKey:f.peer.publicKey,credential:grant}),false);
 const legacy=await f.admin.signAdmission({networkId:room,peerPublicKey:f.peer.publicKey,expiresAt:grant.expiresAt});
 assert.equal(await f.verify(legacy),false);
 const verifier=createAdmissionVerifier({authorityPublicKey:f.admin.publicKey});
 assert.equal(await verifier({networkId:room,peerPublicKey:f.peer.publicKey,credential:legacy}),true);
});

test('authority restart preserves spent invites and revocation',async()=>{
 const f=await fixture(), invite=await f.authority.issueInvite(), grant=await f.redeem(invite.token);
 await f.authority.remove(f.peer.publicKey,grant.membershipId);
 const restored=await createMembershipAuthority(f.options);
 assert.equal(await restored.verify({networkId:room,peerPublicKey:f.peer.publicKey,credential:grant}),false);
 await assert.rejects(restored.redeem({token:invite.token,request:f.request(f.peer,'membership.redeem',{inviteId:await digest(invite.token)})}));
});

test('failed persistence never returns a successful grant or spends an invite',async()=>{
 const backing=createMemoryMembershipStore();let fail=false;
 const f=await fixture({transaction:(scope,update)=>backing.transaction(scope,async state=>{
  const result=await update(state);if(fail)throw Error('disk unavailable');return result;
 })});
 const invite=await f.authority.issueInvite();fail=true;
 await assert.rejects(f.redeem(invite.token),/disk unavailable/);
 fail=false;const grant=await f.redeem(invite.token);assert.equal(await f.verify(grant),true);
});

test('separate authority instances share atomic consumption; memory store rolls back exceptions',async()=>{
 const f=await fixture(), restored=await createMembershipAuthority(f.options), invite=await f.authority.issueInvite(), second=createNostrIdentity();
 const request=f.request(second,'membership.redeem',{inviteId:await digest(invite.token)});
 const outcomes=await Promise.allSettled([f.redeem(invite.token),restored.redeem({token:invite.token,request})]);
 assert.equal(outcomes.filter(x=>x.status==='fulfilled').length,1);
});
