import test from 'node:test';
import assert from 'node:assert/strict';
import {createMembershipPairingHost} from '@phantom-local/doorways/pairing';
import {encodeInvite} from '../packages/phantom-core/src/invite.js';
const networkId='a'.repeat(64),adminPublicKey='b'.repeat(64);
const invite=encodeInvite({networkId,adminPublicKey,doorways:[{type:'lan'}]});
test('local invitation policy is opt-in and preserves legacy lifetimes',async()=>{
  for(const [options,expected] of [[{},undefined],[{pake:{}},{ttlMs:240000}],[{inviteTtlMs:12345},{ttlMs:12345}]]){
    let captured;
    const authority={networkId,onRevocation(){return()=>{};},async issueInvite(value){captured=value;throw Error('captured');}};
    const host=createMembershipPairingHost({invite,authority,doorwayFactory(){},...options});
    try{await assert.rejects(host.newInvite(),/captured/);assert.deepEqual(captured,expected);}finally{host.close();}
  }
  for(const inviteTtlMs of [0,-1,Infinity,'240000'])assert.throws(()=>createMembershipPairingHost({inviteTtlMs}),/lifetime/);
});
