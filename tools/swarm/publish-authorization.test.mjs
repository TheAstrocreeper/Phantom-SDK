import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity,importAdminIdentity,createAdminAuthorization} from '../../packages/phantom-core/src/authorization.js';
import {createPublishAuthorizer} from './publish-authorization.mjs';
test('controller retains admin authority independently of departed publisher and elected roles',async()=>{
 const original=await createAdminIdentity(),identity=await importAdminIdentity(await original.exportSeed());
 const authorize=createPublishAuthorizer({identity,networkId:'room'}),target='sha256:'+'a'.repeat(64);
 const proof=await authorize({networkId:'room',target,role:'syscon1',action:'network.retire'});
 const policy=createAdminAuthorization({networkId:'room',adminPublicKey:original.publicKey});
 assert.equal((await policy.verify(proof)).action,'content.publish');assert.equal(proof.target,target);
 assert.notEqual((await authorize({networkId:'room',target})).operationId,proof.operationId);
 await assert.rejects(authorize({networkId:'other-room',target}),/Invalid/);
 await assert.rejects(authorize({networkId:'room',target:'room'}),/Invalid/);
 assert.equal(await policy.verify({...proof,target:'sha256:'+'b'.repeat(64)}),null);
});
