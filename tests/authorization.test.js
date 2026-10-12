import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity, createAdminAuthorization} from '../packages/phantom-core/src/index.js';

test('configured admin authorizes scoped operations; tampering and impostors fail', async () => {
 const admin = await createAdminIdentity();
 const impostor = await createAdminIdentity();
 const policy = createAdminAuthorization({networkId:'release-network-1',adminPublicKey:admin.publicKey});
 const request = {networkId:'release-network-1',action:'content.revoke',target:'sha256:release-A',operationId:'operation-1'};
 const signed = await admin.sign(request);
 assert.equal((await policy.verify(signed)).target, request.target);
 for (const change of [{networkId:'other'}, {action:'network.retire'}, {target:'release-B'}, {operationId:'operation-2'}, {publicKey:impostor.publicKey}]) assert.equal(await policy.verify({...signed,...change}),null);
 assert.equal(await policy.verify(await impostor.sign(request)),null);
 assert.equal(await policy.verify({...signed,signature:'00'.repeat(64)}),null);
 assert.equal(await policy.verify({...signed,signature:'invalid'}),null);
 assert.equal(await policy.verify(null),null);
 assert.equal((await policy.verify({...signed,role:'syscon1',permissions:['everything']})).permissions,undefined);
});
test('retirement proof targets its network and does not depend on leader role', async () => {
 const admin = await createAdminIdentity();
 const policy = createAdminAuthorization({networkId:'network-instance-A',adminPublicKey:admin.publicKey});
 const record = await admin.sign({networkId:policy.networkId,action:'network.retire',target:policy.networkId,operationId:'retirement-1'});
 assert.equal((await policy.verify(record)).action,'network.retire');
 await assert.rejects(admin.sign({...record,target:'another-network'}));
 assert.throws(()=>createAdminAuthorization({networkId:'A',adminPublicKey:'short-hkey'}));
});
