import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {encodeInvite} from '../packages/phantom-core/src/invite.js';

const room='a'.repeat(64);
const invite=encodeInvite({networkId:room,doorways:[{type:'custom'}]});
function fixture() {
  let authorizations=0,cores=0;
  const sdk=createBrowserPhantom({admission:{
    authorize:({networkId,peerPublicKey})=>{authorizations++;return {networkId,peerPublicKey,expiresAt:Date.now()+60000};},
    verify:()=>true,
  },coreFactory:()=>{cores++;throw Error('must not construct core');}});
  return {sdk,counts:()=>[authorizations,cores]};
}

test('unsupported invited doorway is rejected before issuing a membership grant',async()=>{
  const f=fixture();
  await assert.rejects(f.sdk.join(invite),{code:'DOORWAY_ADAPTER_MISSING'});
  assert.deepEqual(f.counts(),[0,0]);
});

test('invalid doorway filter is rejected before issuing a membership grant',async()=>{
  const f=fixture();
  const known=encodeInvite({networkId:room,doorways:[{type:'nostr',relays:['wss://relay.example']}]});
  await assert.rejects(f.sdk.join(known,{doorwayTypes:'nostr'}),/doorwayTypes must be an array/);
  assert.deepEqual(f.counts(),[0,0]);
});

test('empty invited doorway selection is rejected before issuing a membership grant',async()=>{
  const f=fixture();
  const known=encodeInvite({networkId:room,doorways:[{type:'nostr',relays:['wss://relay.example']}]});
  await assert.rejects(f.sdk.join(known,{doorwayTypes:['websocket']}),/No invited doorway matches/);
  assert.deepEqual(f.counts(),[0,0]);
});
