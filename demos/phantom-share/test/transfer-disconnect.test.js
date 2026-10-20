import test from 'node:test';
import assert from 'node:assert/strict';
import {createShareSession} from '../src/session.js';

test('roster loss interrupts an incoming transfer and actual progress clears the warning', () => {
  const handlers = new Map();
  let peers = [{hKey:'b'.repeat(16)}];
  const network = {
    networkId:'a'.repeat(64), getState:()=>({peerId:'c'.repeat(16)}),
    getPeers:()=>peers,
    // A stale ready transport must not hide an empty roster.
    getPeerConnections:()=>[{peerId:'b'.repeat(16),state:'ready'}],
    send(){}, on(type, fn){handlers.set(type,fn);return ()=>handlers.delete(type);},
  };
  const session = createShareSession({network,adminPublicKey:'d'.repeat(64)});
  handlers.get('distributionFileEvent')({fileEvent:'fileOffer',manifest:{id:'file',name:'video',total:100}});
  handlers.get('distributionProgress')({id:'file',received:30,total:100});
  const asset = session.assets.get('file');
  peers = []; handlers.get('state')({});
  assert.equal(asset.status,'interrupted'); assert.match(asset.error,/disconnected/);
  assert.equal(asset.received,30);
  peers = [{hKey:'b'.repeat(16)}]; handlers.get('peerConnection')({state:'ready',peerId:peers[0].hKey});
  handlers.get('distributionProgress')({id:'file',received:31,total:100});
  assert.equal(asset.status,'receiving'); assert.equal(asset.error,undefined);
  handlers.get('distributionComplete')({id:'file',blob:new Blob(['done'])});
  peers = []; handlers.get('state')({});
  assert.equal(asset.status,'received');
  session.close(); assert.equal(handlers.size,0);
});
