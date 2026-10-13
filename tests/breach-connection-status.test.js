import test from 'node:test';
import assert from 'node:assert/strict';
import {connectionNotice} from '../demos/phantom-breach/src/connection-status.js';
test('initial waiting does not replace invitation or startup instructions',()=>{
  assert.equal(connectionNotice({hadPeer:false,channels:0,state:'discovering'}),null);
});
test('lost channel shows recovery even when room membership remains admitted',()=>{
  const notice=connectionNotice({hadPeer:true,channels:0,state:'admitted'});
  assert.equal(notice.active,true);
  assert.match(notice.text,/keep both pages open/);
});
test('restored channel releases the notice to reveal transfer activity',()=>{
  assert.equal(connectionNotice({hadPeer:true,channels:1,state:'recovering'}),null);
});
test('terminal room states do not claim active reconnection',()=>{
  for(const state of ['failed','closed','retired'])assert.equal(connectionNotice({hadPeer:true,channels:0,state}).active,false);
});
