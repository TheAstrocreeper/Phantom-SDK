import test from 'node:test';
import assert from 'node:assert/strict';
import {leaveAndReset} from '../src/session-cleanup.js';
test('local reset runs once after successful, rejected, and unresponsive Leave',async()=>{
 for(const mode of ['ok','rejected','unresponsive']){
  const resets=[],errors=[];
  const confirmed=await leaveAndReset({leave:()=>mode==='ok'?Promise.resolve():mode==='rejected'?Promise.reject(Error('offline')):new Promise(()=>{}),reset:v=>resets.push(v),onError:e=>errors.push(e.message),timeoutMs:10});
  assert.equal(confirmed,mode==='ok');assert.deepEqual(resets,[confirmed]);assert.equal(errors.length,mode==='ok'?0:1);
 }
});
