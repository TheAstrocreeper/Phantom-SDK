import test from 'node:test';
import assert from 'node:assert/strict';
import {leaveAndReset,closeSessionOwners} from '../src/session-cleanup.js';
test('local reset runs once after successful, rejected, and unresponsive Leave',async()=>{
 for(const mode of ['ok','rejected','unresponsive']){
  const resets=[],errors=[];
  const confirmed=await leaveAndReset({leave:()=>mode==='ok'?Promise.resolve():mode==='rejected'?Promise.reject(Error('offline')):new Promise(()=>{}),reset:v=>resets.push(v),onError:e=>errors.push(e.message),timeoutMs:10});
  assert.equal(confirmed,mode==='ok');assert.deepEqual(resets,[confirmed]);assert.equal(errors.length,mode==='ok'?0:1);
 }
});


test('all owners begin disposal despite errors and closed waits for the final acknowledgement',async()=>{
 const events=[];let finish;
 const closing=closeSessionOwners([
  {close(){events.push('native');return new Promise(r=>finish=r);}},
  {close(){events.push('broken');throw Error('broken');}},
  {close(){events.push('network');}},
 ],e=>events.push(e.message));
 assert.deepEqual(events,['native','broken','network']);
 let done=false;closing.then(()=>done=true);await new Promise(setImmediate);assert.equal(done,false);
 finish();await closing;assert.equal(done,true);assert.deepEqual(events,['native','broken','network','broken']);
});
