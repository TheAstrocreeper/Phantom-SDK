import test from 'node:test';
import assert from 'node:assert/strict';
import {createCandidateInbox} from '../packages/phantom-core/src/candidate-inbox.js';
test('early ICE is consumed only by its incarnation and attempt',()=>{
 const q=createCandidateInbox();q.put('p','boot','old',{candidate:'old'});
 q.put('p','boot','new',{candidate:'new'});q.put('p','stale','new',{candidate:'stale'});
 assert.deepEqual(q.take('p','boot','new'),[{candidate:'new'}]);
 assert.equal(q.snapshot().earlyIceCandidates,0);
 assert.deepEqual(q.take('p','boot','new'),[]);
});
test('candidate flood stays bounded without allocating timers',()=>{
 const q=createCandidateInbox({maxPeers:2,perPeer:4,total:6});let retained=0;
 for(let i=0;i<2000;i++)retained+=Number(q.put('p'+(i%3),'boot','attempt',{candidate:String(i)}));
 assert.equal(retained,6);assert.equal(q.snapshot().earlyIcePeers,2);
 q.clear();assert.equal(q.snapshot().earlyIceCandidates,0);
});

test('candidate inbox bounds payload size and retains only RTC fields',()=>{
 const q=createCandidateInbox();assert.equal(q.put('p','boot','a',{candidate:'x'.repeat(4097)}),false);
 assert.equal(q.put('p','boot','a',{candidate:'ok',untrustedPayload:'x'.repeat(10000)}),true);
 assert.deepEqual(q.take('p','boot','a'),[{candidate:'ok'}]);
});
