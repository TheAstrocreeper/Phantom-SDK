import test from 'node:test';
import assert from 'node:assert/strict';
import {decideFileWorkload} from '../packages/phantom-core/src/file-workload-decision.js';
function failed(){return {version:2,status:'failed',profile:{lanes:[0,1,2],bytes:3*8388608},receiver:{clock:'receiver_monotonic',lanes:[{lane:0,bytes:8388608,verified:true},{lane:1,bytes:3407872,verified:false},{lane:2,bytes:8388608,verified:true}],queues:[{lane:0,bufferedBytes:0},{lane:1,bufferedBytes:0},{lane:2,bufferedBytes:0}]},sourceSubmission:[{lane:0,finalQueue:{bufferedBytes:0}},{lane:1,finalQueue:{bufferedBytes:5458620}},{lane:2,finalQueue:{bufferedBytes:0}}]};}
test('a failed middle lane nominates the exact verified survivors, without claiming they are fastest',()=>{
 assert.deepEqual(decideFileWorkload(failed()),{status:'retry_subset',reason:'verify_completed_subset_without_failed_associations',lanes:[0,2],rejected:[1]});
});
test('arrival counts alone, a queued copy, and missing ownership cannot certify a survivor',()=>{
 const unverified=failed();unverified.receiver.lanes[0].verified=false;assert.deepEqual(decideFileWorkload(unverified).lanes,[2]);
 const queued=failed();queued.sourceSubmission[0].finalQueue.bufferedBytes=1;assert.deepEqual(decideFileWorkload(queued).lanes,[2]);
 const receiverQueued=failed();receiverQueued.receiver.queues[2].bufferedBytes=1;assert.deepEqual(decideFileWorkload(receiverQueued).lanes,[0]);
 const missing=failed();delete missing.receiver.queues;assert.equal(decideFileWorkload(missing).status,'unavailable');
});
test('legacy or contradictory evidence stays unknown instead of reconstructing verification',()=>{
 const legacy=failed();legacy.version=1;assert.equal(decideFileWorkload(legacy).status,'unavailable');
 const wrongOrder=failed();wrongOrder.receiver.lanes.reverse();assert.equal(decideFileWorkload(wrongOrder).status,'unavailable');
 const overflow=failed();overflow.receiver.lanes[1].bytes=8388609;assert.equal(decideFileWorkload(overflow).status,'unavailable');
});
