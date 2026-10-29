import test from 'node:test';
import assert from 'node:assert/strict';
import {compareWorkloadQueues} from '../packages/phantom-core/src/workload-queue-comparison.js';
function trials(){return ['tight-1','baseline-1','baseline-2','tight-2'].map((queueTrial,i)=>{const tight=queueTrial.startsWith('tight');return {trial:i+1,status:'complete',profile:{queueTrial,lanes:[0],bytes:8388608,chunkSize:262144,batchSize:32,chunksPerLane:32,sourceReadBlockBytes:0,messageBytes:null,queuePolicy:{highWaterMark:tight?1048576:16777216,lowWaterMark:tight?524288:4194304}},sourceSubmission:[{lane:0,peakBufferedBytes:tight?786432:2097152,finalQueue:{bufferedBytes:0}}],receiver:{clock:'receiver_monotonic',elapsedMs:tight?1000:1050,lanes:[{lane:0,bytes:8388608,verified:true,maxGapMs:tight?10:20}],queues:[{lane:0,bufferedBytes:0}]}};});}
test('a repeated smaller queue within the observed rate budget and without worse gaps can be selected',()=>{
 const r=compareWorkloadQueues(trials());assert.equal(r.status,'bounded_tradeoff');assert.equal(r.selected,'tight');assert.equal(r.tight.trials.length,2);
});
test('rate or stall regressions retain baseline instead of manufacturing a smaller-queue winner',()=>{
 for(const change of [r=>r.receiver.elapsedMs=1600,r=>r.receiver.lanes[0].maxGapMs=30]){const rows=trials();change(rows[0]);assert.equal(compareWorkloadQueues(rows).selected,'baseline');}
});
test('missing, queued, failed, reordered or different workloads cannot be compared',()=>{
 for(const change of [r=>r.pop(),r=>r[0].sourceSubmission[0].finalQueue.bufferedBytes=1,r=>r[1].status='failed',r=>r.reverse(),r=>r[2].profile.bytes++,r=>r[1].receiver.lanes[0].verified=false]){const rows=trials();change(rows);assert.equal(compareWorkloadQueues(rows).status,'incomplete');}
});

test('a workload that never reaches either boundary cannot establish a queue-depth advantage',()=>{const rows=trials();for(const r of rows)r.sourceSubmission[0].peakBufferedBytes=262144;assert.equal(compareWorkloadQueues(rows).reason,'queue_depth_contrast_not_observed');});
