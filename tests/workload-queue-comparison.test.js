import test from 'node:test';
import assert from 'node:assert/strict';
import {compareWorkloadQueues,workloadQueuePolicy} from '../packages/phantom-core/src/workload-queue-comparison.js';
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

test('peer budget divides both boundaries across the actual lane count',()=>{
 for(const count of [1,2,3])for(const queueTrial of ['tight-1','baseline-1']){
  const lanes=Array.from({length:count},(_,i)=>i),policy=workloadQueuePolicy({lanes,queueTrial,queueBudgetScope:'peer'}),budget=queueTrial==='tight-1'?1048576:16777216;
  assert.ok(policy.highWaterMark*count<=budget);assert.ok(policy.highWaterMark>=262144);assert.ok(policy.lowWaterMark<policy.highWaterMark);
  const original=workloadQueuePolicy({lanes,queueTrial});assert.equal(original.highWaterMark,budget);
 }
 assert.equal(workloadQueuePolicy({lanes:[0],queueTrial:'tight-1',queueBudgetScope:'unknown'}),null);
});
test('queue comparison requires the same budget scope and a contrast at its actual boundary',()=>{
 const rows=trials();for(const r of rows){r.profile.queueBudgetScope='peer';r.profile.lanes=[0,1,2];r.profile.bytes=3*8388608;r.profile.queuePolicy=workloadQueuePolicy(r.profile);r.sourceSubmission=[0,1,2].map(lane=>({lane,peakBufferedBytes:r.profile.queueTrial.startsWith('tight')?262144:524288,finalQueue:{bufferedBytes:0}}));r.receiver.lanes=[0,1,2].map(lane=>({lane,bytes:8388608,verified:true,maxGapMs:10}));r.receiver.queues=[0,1,2].map(lane=>({lane,bufferedBytes:0}));}
 assert.equal(compareWorkloadQueues(rows).selected,'tight');assert.equal(compareWorkloadQueues(rows).queueBudgetScope,'peer');
 rows[1].profile.queueBudgetScope='association';assert.equal(compareWorkloadQueues(rows).status,'incomplete');
});
