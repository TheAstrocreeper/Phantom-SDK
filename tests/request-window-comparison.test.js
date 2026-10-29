import test from 'node:test';
import assert from 'node:assert/strict';
import {requestWindowCandidates,requestWindowOrder,compareRequestWindows} from '../packages/phantom-core/src/request-window-comparison.js';
function trials(){return [1,2,2,1].map((requestWindow,windowStep)=>({status:'complete',profile:{lanes:[0,1],bytes:4194304,chunkSize:262144,chunksPerLane:8,batchSize:32,sourceReadBlockBytes:4194304,messageBytes:null,queuePolicy:{highWaterMark:1048576,lowWaterMark:524288},requestWindow,windowCandidates:[1,2],windowTrial:'measure',windowStep},sourceSubmission:[0,1].map(lane=>({lane,finalQueue:{bufferedBytes:0}})),sourceTransport:[0,1].map(lane=>({lane,change:{counters:{packetsDiscardedOnSend:{status:'observed',value:0}}}})),receiver:{clock:'receiver_monotonic',elapsedMs:requestWindow===1?100:60,requestWindow,laneRequestWindow:requestWindow,lanes:[0,1].map(lane=>({lane,bytes:2097152,verified:true,messages:8,maxGapMs:requestWindow===1?10:10.5})),queues:[0,1].map(lane=>({lane,bufferedBytes:0})),retries:[]}}));}
test('request windows are selectable bounded candidates, not a fixed two-slot policy',()=>{
 assert.deepEqual(requestWindowCandidates([1,4,16],10),[1,4,10]);assert.deepEqual(requestWindowCandidates([1,2,8],1),[1]);assert.deepEqual(requestWindowOrder([1,4]),[1,4,4,1]);
 for(const values of [[],[0,2],[1,33],[1,2,3,4],[1,1.5]])assert.throws(()=>requestWindowCandidates(values,10));
});
test('repeated verified delivery selects a faster clean window within explicit gap bounds',()=>{const picked=compareRequestWindows(trials(),[1,2]);assert.equal(picked.status,'measured');assert.equal(picked.requestWindow,2);});
test('a local send discard disqualifies the otherwise faster candidate',()=>{const r=trials();r[1].sourceTransport[0].change.counters.packetsDiscardedOnSend.value=1;const picked=compareRequestWindows(r,[1,2]);assert.equal(picked.requestWindow,1);assert.equal(picked.reason,'only_repeated_clean_window');});
test('missing counters retain an explicit inconclusive fallback rather than a clean transport claim',()=>{const r=trials();r.forEach(row=>row.sourceTransport=[]);const p=compareRequestWindows(r,[1,2]);assert.equal(p.status,'inconclusive');assert.equal(p.requestWindow,1);});
test('different lane sets, byte counts, queue policies, receiver windows or candidate sets cannot be compared',()=>{
 for(const change of [r=>r.profile.lanes=[0],r=>r.profile.bytes++,r=>r.profile.queuePolicy.highWaterMark*=2,r=>r.receiver.requestWindow=8,r=>r.profile.windowCandidates=[1,4]]){const r=trials();change(r[1]);assert.equal(compareRequestWindows(r,[1,2]).reason,'window_trials_incomparable_or_unverified');}
});
test('queued bytes, retries and reset counters cannot qualify a faster candidate',()=>{
 for(const change of [r=>r.sourceSubmission[0].finalQueue.bufferedBytes=1,r=>r.receiver.queues[0].bufferedBytes=1,r=>r.receiver.retries.push({}),r=>r.sourceTransport[0].change.counters.packetsDiscardedOnSend.status='reset']){const r=trials();change(r[1]);assert.equal(compareRequestWindows(r,[1,2]).requestWindow,1);}
});
test('large receive-gap regression prevents a throughput-only win',()=>{const r=trials();r[1].receiver.lanes[0].maxGapMs=30;r[2].receiver.lanes[0].maxGapMs=30;assert.equal(compareRequestWindows(r,[1,2]).requestWindow,1);});
test('partial repetitions and nonascending candidates cannot establish a winner',()=>{assert.equal(compareRequestWindows(trials().slice(0,3),[1,2]).status,'inconclusive');assert.throws(()=>compareRequestWindows(trials(),[2,1]));});
