import test from 'node:test';
import assert from 'node:assert/strict';
import {requestWindowCandidates,requestWindowOrder,compareRequestWindows,readRequestWindowDelivery} from '../packages/phantom-core/src/request-window-comparison.js';
function trials(){return [1,2,2,1].map((requestWindow,windowStep)=>({status:'complete',profile:{deliveryWindowCount:4,lanes:[0,1],bytes:4194304,chunkSize:262144,chunksPerLane:8,batchSize:32,sourceReadBlockBytes:4194304,messageBytes:null,queuePolicy:{highWaterMark:1048576,lowWaterMark:524288},requestWindow,windowCandidates:[1,2],windowTrial:'measure',windowStep},sourceSubmission:[0,1].map(lane=>({lane,finalQueue:{bufferedBytes:0}})),sourceTransport:[0,1].map(lane=>({lane,change:{counters:{packetsDiscardedOnSend:{status:'observed',value:0}}}})),receiver:{clock:'receiver_monotonic',deliveryWindows:[1,2,3,4].map(n=>({messages:n*4,bytes:n*1048576,elapsedMs:n*(requestWindow===1?100:60)/4})),elapsedMs:requestWindow===1?100:60,requestWindow,laneRequestWindow:requestWindow,lanes:[0,1].map(lane=>({lane,bytes:2097152,verified:true,messages:8,maxGapMs:requestWindow===1?10:10.5})),queues:[0,1].map(lane=>({lane,bufferedBytes:0})),retries:[]}}));}
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

test('a faster whole-trial burst loses to repeatable later-window delivery',()=>{
 const r=trials();for(const t of r){const times=t.profile.requestWindow===1?[40,60,80,100]:[3,32,61,90];t.receiver.elapsedMs=times.at(-1);t.receiver.deliveryWindows.forEach((w,i)=>w.elapsedMs=times[i]);for(const l of t.receiver.lanes)l.maxGapMs=10;}
 const picked=compareRequestWindows(r,[1,2]);assert.equal(picked.requestWindow,1);assert.equal(picked.status,'measured');assert.ok(picked.evidence[1].totalRateMin>picked.evidence[0].totalRateMax);assert.ok(picked.evidence[0].rateMin>picked.evidence[1].rateMax);
});
test('missing, partial, forged and reordered delivery boundaries cannot select a fast window',()=>{
 for(const change of [r=>delete r.receiver.deliveryWindows,r=>r.receiver.deliveryWindows.pop(),r=>r.receiver.deliveryWindows[1].bytes++,r=>r.receiver.deliveryWindows[1].messages++,r=>r.receiver.deliveryWindows[2].elapsedMs=0,r=>r.receiver.deliveryWindows[3].elapsedMs=r.receiver.elapsedMs+1]){const r=trials();change(r[1]);assert.equal(compareRequestWindows(r,[1,2]).reason,'window_delivery_evidence_invalid');}
});
test('zero-duration and fewer than four windows stay unknown rather than claiming sustained speed',()=>{
 const r=trials();for(const t of r){t.receiver.deliveryWindows.forEach(w=>w.elapsedMs=0);}assert.equal(compareRequestWindows(r,[1,2]).status,'inconclusive');
 const t=trials()[0];t.profile.chunksPerLane=2;t.profile.deliveryWindowCount=2;t.profile.bytes=1048576;t.receiver.deliveryWindows=[{messages:2,bytes:524288,elapsedMs:10},{messages:4,bytes:1048576,elapsedMs:20}];assert.equal(readRequestWindowDelivery(t.profile,t.receiver).lateRateMin,null);
});
