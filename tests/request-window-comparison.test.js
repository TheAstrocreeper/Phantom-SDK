import test from 'node:test';
import assert from 'node:assert/strict';
import {requestWindowCandidates,requestWindowOrder,compareRequestWindows,readRequestWindowDelivery,nextRequestWindowProbe} from '../packages/phantom-core/src/request-window-comparison.js';
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

function adaptive(order,candidates=[1,2]){return order.map((n,i)=>{const r=structuredClone(trials().find(t=>t.profile.requestWindow===(n===1?1:2)));r.profile.requestWindow=n;r.profile.windowCandidates=candidates;r.profile.windowAdaptive=true;r.profile.windowStep=i;r.receiver.requestWindow=n;r.receiver.laneRequestWindow=n;return r;});}
test('first contact surveys bounds then confirms a clear leader without repeating its loser',()=>{
 assert.equal(nextRequestWindowProbe([],[1,2]).requestWindow,1);
 assert.equal(nextRequestWindowProbe(adaptive([1]),[1,2]).requestWindow,2);
 const confirm=nextRequestWindowProbe(adaptive([1,2]),[1,2]);assert.equal(confirm.action,'probe');assert.equal(confirm.requestWindow,2);assert.equal(confirm.reason,'confirm_observed_leader');
 const done=nextRequestWindowProbe(adaptive([1,2,2]),[1,2]);assert.equal(done.action,'select');assert.equal(done.requestWindow,2);assert.equal(done.evidence.find(s=>s.requestWindow===1).count,1);
});
test('the same first-contact logic can confirm the smaller window instead',()=>{
 const r=adaptive([1,2]);for(const t of r){const time=t.profile.requestWindow===1?60:100;t.receiver.elapsedMs=time;t.receiver.deliveryWindows.forEach((w,i)=>w.elapsedMs=(i+1)*time/4);t.receiver.lanes.forEach(l=>l.maxGapMs=10);}
 assert.equal(nextRequestWindowProbe(r,[1,2]).requestWindow,1);
 const confirm=structuredClone(r[0]);confirm.profile.windowStep=2;r.push(confirm);const done=nextRequestWindowProbe(r,[1,2]);assert.equal(done.action,'select');assert.equal(done.requestWindow,1);
});
test('overlap uses remaining repetitions rather than claiming an early winner',()=>{
 const r=adaptive([1,2]);for(const t of r){t.receiver.elapsedMs=100;t.receiver.deliveryWindows.forEach((w,i)=>w.elapsedMs=(i+1)*25);}
 const next=nextRequestWindowProbe(r,[1,2]);assert.equal(next.action,'probe');assert.equal(next.reason,'resolve_overlapping_or_unknown_evidence');
 for(const n of [1,2]){const t=structuredClone(r.find(t=>t.profile.requestWindow===n));t.profile.windowStep=r.length;r.push(t);}
 assert.equal(nextRequestWindowProbe(r,[1,2]).action,'select');
});
test('missing counters cannot establish an early performance winner',()=>{
 const r=adaptive([1,2,2]);r.forEach(t=>t.sourceTransport=[]);assert.equal(nextRequestWindowProbe(r,[1,2]).action,'probe');
 const last=adaptive([1])[0];last.profile.windowStep=3;last.sourceTransport=[];r.push(last);const done=nextRequestWindowProbe(r,[1,2]);assert.equal(done.status,'inconclusive');assert.equal(done.requestWindow,1);
});
test('all configured candidates are surveyed and winner repetition can change the next probe',()=>{
 const r=adaptive([1,2],[1,2,4]);assert.equal(nextRequestWindowProbe(r,[1,2,4]).requestWindow,4);
 const regressed=adaptive([1,2,2]);regressed[2].receiver.elapsedMs=180;regressed[2].receiver.deliveryWindows.forEach((w,i)=>w.elapsedMs=(i+1)*45);const next=nextRequestWindowProbe(regressed,[1,2]);assert.equal(next.action,'probe');assert.equal(next.requestWindow,1);
});
test('adaptive evidence rejects replayed steps, changed lanes, excess repetitions and invalid delivery',()=>{
 for(const change of [r=>r[1].profile.windowStep=0,r=>r[1].profile.lanes=[0],r=>r[1].receiver.deliveryWindows[0].bytes++,r=>r[1].profile.windowAdaptive=false]){const r=adaptive([1,2]);change(r);assert.equal(nextRequestWindowProbe(r,[1,2]).status,'inconclusive');}
 assert.equal(nextRequestWindowProbe(adaptive([1,2,2,2]),[1,2]).reason,'adaptive_window_evidence_invalid');
});
