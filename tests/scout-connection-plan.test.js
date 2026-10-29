import test from 'node:test';
import assert from 'node:assert/strict';
import {buildScoutConnectionPlan} from '../packages/phantom-core/src/scout-connection-plan.js';
function fixture(){
 const samples=[0,0,1,1,2,2].map((lane,round)=>({round,stage:'confirm',lanes:Array.from({length:lane+1},(_,i)=>i),bytes:3145728}));
 const queueSamples=[16777216,1048576,1048576,16777216].map((queueHighBytes,i)=>({round:21+i,stage:'queue',lanes:[0],bytes:9437184,queueHighBytes}));
 const observations=[...samples,...queueSamples].map(s=>{const elapsedMs=s.stage==='queue'?(s.queueHighBytes===1048576?200:400):s.lanes.length*100;return {round:s.round,stage:s.stage,bytes:s.bytes,elapsedMs,provenance:'peer_reported',clock:'receiver_monotonic',lanes:s.lanes.map(lane=>({lane,bytes:s.bytes/s.lanes.length,maxGapMs:elapsedMs/10}))};});
 return {status:'measured',reason:'sender_confirmed',lanes:[0],connections:1,samples,queueSamples,queuePolicy:{status:'measured',highWaterMark:1048576,lowWaterMark:524288},receiverEvidence:{receivedRounds:10,expectedRounds:10,observations}};
}
test('a connection plan requires repeated receiver rate and stall support for measured sender winners',()=>{
 const plan=buildScoutConnectionPlan(fixture());assert.deepEqual(plan.lanes,[0]);assert.equal(plan.laneDecision.status,'measured');assert.equal(plan.queuePolicy.status,'measured');assert.equal(plan.queuePolicy.highWaterMark,1048576);assert.equal(plan.nativeReceiveState.status,'unknown');assert.equal(plan.laneDecision.evidence.length,3);
});
test('receiver rate or stall regressions veto sender-only lane winners and invalidate mismatched queue profiles',()=>{
 for(const field of ['elapsedMs','maxGapMs']){
  const result=fixture();for(const r of result.receiverEvidence.observations.filter(r=>r.stage==='confirm'&&r.lanes.length===1)){if(field==='elapsedMs')r.elapsedMs=400;else r.lanes[0].maxGapMs=80;}
  const plan=buildScoutConnectionPlan(result);assert.deepEqual(plan.lanes,[0,1,2]);assert.match(plan.laneDecision.reason,/receiver_(rate|stalls)_does?_?not_confirm_lane_winner|receiver_stalls_do_not_confirm_lane_winner/);assert.equal(plan.queuePolicy.reason,'queue_profile_does_not_match_selected_lanes');
 }
});
test('overlapping repetitions and missing or incomparable receiver evidence retain explicit usable defaults',()=>{
 for(const mutate of [r=>delete r.receiverEvidence,r=>r.receiverEvidence.observations.shift(),r=>r.receiverEvidence.observations[0].bytes++,r=>r.receiverEvidence.observations[0].lanes[0].maxGapMs=Infinity,r=>r.receiverEvidence.observations[0].elapsedMs=250]){
  const r=fixture();mutate(r);const plan=buildScoutConnectionPlan(r);assert.equal(plan.laneDecision.status,'fallback');assert.deepEqual(plan.lanes,[0,1,2]);assert.equal(plan.queuePolicy.highWaterMark,16777216);
 }
});
test('a smaller queue cannot qualify on speed while worsening receiver gaps, or on gaps without repeatable speed',()=>{
 for(const field of ['elapsedMs','maxGapMs']){
  const r=fixture();for(const o of r.receiverEvidence.observations.filter(o=>o.round===22||o.round===23)){if(field==='elapsedMs')o.elapsedMs=390;else o.lanes[0].maxGapMs=50;}
  const plan=buildScoutConnectionPlan(r);assert.equal(plan.laneDecision.status,'measured');assert.equal(plan.queuePolicy.status,'fallback');assert.equal(plan.queuePolicy.highWaterMark,16777216);assert.match(plan.queuePolicy.reason,/receiver_(rate_does|stalls_do)_not_confirm_smaller_queue/);
 }
});
test('partial scouting cannot be promoted by attractive receiver samples or a device name',()=>{
 const r=fixture();r.status='inconclusive';r.reason='screen_ranges_overlap_irrecoverable';r.environment={userAgent:'Android'};r.queuePolicy={status:'inconclusive',reason:'smaller_queue_not_confirmed'};
 const before=structuredClone(r),plan=buildScoutConnectionPlan(r);assert.equal(plan.laneDecision.reason,r.reason);assert.equal(plan.laneDecision.status,'fallback');assert.deepEqual(r,before);assert.throws(()=>buildScoutConnectionPlan(r,{fallback:0}),TypeError);
});
function overlapping(){
 const sets=[[0],[0,1],[0,1,2]],samples=[];let round=1;
 for(const lanes of [[0],[1],[2],[0,1],[0,2],[1,2],[0,1,2]])for(let i=0;i<2;i++)samples.push({round:round++,stage:'screen',lanes,connections:lanes.length,bytes:786432,elapsedMs:100+i});
 for(const lanes of sets)for(let i=0;i<2;i++){const elapsedMs=i?110:100;samples.push({round:round++,stage:'confirm',lanes,connections:lanes.length,bytes:3145728,elapsedMs,delivery:[{bytes:0,elapsedMs:0},{bytes:1572864,elapsedMs:elapsedMs/2},{bytes:3145728,elapsedMs}]});}
 const observations=samples.map(s=>({round:s.round,stage:s.stage,bytes:s.bytes,elapsedMs:s.elapsedMs,provenance:'peer_reported',clock:'receiver_monotonic',lanes:s.lanes.map(lane=>({lane,bytes:s.bytes/s.lanes.length,maxGapMs:lane+10}))}));
 return {status:'inconclusive',reason:'confirmation_ranges_overlap',samples,receiverEvidence:{observations,receivedRounds:20,expectedRounds:20}};
}
test('overlapping full confirmations can choose fewer connections as a bounded tradeoff, never a speed winner',()=>{
 const r=overlapping(),before=structuredClone(r),plan=buildScoutConnectionPlan(r);assert.deepEqual(plan.lanes,[0]);assert.equal(plan.laneDecision.status,'bounded_tradeoff');assert.equal(plan.laneDecision.tradeoff.maxRateReduction,0.1);assert.equal(plan.laneDecision.tradeoff.candidates.length,2);assert.equal(plan.laneDecision.evidence.length,3);assert.deepEqual(r,before);
 assert.equal(buildScoutConnectionPlan(r,{maxRateReduction:0}).laneDecision.status,'fallback');
});
test('tradeoff refuses sender, sender-tail, receiver-rate or receiver-stall regressions independently',()=>{
 for(const field of ['sender','tail','receiver','stall']){
  const r=overlapping();for(const s of r.samples.filter(s=>s.stage==='confirm'&&s.lanes.length<3)){
   if(field==='sender'){s.elapsedMs=200;s.delivery.at(-1).elapsedMs=200;}
   if(field==='tail')s.delivery[1].elapsedMs=1;
   const o=r.receiverEvidence.observations.find(o=>o.round===s.round);if(field==='receiver')o.elapsedMs=200;if(field==='stall')o.lanes[0].maxGapMs=50;
  }
  assert.equal(buildScoutConnectionPlan(r).laneDecision.status,'fallback',field);
 }
});
test('partial or invalid confirmation evidence never becomes a resource tradeoff',()=>{
 for(const mutate of [r=>r.samples.pop(),r=>r.receiverEvidence.observations.pop(),r=>r.samples[14].delivery[1].elapsedMs=Infinity,r=>r.samples[14].delivery.reverse(),r=>r.samples[14].bytes++,r=>r.samples[14].elapsedMs=NaN]){const r=overlapping();mutate(r);assert.equal(buildScoutConnectionPlan(r).laneDecision.status,'fallback');}
 assert.throws(()=>buildScoutConnectionPlan(overlapping(),{maxRateReduction:.2}),TypeError);
});

test('verified native readbacks remain local preparation evidence, never invented remote or closing drop counts',()=>{
 const r=fixture();r.localTransportPreparation=[{lane:0,report:{scope:'owned_webview_udp_socket',before:{status:'observed',port:1234,inode:'socket',cookie:'cookie'},applied:{after:{status:'observed',port:1234,inode:'socket',cookie:'cookie',receiveBufferBytes:2097152,receiveDrops:0}}}}];
 const state=buildScoutConnectionPlan(r).nativeReceiveState;assert.equal(state.status,'observed');assert.equal(state.remoteState,'unknown');assert.equal(state.lanes[0].receiveBufferBytes,2097152);assert.equal(state.lanes[0].receiveDropsAtPreparation,0);
 r.localTransportPreparation[0].report.applied.after.inode='other';assert.equal(buildScoutConnectionPlan(r).nativeReceiveState.status,'unknown');
});
