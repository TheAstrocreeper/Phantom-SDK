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
