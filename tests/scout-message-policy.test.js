import test from 'node:test';
import assert from 'node:assert/strict';
import {selectMessagePolicy} from '../packages/phantom-core/src/transfer-scout.js';
import {buildScoutConnectionPlan} from '../packages/phantom-core/src/scout-connection-plan.js';
function samples(){return [262144,16384,16384,262144].map((messageCeilingBytes,i)=>{const bytes=9437184,elapsedMs=messageCeilingBytes===16384?1000+i:2000+i;return {round:21+i,stage:'message',connections:3,lanes:[0,1,2],bytes,elapsedMs,queueHighBytes:16777216,queueLowBytes:4194304,messageCeilingBytes,messageBytes:Array(3).fill(messageCeilingBytes),delivery:[{bytes:0,elapsedMs:0},{bytes:bytes/2,elapsedMs:elapsedMs/2},{bytes,elapsedMs}]};});}
function result(){const messageSamples=samples();return {status:'inconclusive',reason:'screen_ranges_overlap_irrecoverable',comparison:'message',samples:[],queueSamples:[],messageSamples,messagePolicy:selectMessagePolicy(messageSamples),receiverEvidence:{receivedRounds:4,expectedRounds:4,observations:messageSamples.map(s=>({round:s.round,stage:s.stage,bytes:s.bytes,elapsedMs:s.elapsedMs,clock:'receiver_monotonic',provenance:'peer_reported',lanes:s.lanes.map(lane=>({lane,bytes:s.bytes/3,maxGapMs:s.elapsedMs/10}))}))}};}
test('message policy requires fixed queues, actual size contrast and repeated sender total and late improvement',()=>{
 assert.equal(selectMessagePolicy(samples()).maxPayloadBytes,16384);
 for(const mutate of [s=>s.pop(),s=>s[0].queueHighBytes=1048576,s=>s[0].messageBytes[0]=16384,s=>s[2].elapsedMs=3000,s=>s[1].messageCeilingBytes=32768]){const rows=samples();mutate(rows);assert.equal(selectMessagePolicy(rows).status,'inconclusive');}
 const rows=samples();for(const s of rows)s.messageBytes=[16384,16384,16384];assert.equal(selectMessagePolicy(rows).reason,'negotiated_limit_prevented_message_size_contrast');
});
test('confirmed message size can apply to the tested fallback lane set without pretending lane count is optimized',()=>{
 const r=result(),plan=buildScoutConnectionPlan(r);assert.equal(plan.laneDecision.status,'fallback');assert.deepEqual(plan.lanes,[0,1,2]);assert.equal(plan.messagePolicy.status,'measured');assert.equal(plan.messagePolicy.maxPayloadBytes,16384);assert.equal(plan.queuePolicy.highWaterMark,16777216);assert.equal(plan.queuePolicy.reason,'queue_held_fixed_for_message_comparison');assert.equal(plan.testedProfiles.length,2);
});
test('receiver message rate or stalls can veto the sender result, and missing reports preserve negotiated sizing',()=>{
 for(const mutate of [r=>r.receiverEvidence.observations.pop(),r=>r.receiverEvidence.observations[1].elapsedMs=3000,r=>r.receiverEvidence.observations[1].lanes[0].maxGapMs=500]){const r=result();mutate(r);const plan=buildScoutConnectionPlan(r);assert.equal(plan.messagePolicy.status,'fallback');assert.equal(plan.messagePolicy.maxPayloadBytes,null);}
});
