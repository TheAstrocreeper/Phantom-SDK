import test from 'node:test';
import assert from 'node:assert/strict';
import {makeReceiverEvidence,readReceiverEvidence} from '../packages/phantom-core/src/scout-receiver-evidence.js';
const spec={stage:'screen',bytes:4096,perLane:4096,lanes:[1]};
const sample={...spec,round:3,elapsedMs:200,lanes:[{lane:1,bytes:4096,messages:2,firstAtMs:10,lastAtMs:180,completeAtMs:180,maxGapMs:170,largestGaps:[{fromMs:10,toMs:180,gapMs:170,afterBytes:2048}]}]};
test('returned evidence strips extra fields and marks older transport observations as latest available',()=>{
 const observations=[];observations[1]={lane:1,round:2,phase:'preflight-receive-stats',startedAtMs:1,endedAtMs:2,current:{transportId:'t',pairId:'p',timestampMs:100,packetsSent:1,packetsReceived:2,bytesSent:20,bytesReceived:40,packetsDiscardedOnSend:null,bytesDiscardedOnSend:null},report:{environment:{userAgent:'private'},stats:{address:'private'}}};
 const wire=makeReceiverEvidence(sample,observations);wire.lanes[0].transport.current.address='private';wire.environment='private';
 const read=readReceiverEvidence(wire,spec);assert.equal(read.transportCoverage,'latest_available_not_round_aligned');
 assert.equal(read.lanes[0].transport.round,2);assert.equal(read.lanes[0].transport.current.packetsDiscardedOnSend,null);
 assert.doesNotMatch(JSON.stringify(read),/private/);assert.equal(read.clock,'receiver_monotonic');
});
test('malformed sizes, lanes, counters, gaps and sample time cannot enter receiver evidence',()=>{
 const wire=makeReceiverEvidence(sample,[]);assert.ok(readReceiverEvidence(wire,spec));
 for(const mutate of [v=>v.bytes++,v=>v.lanes.push(v.lanes[0]),v=>v.lanes[0].lane=2,v=>v.lanes[0].messages=Infinity,v=>v.lanes[0].completeAtMs=201,v=>v.lanes[0].largestGaps=Array(5).fill(v.lanes[0].largestGaps[0]),v=>v.lanes[0].largestGaps[0].gapMs=0,v=>v.lanes[0].transport={round:999,phase:'preflight-receive-stats'}]){
  const value=structuredClone(wire);mutate(value);assert.equal(readReceiverEvidence(value,spec),null);
 }
});
