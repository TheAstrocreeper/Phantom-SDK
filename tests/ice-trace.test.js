import test from 'node:test';
import assert from 'node:assert/strict';
import {createIceTrace,describeIceCandidate} from '../packages/phantom-core/src/ice-trace.js';
test('native toJSON and received JSON produce the same exact candidate key',()=>{
 const c={candidate:'candidate:1 1 UDP 123 host.local 2222 typ host ufrag generation',sdpMid:'0',sdpMLineIndex:0,usernameFragment:'generation'};
 const trace=createIceTrace();trace.record('generated',{toJSON:()=>c});trace.record('received',JSON.parse(JSON.stringify(c)));const es=trace.snapshot().events;assert.equal(es[0].key,es[1].key);assert.equal(es[0].candidate.address,'host.local');assert.equal(es[0].candidate.candidateType,'host');
});
test('candidate history is bounded, counts drops and returns detached snapshots',()=>{
 const trace=createIceTrace({limit:2});for(let i=0;i<4;i++)trace.record('received',{candidate:String(i)});const s=trace.snapshot();assert.equal(s.events.length,2);assert.equal(s.dropped,2);s.events[0].stage='changed';assert.equal(trace.snapshot().events[0].stage,'received');
});
test('description handles end-of-candidates and bounds raw input',()=>{
 assert.equal(describeIceCandidate(null).candidate,'');assert.equal(describeIceCandidate({candidate:'x'.repeat(5000)}).candidate.length,2048);
});
