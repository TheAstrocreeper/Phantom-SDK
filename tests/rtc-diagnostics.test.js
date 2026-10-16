import test from 'node:test';
import assert from 'node:assert/strict';
import {createRtcDiagnostics} from '../packages/phantom-core/src/rtc-diagnostics.js';
const tick=()=>new Promise(setImmediate);
function fixture(getStats){return {connectionState:'connecting',iceConnectionState:'checking',iceGatheringState:'complete',signalingState:'stable',sctp:{state:'connecting',transport:{state:'connecting'}},getStats};}
const report=()=>new Map([['pair',{id:'pair',type:'candidate-pair',state:'in-progress',localCandidateId:'local',remoteCandidateId:'remote',requestsSent:4,responsesReceived:0,privateUnlisted:'excluded'}],['local',{id:'local',type:'local-candidate',address:'192.0.2.1',port:1234,candidateType:'host'}],['transport',{id:'transport',type:'transport',dtlsState:'connecting',selectedCandidatePairId:'pair'}]]);
test('retirement invokes getStats before close and preserves pre-close state after resolution',async()=>{
 let resolve;const calls=[],events=[];const pc=fixture(()=>{calls.push(pc.connectionState);return new Promise(r=>resolve=r);});
 const d=createRtcDiagnostics({emit:e=>events.push(e)});d.track(pc,{peer:'peer',pipe:'sys'});d.retire(pc,'deadline');pc.connectionState='closed';d.capture(pc,'late_closed_callback');resolve(report());await tick();
 const r=d.snapshot().connections[0];assert.deepEqual(calls,['connecting']);assert.equal(r.retired,true);assert.equal(r.state.connectionState,'connecting');assert.equal(r.retirementReason,'deadline');assert.equal(r.stats.rows[0].responsesReceived,0);assert.equal(r.stats.rows[0].privateUnlisted,undefined);assert(events.some(e=>e.stats));
});
test('one terminal capture can run beside an in-flight capture; older results cannot overwrite it',async()=>{
 const requests=[];const pc=fixture(()=>new Promise(resolve=>requests.push(resolve)));const d=createRtcDiagnostics();d.track(pc,{peer:'peer',pipe:'sys'});d.capture(pc,'checking');d.capture(pc,'other');d.retire(pc,'failure');assert.equal(requests.length,2);
 requests[1](report());await tick();requests[0](new Map());await tick();assert.equal(d.snapshot().connections[0].stats.requestedReason,'before_close:failure');assert.equal(d.snapshot().connections[0].stats.rows.length,3);assert.equal(requests.length,2);
});
test('connection history, transitions and stats remain bounded',async()=>{
 const d=createRtcDiagnostics({maxConnections:2,maxEvents:2,maxStats:1});for(let i=0;i<3;i++){const pc=fixture(async()=>report());d.track(pc,{peer:String(i),pipe:'sys'});for(let j=0;j<4;j++)d.capture(pc,'event');d.retire(pc,'close');}await tick();await tick();const s=d.snapshot();assert.equal(s.connections.length,2);assert.equal(s.droppedConnections,1);for(const r of s.connections){assert.equal(r.events.length,2);assert(r.droppedEvents>0);assert.equal(r.stats.rows.length,1);assert.equal(r.stats.dropped,2);}
});
test('getStats rejection is retained as evidence',async()=>{
 const d=createRtcDiagnostics();const pc=fixture(async()=>{throw Error('stats unavailable');});d.track(pc,{peer:'peer',pipe:'sys'});d.retire(pc,'close');await tick();assert.equal(d.snapshot().connections[0].statsError,'stats unavailable');
});

test('native SDP ICE usernames are parsed across real CRLF lines',()=>{
 const pc=fixture(async()=>report());
 pc.localDescription={type:'offer',sdp:'v=0\r\na=ice-ufrag:runLocal\r\na=ice-pwd:secret\r\na=ice-ufrag:runLocal\r\n'};
 pc.remoteDescription={type:'answer',sdp:'v=0\r\na=ice-ufrag:remoteN\r\na=ice-pwd:other\r\n'};
 const d=createRtcDiagnostics();d.track(pc,{peer:'peer',pipe:'sys'});
 const state=d.snapshot().connections[0].state;
 assert.deepEqual(state.localIceUfrags,['runLocal']);assert.deepEqual(state.remoteIceUfrags,['remoteN']);
});
