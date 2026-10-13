import test from 'node:test';
import assert from 'node:assert/strict';
import {createRtcDiagnostics,iceDetails} from '../packages/phantom-core/src/rtc-diagnostics.js';
import {createDiagnostics} from '../demos/phantom-breach/src/diagnostics.js';
test('trace preserves exact decisions and errors, with bounded retention and fields',()=>{
 const d=createRtcDiagnostics({limit:2,now:()=>1});
 d.record('offer',{attempt:'a',sdp:'SECRET'});d.record('rejected',{incomingAttempt:'b',reason:'sys_rtc_ice_ignored_stale_transport_attempt'});
 d.record('error',{errorName:'OperationError',errorMessage:'Unknown ufrag',arbitraryPacket:'SECRET'});
 const r=d.snapshot();assert.equal(r.dropped,1);assert.equal(r.events[0].incomingAttempt,'b');assert.equal(r.events[1].errorMessage,'Unknown ufrag');assert.ok(!JSON.stringify(r).includes('SECRET'));
});
test('candidate generations compare without retaining SDP credentials',()=>{
 const r=iceDetails({candidate:'candidate:1 1 udp 1 192.0.2.1 42 typ srflx ufrag wrong'},
 {localDescription:{sdp:'a=ice-ufrag:local\r\na=ice-pwd:SECRET'},remoteDescription:{sdp:'a=ice-ufrag:remote\r\na=ice-pwd:SECRET'}});
 assert.equal(r.generationMatches,false);assert.equal(r.candidateType,'srflx');assert.equal(r.remoteUfrag,'remote');assert.ok(r.candidate.includes('192.0.2.1'));assert.ok(!JSON.stringify(r).includes('SECRET'));
});
test('candidate-pair stats retain unanswered checks and candidate types; concurrent reads coalesce',async()=>{
 const d=createRtcDiagnostics();let release,calls=0;
 const pc={getStats:()=>{calls++;return new Promise(r=>release=r);}};
 const first=d.stats(pc,{peer:'p',attempt:'a'});await d.stats(pc);assert.equal(calls,1);
 release(new Map([['l',{id:'l',type:'local-candidate',candidateType:'host',protocol:'udp',address:'192.0.2.1',port:42}],['r',{id:'r',type:'remote-candidate',candidateType:'srflx'}],['pair',{id:'pair',type:'candidate-pair',localCandidateId:'l',remoteCandidateId:'r',state:'in-progress',requestsSent:12,responsesReceived:0,nominated:false}]]));await first;
 const event=d.snapshot().events[0];assert.equal(event.requestsSent,12);assert.equal(event.responsesReceived,0);assert.equal(event.remoteCandidateType,'srflx');assert.equal(event.localAddress,'192.0.2.1');assert.equal(event.localPort,42);
 const report=createDiagnostics({build:'test'});report.recovery({rtcDiagnostics:d.snapshot()},{});assert.equal(report.report().transportTrace.events[0].attempt,'a');
});
