import test from 'node:test';
import assert from 'node:assert/strict';
import {createDiagnostics} from '../demos/phantom-breach/src/diagnostics.js';
test('diagnostic exports preserve measurements and omit private packet fields',()=>{
 const d=createDiagnostics({build:'test',now:()=>12,wall:()=> '2026-10-12T00:00:00Z'});
 d.record('verifiedProgress',{bytes:65536,elapsedMs:42,inFlight:8,credential:'SECRET',admissionProof:{signature:'SECRET'},name:'private.mp4',url:'https://example/#SECRET',from:'identity',packet:{data:'SECRET'}});
 const report=d.report();assert.deepEqual(report.events[0],{at:'2026-10-12T00:00:00Z',sessionElapsedMs:0,type:'verifiedProgress',bytes:65536,elapsedMs:42,inFlight:8});
 assert.ok(!d.text().includes('SECRET'));assert.ok(!d.text().includes('private.mp4'));report.events[0].bytes=0;assert.equal(d.report().events[0].bytes,65536);
});
test('bounded reports disclose dropped events and preserve multiple completions',()=>{
 const d=createDiagnostics({build:'test',limit:2});d.record('networkState',{state:'admitted'});d.record('complete',{durationMs:26,size:32,result:'verified'});d.record('complete',{durationMs:45,size:96,result:'verified'});
 assert.equal(d.report().droppedEvents,1);assert.equal(d.report().events.length,2);
});
