import test from 'node:test';
import assert from 'node:assert/strict';
import {createDiagnostics} from '../demos/phantom-breach/src/diagnostics.js';
import {createStartupRecovery} from '../demos/phantom-breach/src/startup-recovery.js';
test('failed join retains sanitized snapshots without a network handle',()=>{
 const diagnostics=createDiagnostics({build:'test',codes:['connecting','failed','syscon2','inspect_diagnostics','sys_birth_response_deadline_exhausted']});
 const observer=createStartupRecovery(diagnostics);
 observer.state({state:'connecting',role:'syscon2',peerId:'SECRET'});
 observer.snapshot({self:{hKey:'SECRET'},sysPairs:[{pcState:'connecting',iceState:'checking'}]});
 observer.state({state:'failed',reason:'sys_birth_response_deadline_exhausted',nextAction:'inspect_diagnostics'});
 observer.snapshot({reason:'sys_birth_response_deadline_exhausted',self:{hKey:'SECRET'}});
 const report=diagnostics.report();assert.equal(report.latestRecovery.state,'failed');
 assert.equal(report.events[0].context.systemTransports[0].ice,'checking');
 assert.ok(!diagnostics.text().includes('SECRET'));
 observer.reset();observer.snapshot({});assert.equal(diagnostics.report().latestRecovery.state,'none');
});
