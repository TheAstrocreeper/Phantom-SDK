import test from 'node:test';
import assert from 'node:assert/strict';
import {createDiagnostics} from '../demos/phantom-breach/src/diagnostics.js';
const codes=['recovering','transport_fault','observe_core_recovery','syscon1','failed','ready','replacement_window_expired','terminal_staged','sys_replacement_terminal_exit_recorded','pc_failed','connecting','open','closed','inside'];
const state={state:'recovering',reason:'transport_fault',nextAction:'observe_core_recovery',role:'syscon1',peerId:'SECRET',admitted:true};
function snapshot(){return {reason:'sys_replacement_terminal_exit_recorded',self:{hKey:'SECRET'},concierge:{sysReady:false,sysPipeState:'failed',sysLastErr:'pc_failed:SECRET',sysAttempt:{id:'SECRET',target:'SECRET',state:'failed',faultReason:'replacement_window_expired',startedAt:Date.now()-1000,recoveryCount:1},membershipAudit:{stage:'terminal_staged',peer:'SECRET',trace:[{detail:'SECRET'}],topology:{committedCount:2,pendingCount:1,terminalKeys:'SECRET',committedEpoch:2}}},chatPairs:[{hKey:'SECRET',handle:'SECRET',relationship:'inside',offerOwner:true,dcState:'closed',pcState:'failed',chatFault:'pc_failed',reconnecting:true}]};}
test('recovery reports expose terminal membership and transport state without identifiers',()=>{
 const d=createDiagnostics({build:'test',codes});d.recovery(snapshot(),state);const r=d.report().latestRecovery;
 assert.equal(r.system.attempt.fault,'replacement_window_expired');assert.equal(r.system.fault,'pc_failed');assert.equal(r.membership.terminalCount,1);assert.equal(r.membership.committedCount,2);assert.equal(r.chat[0].channel,'closed');assert.equal(r.coreEvent,'sys_replacement_terminal_exit_recorded');assert.ok(!d.text().includes('SECRET'));
});
test('unrecognized faults and messages are omitted; recovery history remains bounded',()=>{
 const d=createDiagnostics({build:'test',codes,limit:2});const s=snapshot();s.concierge.sysAttempt.faultReason='SECRET';d.recovery(s,state);assert.equal(d.report().latestRecovery.system.attempt.fault,'unrecognized');
 d.recovery(s,state);assert.equal(d.report().events.length,1,'age alone does not create an event');s.concierge.membershipAudit.topology.committedCount=1;d.recovery(s,state);s.chatPairs=[];d.recovery(s,state);assert.equal(d.report().droppedEvents,1);assert.equal(d.report().events.length,2);assert.ok(!d.text().includes('SECRET'));
 const r=d.report();r.latestRecovery.membership.committedCount=99;assert.equal(d.report().latestRecovery.membership.committedCount,1);
});
