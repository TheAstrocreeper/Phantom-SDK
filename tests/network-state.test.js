import test from 'node:test';import assert from 'node:assert/strict';
import {projectNetworkState} from '../packages/phantom-core/src/network-state.js';
test('ready doorway and accepted publication do not imply admission',()=>{
 const s={self:{hKey:'1234567890abcdef'},bootstrap:{doorwayState:'ready'},concierge:{sysAttempt:{id:'attempt',state:'connecting'},sysPipeState:'connecting'}};
 assert.equal(projectNetworkState(s).state,'connecting');assert.equal(projectNetworkState(s).admitted,false);
});
test('committed member transport loss projects recovery while an uncommitted retry remains recovering',()=>{
 const s={self:{hKey:'1234567890abcdef'},concierge:{sysAttempt:{state:'faulted',faultReason:'sys_transport_lost'},membershipAudit:{topology:{committedKeys:'12345678'}}}};
 assert.equal(projectNetworkState(s).state,'recovering');s.concierge.membershipAudit.topology.committedKeys='';assert.equal(projectNetworkState(s).state,'recovering');
 s.concierge.sysAttempt.faultReason='sys_birth_response_deadline_exhausted';assert.equal(projectNetworkState(s).state,'failed');
});
