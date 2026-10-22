import test from 'node:test';
import assert from 'node:assert/strict';
import {projectNetworkState} from '../packages/phantom-core/src/network-state.js';

test('native terminal recovery is failed, while an in-progress fault remains recoverable', () => {
  const snapshot = {self:{hKey:'a'.repeat(16)},concierge:{role:'peer',sysReady:false,
    membershipAudit:{topology:{committedKeys:'aaaaaaaa'}},
    sysAttempt:{id:'attempt-1',state:'terminal',faultReason:'replacement_window_expired'}}};
  const state = projectNetworkState(snapshot);
  assert.equal(state.state,'failed');assert.equal(state.nextAction,'create_or_join');
  assert.equal(state.attempt.id,'attempt-1');assert.equal(state.reason,'replacement_window_expired');
  snapshot.concierge.sysAttempt.state='recovering';
  assert.equal(projectNetworkState(snapshot).state,'recovering');
  snapshot.concierge.sysReady=true;
  assert.equal(projectNetworkState(snapshot).state,'admitted');
});
