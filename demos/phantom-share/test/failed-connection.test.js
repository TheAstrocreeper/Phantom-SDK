import test from 'node:test';
import assert from 'node:assert/strict';
import {disposeFailedAttempt} from '../src/failed-connection.js';

test('cleanup tolerates owner/observer failures and always closes transports after membership release',async()=>{
  const events=[];let finish;
  const done=disposeFailedAttempt({code:'PAIR',
    nativeSession:{close(){events.push('native');throw Error('native close failed');}},
    control:{releaseMembership(){events.push('release');return new Promise(resolve=>finish=resolve);},close(){events.push('control');}},
    network:{close(){events.push('network');throw Error('network close failed');}},
    onEvent(){throw Error('observer failed');},
  });
  await new Promise(setImmediate);
  assert.deepEqual(events,['native','release']);finish();await done;
  assert.deepEqual(events,['native','release','network','control']);
});

test('unresponsive release is aborted and all local owners close before cleanup completes',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});
  let signal;const events=[];
  const done=disposeFailedAttempt({code:'PAIR',nativeSession:{close(){events.push('native');}},
    control:{releaseMembership(input){signal=input.signal;return new Promise(()=>{});},close(){events.push('control');}},
    network:{close(){events.push('network');}},onEvent:event=>events.push(event.payload.status),
  });
  await new Promise(setImmediate);
  assert.equal(signal.aborted,false);t.mock.timers.tick(2000);await done;
  assert.equal(signal.aborted,true);assert.deepEqual(events,['native','failure','network','control']);
});
