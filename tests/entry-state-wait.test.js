import test from 'node:test';
import assert from 'node:assert/strict';
import {createPhantomEntry} from '../packages/phantom-core/src/entry.js';
import {createInvite} from '../packages/phantom-core/src/invite.js';
const tick=()=>new Promise(setImmediate);
function fixture(rtcDiagnostics){
 let state={state:'recovering',role:'peer',reason:'waiting_for_system_connection'},disposed=0;
 const listeners=new Set(),events=new Set();
 const core={getState:()=>state,snapshot:()=>({rtcDiagnostics}),onState(fn){listeners.add(fn);return()=>listeners.delete(fn);},onEvent(fn){events.add(fn);return()=>events.delete(fn);},async join(){},command(){},dispose(){disposed++;listeners.clear();events.clear();}};
 const sdk=createPhantomEntry({relays:['wss://relay.example'],openContext:async()=>({}),coreFactory:()=>core});
 const invite=createInvite({relays:['wss://relay.example']});
 return {sdk,invite,get disposed(){return disposed},get listeners(){return listeners.size},update(next){state=next;for(const fn of [...listeners])fn(state);}};
}
test('default join stays pending through elapsed time and resolves only on admission',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});const f=fixture();let resolved=false;
 const p=f.sdk.join(f.invite).then(n=>{resolved=true;return n;});await tick();t.mock.timers.tick(60000);await tick();assert.equal(f.disposed,0);assert.equal(resolved,false);
 f.update({state:'admitted',role:'peer'});const n=await p;assert.equal(resolved,true);n.close();assert.equal(f.disposed,1);assert.equal(f.listeners,0);
});
test('aborting pending join disposes once and releases listeners',async()=>{
 const f=fixture(),controller=new AbortController();const p=f.sdk.join(f.invite,{signal:controller.signal});const rejected=assert.rejects(p,/Join aborted/);await tick();controller.abort();await rejected;assert.equal(f.disposed,1);assert.equal(f.listeners,0);
});
test('native/core failed state terminates pending admission and disposes it',async()=>{
 const f=fixture();const p=f.sdk.join(f.invite);const rejected=assert.rejects(p,/native_transport_failed/);await tick();f.update({state:'failed',reason:'native_transport_failed',role:'peer'});await rejected;assert.equal(f.disposed,1);
});
test('caller explicitly requesting a deadline can still cancel its join',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});const f=fixture();const p=f.sdk.join(f.invite,{timeoutMs:10});const rejected=assert.rejects(p,/Network admission timed out/);await tick();t.mock.timers.tick(11);await rejected;assert.equal(f.disposed,1);
});

test('admission timeout retains browser RTC evidence before disposal without inferring a cause',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});
 const rtc={connections:[{events:[{reason:'ice_candidate_error',detail:{errorCode:401,errorText:'Unauthorized.',url:'turn:relay.example:3478'}}]}]},f=fixture(rtc);
 const p=f.sdk.join(f.invite,{timeoutMs:10});
 const rejected=assert.rejects(p,error=>{assert.equal(error.code,'ADMISSION_TIMEOUT');assert.equal(error.message,'Network admission timed out');assert.deepEqual(error.diagnostics,{state:{state:'recovering',role:'peer',reason:'waiting_for_system_connection'},rtc});return true;});
 await tick();t.mock.timers.tick(10);await rejected;assert.equal(f.disposed,1);assert.equal(f.listeners,0);
});
test('admission timeout permits absent RTC evidence',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});const f=fixture(),p=f.sdk.join(f.invite,{timeoutMs:10});
 const rejected=assert.rejects(p,error=>{assert.equal(error.code,'ADMISSION_TIMEOUT');assert.equal(error.diagnostics.rtc,null);return true;});
 await tick();t.mock.timers.tick(10);await rejected;assert.equal(f.disposed,1);
});

test('cancelled context initialization closes its checkpoint without constructing a core',async()=>{
 let ready,closed=0,allocated=0;
 const sdk=createPhantomEntry({openContext:()=>new Promise(resolve=>ready=resolve),coreFactory:()=>{allocated++;throw Error('must not allocate');}});
 const controller=new AbortController(),p=sdk.join(createInvite({relays:['wss://relay.example']}),{signal:controller.signal});
 const rejected=assert.rejects(p,/Join aborted/);
 controller.abort();ready({admissionCheckpoint:{close(){closed++;}}});await rejected;
 assert.equal(allocated,0);assert.equal(closed,1);
});

test('terminal attempt emits cleaned only after disposal and the next join succeeds on the same facade',async()=>{
 const cores=[],events=[];
 const sdk=createPhantomEntry({openContext:async()=>({}),coreFactory:()=>{
  let state={state:'recovering',reason:'waiting_for_owner'},disposed=false;
  const states=new Set(),observers=new Set();
  const core={getState:()=>state,snapshot:()=>({}),onState(fn){states.add(fn);return()=>states.delete(fn);},onEvent(fn){observers.add(fn);return()=>observers.delete(fn);},async join(){},command(){},dispose(){disposed=true;states.clear();observers.clear();},update(next){state=next;for(const fn of [...states])fn(state);},get disposed(){return disposed;},get listeners(){return states.size+observers.size;}};
  cores.push(core);return core;
 }});
 const invite=createInvite({relays:['wss://relay.example']}),p=sdk.join(invite,{onEvent:event=>{events.push(event.type);if(event.type==='ADMISSION_ATTEMPT_CLEANED')assert.equal(cores[0].disposed,true);}});
 const rejected=assert.rejects(p,error=>{assert.equal(error.code,'TRANSPORT_TERMINAL');assert.equal(error.attempt.status,'cleaned');return true;});
 await tick();cores[0].update({state:'failed',reason:'owner_attempt_exhausted'});await rejected;
 assert.deepEqual(events,['ADMISSION_ATTEMPT_TERMINAL','ADMISSION_ATTEMPT_CLEANED']);assert.equal(cores[0].listeners,0);
 const next=sdk.join(invite);await tick();cores[1].update({state:'admitted'});const network=await next;network.close();assert.equal(cores[1].disposed,true);
});

test('abort before queued join work runs cannot start the disposed core',async()=>{
 const controller=new AbortController();let starts=0,disposed=0;
 const sdk=createPhantomEntry({openContext:async()=>({}),coreFactory:()=>({
  getState:()=>({state:'recovering'}),snapshot:()=>({}),onEvent:()=>()=>{},
  onState(){controller.abort();return()=>{};},join(){starts++;return Promise.resolve();},command(){},dispose(){disposed++;}
 })});
 await assert.rejects(sdk.join(createInvite({relays:['wss://relay.example']}),{signal:controller.signal}),/Join aborted/);
 await tick();assert.equal(starts,0);assert.equal(disposed,1);
});

test('waiting-for-owner observations do not restart the requested attempt budget',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});
 const f=fixture(),events=[],p=f.sdk.join(f.invite,{timeoutMs:30,onEvent:e=>events.push(e.type)});
 const rejected=assert.rejects(p,error=>error.code==='ADMISSION_TIMEOUT');await tick();
 for(let elapsed=0;elapsed<30;elapsed+=10){f.update({state:'recovering',reason:'sys_preflight_acceptor_waiting_for_owner'});t.mock.timers.tick(10);await tick();}
 await rejected;assert.equal(f.disposed,1);assert.equal(f.listeners,0);
 assert.deepEqual(events,['ADMISSION_ATTEMPT_TERMINAL','ADMISSION_ATTEMPT_CLEANED']);
});

test('throwing cleanup owner cannot prevent core disposal or listener cleanup',async()=>{
 let disposed=0;const states=new Set(),events=new Set(),reports=[];
 const sdk=createPhantomEntry({openContext:async()=>({}),coreFactory:()=>({getState:()=>({state:'admitted'}),snapshot:()=>({}),
  onState(fn){states.add(fn);return()=>{states.delete(fn);throw Error('unsubscribe failed');};},onEvent(fn){events.add(fn);return()=>events.delete(fn);},async join(){},command(){},dispose(){disposed++;}
 })});
 const network=await sdk.join(createInvite({relays:['wss://relay.example']}),{onEvent:e=>reports.push(e)});
 network.close();assert.equal(disposed,1);assert.equal(states.size,0);assert.equal(events.size,0);
 assert.ok(reports.some(e=>e.type==='CONNECTION_CLEANUP_ERROR'));network.close();assert.equal(disposed,1);
});
