import test from 'node:test';
import assert from 'node:assert/strict';
import {createPhantomEntry} from '../packages/phantom-core/src/entry.js';
import {createInvite} from '../packages/phantom-core/src/invite.js';
const tick=()=>new Promise(setImmediate);
function fixture(){
 let state={state:'recovering',role:'peer',reason:'waiting_for_system_connection'},disposed=0;
 const listeners=new Set(),events=new Set();
 const core={getState:()=>state,snapshot:()=>({}),onState(fn){listeners.add(fn);return()=>listeners.delete(fn);},onEvent(fn){events.add(fn);return()=>events.delete(fn);},async join(){},command(){},dispose(){disposed++;listeners.clear();events.clear();}};
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
