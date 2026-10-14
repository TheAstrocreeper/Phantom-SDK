import test from 'node:test';
import assert from 'node:assert/strict';
import {createNetworkPermissions} from '../packages/phantom-core/src/network-permissions.js';
function fixture(state='prompt'){
 const listeners=new Set(),status={state,addEventListener:(type,fn)=>listeners.add(fn),removeEventListener:(type,fn)=>listeners.delete(fn)};
 return {status,listeners,runtime:{navigator:{permissions:{query:async()=>status}}},change(state){status.state=state;for(const fn of listeners)fn();}};
}
test('permission grant/revocation changes evidence and removes listener on disposal',async()=>{
 const f=fixture(),events=[],m=createNetworkPermissions({runtime:f.runtime,onChange:e=>events.push(e)});
 await m.refresh();assert.equal(m.snapshot().localNetwork.state,'prompt');assert.equal(m.snapshot().localNetwork.name,'local-network');
 f.change('granted');assert.equal(events.at(-1).localNetwork.state,'granted');f.change('denied');assert.equal(events.at(-1).localNetwork.nextAction,'review_local_network_if_connection_fails');
 const detached=m.snapshot();detached.localNetwork.state='granted';assert.equal(m.snapshot().localNetwork.state,'denied');m.dispose();assert.equal(f.listeners.size,0);f.change('granted');assert.equal(m.snapshot().localNetwork.state,'denied');
});
test('legacy permission fallback and unsupported runtimes remain explicit unknown',async()=>{
 const calls=[],m=createNetworkPermissions({runtime:{navigator:{permissions:{query:async({name})=>{calls.push(name);if(name==='local-network')throw new TypeError('unsupported');return {state:'granted'};}}}}});
 await m.refresh();assert.deepEqual(calls,['local-network','local-network-access']);assert.equal(m.snapshot().localNetwork.state,'granted');m.dispose();
 for(const runtime of [{},{navigator:{permissions:{query:async()=>{throw new TypeError('unsupported');}}}}]){const n=createNetworkPermissions({runtime});await n.refresh();assert.equal(n.snapshot().localNetwork.state,'unknown');assert.equal(n.snapshot().localNetwork.nextAction,'none');n.dispose();}
});
test('late permission responses cannot overwrite refresh or attach listeners after disposal',async()=>{
 const deferred=[],f=fixture('granted'),m=createNetworkPermissions({runtime:{navigator:{permissions:{query:()=>new Promise(resolve=>deferred.push(resolve))}}}});
 const first=m.refresh(),second=m.refresh();deferred[1](f.status);await second;deferred[0]({state:'denied'});await first;assert.equal(m.snapshot().localNetwork.state,'granted');assert.equal(f.listeners.size,1);
 const third=m.refresh();assert.equal(f.listeners.size,0);m.dispose();deferred[2](f.status);await third;assert.equal(f.listeners.size,0);
});
