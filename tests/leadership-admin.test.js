import test from 'node:test';
import assert from 'node:assert/strict';
import {createLeadershipAdmin} from '../packages/phantom-core/src/leadership-admin.js';
const leader={role:'syscon1',peerId:'a',leader:'a'},peer={role:'peer',peerId:'a',leader:'b'};
const flush=()=>new Promise(resolve=>setTimeout(resolve,10));
test('leadership duties wait for orders; approval is cached for that tenure',async()=>{
 let resolve,calls=0,resumed=0;
 const gate=createLeadershipAdmin({readOrders:()=>{calls++;return new Promise(r=>resolve=r);}});
 try{
 assert.equal(gate.authorize(leader,'roster',()=>resumed++),false);
 await flush();assert.equal(resumed,0);assert.equal(gate.snapshot().phase,'checking');
 resolve([]);await flush();assert.equal(resumed,1);
 assert.equal(gate.authorize(leader,'roster',()=>resumed++),true);assert.equal(calls,1);
 }finally{gate.close();}
});
test('losing leadership discards stale approval and queries again on promotion',async()=>{
 const resolvers=[];let resumed=0;
 const gate=createLeadershipAdmin({readOrders:()=>new Promise(r=>resolvers.push(r))});
 try{
 gate.authorize(leader,'roster',()=>resumed++);await flush();gate.observe(peer);
 resolvers[0]([]);await flush();assert.equal(resumed,0);
 gate.authorize(leader,'roster',()=>resumed++);await flush();resolvers[1]([]);
 await flush();assert.equal(resumed,1);assert.equal(resolvers.length,2);
 }finally{gate.close();}
});
test('retained kill prevents deferred duties; unavailable authority never approves',async()=>{
 let retired=false,resumed=0;
 const gate=createLeadershipAdmin({readOrders:async()=>['kill'],applyOrder:async()=>{retired=true;return true;},retired:()=>retired});
 gate.authorize(leader,'roster',()=>resumed++);await flush();
 assert.equal(resumed,0);assert.equal(gate.snapshot().phase,'retired');gate.close();
 let calls=0;
 const failed=createLeadershipAdmin({retryMs:15,readOrders:async()=>{calls++;throw Error('offline');}});
 failed.authorize(leader,'roster',()=>resumed++);await new Promise(r=>setTimeout(r,40));
 assert.ok(calls>=2);assert.equal(resumed,0);assert.equal(failed.snapshot().phase,'failed');failed.close();
});
