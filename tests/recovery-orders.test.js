import test from 'node:test';
import assert from 'node:assert/strict';
import {createPhantomEntry} from '../packages/phantom-core/src/entry.js';
import {encodeInvite} from '../packages/phantom-core/src/invite.js';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createControlStore} from '../packages/phantom-core/src/admin-lifecycle.js';
const tick=()=>new Promise(r=>setTimeout(r,15));
async function setup(read){const admin=await createAdminIdentity(),room='a'.repeat(64),records=new Map(),states=new Set(),errors=[];let state={state:'admitted',peerId:'b'.repeat(16),leader:'c'.repeat(16),mirror:'d'.repeat(16),role:'peer'};
 const core={getState:()=>state,snapshot:()=>null,onState(fn){states.add(fn);return()=>states.delete(fn);},onEvent(){return()=>{};},async join(){for(const fn of states)fn(state);},command(){},dispose(){state={...state,state:'closed'};}};
 const store=createControlStore({getItem:k=>records.get(k)??null,setItem:(k,v)=>records.set(k,v)});
 const network=await createPhantomEntry({coreFactory:()=>core,openContext:async()=>({controlStore:store,runtime:globalThis,readAdminOrders:read,retireGraceMs:0})}).join(encodeInvite({networkId:room,adminPublicKey:admin.publicKey,doorways:[{type:'websocket',url:'wss://fixture/'}]}));network.on('controlError',e=>errors.push(e));
 return {admin,room,network,records,errors,recover(){state={...state,state:'recovering'};for(const fn of states)fn(state);},admit(){state={...state,state:'admitted'};for(const fn of states)fn(state);}};
}
test('recovering leaf independently verifies and retains isolated admin retirement',async()=>{let order,calls=0;const f=await setup(async context=>{calls++;assert.equal(context.reason,'recovery');return[order];});try{order=await f.admin.sign({networkId:f.room,action:'network.retire',target:f.room,operationId:'retire'});f.recover();for(let i=0;i<20&&f.network.getState().state!=='retired';i++)await tick();assert.equal(f.network.getState().state,'retired');assert.equal(f.network.getControls().length,1);assert.equal(calls,1);}finally{f.network.close();}});
test('recovery response arriving after close or restored admission cannot apply controls',async()=>{for(const action of ['close','admit']){let release;const f=await setup(()=>new Promise(r=>release=r));const order=await f.admin.sign({networkId:f.room,action:'network.retire',target:f.room,operationId:'retire'});try{f.recover();await tick();assert.equal(typeof release,'function');action==='close'?f.network.close():f.admit();release([order]);await tick();assert.equal(f.network.getControls().length,0);assert.equal(f.records.size,0);}finally{f.network.close();}}});
test('forged recovery order cannot retire a member or approve leadership',async()=>{const f=await setup(async()=>[{signature:'forged'}]);try{f.recover();await tick();assert.equal(f.network.getState().state,'recovering');assert.equal(f.network.getControls().length,0);assert.equal(f.network.getAdminState().phase,'idle');assert.ok(f.errors.length);}finally{f.network.close();}});
