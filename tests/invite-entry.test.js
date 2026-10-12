import test from 'node:test';
import assert from 'node:assert/strict';
import {createInvite,parseInvite,inviteLink} from '../packages/phantom-core/src/invite.js';
import {createPhantomEntry} from '../packages/phantom-core/src/entry.js';
const relays=['wss://relay.example/'];
test('portable invite roundtrips through browser fragment and isolates networks',()=>{
 const a=createInvite({relays}),b=createInvite({relays});assert.notEqual(a,b);
 const link=inviteLink(a,'https://app.example/?x=1');assert.equal(new URLSearchParams(new URL(link).hash.slice(1)).get('invite'),a);
 assert.deepEqual(parseInvite(a).relays,relays);
 for(const bad of ['phantom://v2/x','phantom://v1/'+ 'a'.repeat(64)+'?relay=ws://bad','phantom://v1/'+ 'a'.repeat(64)+'?relay=wss://user:password@host'])assert.throws(()=>parseInvite(bad));
});
function fixture(mode){let disposed=false,listener,state={state:'discovering'};return {get disposed(){return disposed;},core:{onEvent:()=>()=>{},onState:fn=>{listener=fn;return()=>{};},getState:()=>state,async join(){if(mode==='silence')return;state={state:mode,reason:'test_failure'};listener(state);},dispose(){disposed=true;}}};}
test('entry resolves only on admission and uses decoded network context',async()=>{
 const f=fixture('admitted');let context;const entry=createPhantomEntry({relays,openContext:async c=>{context=c;return{};},coreFactory:()=>f.core});
 const network=await entry.create();assert.equal(context.networkId,parseInvite(network.invite).networkId);assert.equal(network.getState().state,'admitted');network.close();assert.equal(f.disposed,true);
});
test('failed admission and silence release the instance',async()=>{
 for(const mode of ['failed','silence']){const f=fixture(mode);const entry=createPhantomEntry({openContext:async()=>({}),coreFactory:()=>f.core});await assert.rejects(entry.join(createInvite({relays}),{timeoutMs:10}));assert.equal(f.disposed,true);}
});
test('abort releases a pending join',async()=>{
 const f=fixture('silence'),controller=new AbortController();const entry=createPhantomEntry({openContext:async()=>({}),coreFactory:()=>f.core});const result=entry.join(createInvite({relays}),{signal:controller.signal});await Promise.resolve();controller.abort();await assert.rejects(result,/aborted/);assert.equal(f.disposed,true);
});
test('creator returns an invite before admission so a second peer can enter',async()=>{
 const f=fixture('silence');const entry=createPhantomEntry({relays,openContext:async()=>({}),coreFactory:()=>f.core});
 const network=await entry.create({timeoutMs:100});assert.equal(network.getState().state,'discovering');assert.ok(parseInvite(network.invite).networkId);network.close();
});
test('real Nostr adapter creator initializes without falsely claiming singleton admission',async()=>{
 const {createBrowserPhantom}=await import('../packages/phantom-doorways/src/browser-entry.js');
 class Socket {static OPEN=1;readyState=0;constructor(){queueMicrotask(()=>{this.readyState=1;this.onopen?.();});}send(raw){const m=JSON.parse(raw);if(m[0]==='REQ')queueMicrotask(()=>this.onmessage?.({data:JSON.stringify(['EOSE',m[1]])}));if(m[0]==='EVENT')queueMicrotask(()=>this.onmessage?.({data:JSON.stringify(['OK',m[1].id,true,''])}));}close(){this.readyState=3;}}
 const sdk=createBrowserPhantom({relays,WebSocket:Socket});const network=await sdk.create({name:'First',timeoutMs:1000});try{assert.notEqual(network.getState().state,'admitted');assert.equal(parseInvite(network.invite).networkId,network.networkId);}finally{network.close();}
});
