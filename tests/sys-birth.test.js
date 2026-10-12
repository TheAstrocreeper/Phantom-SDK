import test from 'node:test';import assert from 'node:assert/strict';
import {sysBirthDisposition,isCurrentSysBirth} from '../packages/phantom-core/src/sys-birth.js';
test('stable SDP with a connecting channel remains the same birth',()=>{
 const pc={signalingState:'stable',connectionState:'connecting',iceConnectionState:'checking',localDescription:{type:'offer'}};
 assert.equal(sysBirthDisposition(pc,{readyState:'connecting'}),'waiting');
 pc.remoteDescription={type:'answer'};assert.equal(sysBirthDisposition(pc,{readyState:'connecting'}),'waiting');
 assert.equal(sysBirthDisposition(pc,{readyState:'open'}),'open');pc.connectionState='failed';assert.equal(sysBirthDisposition(pc,{readyState:'connecting'}),'failed');
});
test('retired asynchronous birth cannot mutate a replacement transport',()=>{
 const old={pc:{signalingState:'stable'}},next={pc:{signalingState:'stable'}},registry=new Map([['peer',old]]);
 assert.equal(isCurrentSysBirth(registry,'peer',old),true);registry.set('peer',next);assert.equal(isCurrentSysBirth(registry,'peer',old),false);assert.equal(isCurrentSysBirth(registry,'peer',next),true);
});

import {runSysBirth} from '../packages/phantom-core/src/sys-birth.js';
test('reentrant birth requests share setup through its asynchronous completion',async()=>{
 const flights=new Map();let release,calls=0;
 const operation=async()=>{calls++;await new Promise(resolve=>{release=resolve});return 'sent';};
 const first=runSysBirth(flights,'peer',operation);
 await Promise.resolve();
 const repeated=runSysBirth(flights,'peer',operation);
 assert.equal(first,repeated);assert.equal(calls,1);
 release();assert.equal(await repeated,'sent');assert.equal(flights.size,0);
 assert.equal(await runSysBirth(flights,'peer',()=> 'replacement'),'replacement');
});
test('failed birth releases only its own flight and another peer proceeds independently',async()=>{
 const flights=new Map();let reject;
 const failed=runSysBirth(flights,'a',()=>new Promise((_,r)=>{reject=r}));
 await Promise.resolve();
 assert.equal(await runSysBirth(flights,'b',()=>true),true);
 reject(new Error('setup failure'));await assert.rejects(failed,/setup failure/);
 assert.equal(flights.size,0);
});

import {completeSysBirth,armSysBirthDeadline} from '../packages/phantom-core/src/sys-birth.js';
test('successful setup cancels deadline and later channel loss cannot expire that setup',()=>{
 let callback,cancelled=0,expired=0;
 const entry={sysdc:{readyState:'connecting'}};
 armSysBirthDeadline(entry,{schedule:fn=>{callback=fn;return 42},cancel:id=>{assert.equal(id,42);cancelled++}},10000,()=>expired++);
 entry.sysdc.readyState='open';completeSysBirth(entry);
 entry.sysdc.readyState='closed';callback();
 assert.equal(cancelled,1);assert.equal(expired,0);assert.equal(entry.sdkSysBirthDeadline,null);
});
test('completion before arming is inert; genuine silence expires once; replaced deadlines cannot fire',()=>{
 const callbacks=[];let expired=0;
 const timers={schedule:fn=>{callbacks.push(fn);return callbacks.length},cancel:()=>{}};
 const ready={};completeSysBirth(ready);armSysBirthDeadline(ready,timers,10,()=>expired++);assert.equal(callbacks.length,0);
 const pending={};armSysBirthDeadline(pending,timers,10,()=>expired++);armSysBirthDeadline(pending,timers,10,()=>expired++);
 callbacks[0]();assert.equal(expired,0);callbacks[1]();callbacks[1]();assert.equal(expired,1);
});
