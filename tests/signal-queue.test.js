import test from 'node:test';
import assert from 'node:assert/strict';
import {createSignalQueue} from '../packages/phantom-core/src/signal-queue.js';
test('one peer queue survives transport replacement and remains FIFO',async()=>{
 const q=createSignalQueue();let release,transport='old';const order=[];
 const first=q.enqueue('peer',async()=>{await new Promise(r=>release=r);transport='new';order.push('offer');});
 await new Promise(r=>setImmediate(r));const second=q.enqueue('peer',()=>{order.push(transport);});release();await Promise.all([first,second]);
 assert.deepEqual(order,['offer','new']);assert.equal(q.snapshot().queuedSignals,0);assert.equal(q.snapshot().activeQueues,0);
});
test('flood capacity is bounded before tasks execute and drains cleanly',async()=>{
 const q=createSignalQueue({perKey:4,total:8});let release;const blocked=new Promise(r=>release=r);let ran=0;
 const jobs=[];for(let i=0;i<2000;i++)jobs.push(q.enqueue('peer'+i%2,async()=>{ran++;await blocked;}));
 assert.equal(q.snapshot().queuedSignals,8);assert.equal(q.snapshot().activeQueues,2);await new Promise(r=>setImmediate(r));assert.equal(ran,2);
 release();const result=await Promise.all(jobs);assert.equal(ran,8);assert.equal(result.filter(v=>v===false).length,1992);assert.equal(q.snapshot().queuedSignals,0);assert.equal(q.snapshot().activeQueues,0);
});
test('failed task releases capacity without poisoning the successor',async()=>{
 const q=createSignalQueue();const failed=q.enqueue('peer',()=>{throw Error('failed');});const next=q.enqueue('peer',()=>true);
 await assert.rejects(failed,/failed/);assert.equal(await next,true);assert.equal(q.snapshot().queuedSignals,0);
});
