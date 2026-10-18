import test from 'node:test';
import assert from 'node:assert/strict';
import {createPreparedSendQueue} from '../packages/phantom-core/src/file-pipeline.js';
const tick=()=>new Promise(r=>setImmediate(r));
test('preparation queue caps admitted work and preserves dispatch order across out-of-order reads',async()=>{
 const queue=createPreparedSendQueue(),gates=[],sent=[],tasks=[];let active=0,peak=0;
 try{
  for(let i=0;i<16;i++)tasks.push(queue.enqueue('peer',()=>new Promise(resolve=>{active++;peak=Math.max(peak,active);gates[i]=()=>{active--;resolve(i);};}),i=>sent.push(i)));
  assert.equal(queue.enqueue('peer',()=>0,()=>{}),null);await tick();assert.equal(peak,8);
  for(let i=7;i>=0;i--)gates[i]();await tick();
  for(let i=15;i>=8;i--)gates[i]();await Promise.all(tasks);
  assert.deepEqual(sent,Array.from({length:16},(_,i)=>i));assert.equal(peak,8);
 }finally{queue.close();}
});
test('closing preparation queue prevents prepared and waiting jobs from dispatching',async()=>{
 const queue=createPreparedSendQueue({concurrency:1});let release,sent=0;
 const a=queue.enqueue('peer',()=>new Promise(r=>release=r),()=>sent++),b=queue.enqueue('peer',()=>1,()=>sent++);
 const results=Promise.allSettled([a,b]);await tick();queue.close();release(0);
 assert.ok((await results).every(r=>r.status==='rejected'));assert.equal(sent,0);assert.equal(queue.enqueue('peer',()=>1,()=>sent++),null);
});
