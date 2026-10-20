import test from 'node:test';
import assert from 'node:assert/strict';
import {createConnectionQueue} from '../src/connection-queue.js';
const tick = () => new Promise(setImmediate);

test('replacement waits for failed attempt cleanup and failure does not poison the queue', async () => {
  const queue = createConnectionQueue(), events = [];
  let finishCleanup;
  const first = queue.run(async () => {
    events.push('first failed');
    await new Promise(resolve => finishCleanup = resolve);
    events.push('first cleaned');
    throw Error('owner did not respond');
  });
  const rejected = assert.rejects(first,/owner did not respond/);
  const second = queue.run(async () => {events.push('second started');return {id:2};});
  await tick(); assert.deepEqual(events,['first failed']); assert.equal(queue.busy,true);
  finishCleanup(); await rejected; assert.equal((await second).id,2);
  assert.deepEqual(events,['first failed','first cleaned','second started']);
  await queue.settled(); assert.equal(queue.busy,false);
});

test('late success is closed before replacement starts; cancelled queued attempt allocates nothing', async () => {
  const queue = createConnectionQueue(), firstController = new AbortController(), nextController = new AbortController(), events = [];
  let finish;
  const first = queue.run(() => new Promise(resolve => finish = resolve), firstController.signal);
  const firstRejected = assert.rejects(first,/cancelled/);
  const cancelled = queue.run(() => {throw Error('must never allocate');}, nextController.signal);
  const cancelledRejected = assert.rejects(cancelled,/cancelled/);
  const replacement = queue.run(() => {events.push('replacement started');return {};});
  await tick(); firstController.abort(); nextController.abort();
  finish({close(){events.push('late connection closed');}});
  await firstRejected; await cancelledRejected; await replacement;
  assert.deepEqual(events,['late connection closed','replacement started']);
  assert.equal(queue.busy,false);
});

test('late successful admission releases membership before a new session starts',async()=>{
 const queue=createConnectionQueue(),controller=new AbortController(),events=[];let finish,release;
 const first=queue.run(()=>new Promise(resolve=>finish=resolve),controller.signal);
 const rejected=assert.rejects(first,/cancelled/);await tick();controller.abort();
 finish({async cancel(){events.push('releasing grant');await new Promise(resolve=>release=resolve);events.push('closed');}});
 const replacement=queue.run(()=>{events.push('replacement');return {};});
 await tick();assert.deepEqual(events,['releasing grant']);release();await rejected;await replacement;
 assert.deepEqual(events,['releasing grant','closed','replacement']);
});


test('active shutdown owns ending until asynchronous disposal completes, then replacement starts',async()=>{
 const phases=[],events=[],queue=createConnectionQueue({onState:s=>phases.push(s)});
 await queue.run(async()=>({}));assert.equal(queue.phase,'active');
 let finish;
 const ending=queue.cleanup(async()=>{events.push('disposing');await new Promise(r=>finish=r);events.push('disposed');});
 assert.equal(queue.phase,'ending');assert.equal(queue.busy,true);
 await tick();assert.deepEqual(events,['disposing']);
 const next=queue.run(()=>{events.push('replacement');return {};});
 finish();await ending;await next;
 assert.deepEqual(events,['disposing','disposed','replacement']);
 assert.equal(queue.phase,'active');assert.equal(queue.busy,false);
 assert.deepEqual(phases,['connecting','active','ending','connecting','active']);
});

test('ending cancellation fences late active callbacks and a cleanup failure cannot poison replacement',async()=>{
 const phases=[],queue=createConnectionQueue({onState:s=>phases.push(s)});
 let finish;const joining=queue.run(()=>new Promise(r=>finish=r));await tick();
 const ending=queue.cleanup(()=>{throw Error('owner disposal failed');});
 const rejected=assert.rejects(ending,/disposal failed/);
 finish({});await joining;await rejected;
 assert.deepEqual(phases,['connecting','ending','closed']);
 await queue.run(()=>({}));assert.equal(queue.phase,'active');
});
