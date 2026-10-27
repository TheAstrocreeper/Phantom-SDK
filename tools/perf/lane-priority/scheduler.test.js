import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createLaneScheduler} from './scheduler.js';

test('assigns distinct work and finishes ranges out of order',()=>{
 const s=createLaneScheduler({rangeCount:3});const a=s.next(0),b=s.next(1),c=s.next(2);
 assert.deepEqual([a.index,b.index,c.index],[0,1,2]);assert.equal(s.finish(2,c),true);assert.equal(s.finish(0,a),true);assert.equal(s.finish(1,b),true);assert.equal(s.snapshot().complete,3);
});
test('control uses the first idle lane and falls back when it closes',()=>{
 const s=createLaneScheduler({rangeCount:3});const first=s.next(0);s.control('cancel');const c=s.next(1);assert.equal(c.kind,'control');s.close(1);const fallback=s.next(2);assert.equal(fallback.kind,'control');assert.equal(fallback.id,c.id);assert.equal(s.finish(1,c),false);assert.equal(s.finish(0,first),true);
});
test('failed lane returns unfinished range and stale completion cannot finish reassignment',()=>{
 const s=createLaneScheduler({rangeCount:2});const old=s.next(0);s.close(0);const next=s.next(1);assert.equal(next.index,old.index);assert.notEqual(next.generation,old.generation);assert.equal(s.finish(1,old),false);assert.equal(s.finish(1,next),true);assert.equal(s.next(1).index,1);
});
test('queued control work remains bounded',()=>{const s=createLaneScheduler({rangeCount:1,maxControl:1});s.control('a');assert.throws(()=>s.control('b'),/full/);});
