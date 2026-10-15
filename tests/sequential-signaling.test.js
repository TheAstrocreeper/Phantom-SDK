import test from 'node:test';
import assert from 'node:assert/strict';
import {createSequentialSignaling} from '../packages/phantom-core/src/sequential-signaling.js';
function fixture(limit=256){let at=0;const timers=new Map(),events=[],calls=[];let seq=0,ready=false,current=true;
 const router=createSequentialSignaling({limit,graceMs:750,now:()=>at,setTimeout:fn=>{timers.set(++seq,fn);return seq;},clearTimeout:id=>timers.delete(id),status:e=>events.push(e)});
 const send=(available=[true,true,true])=>router.send({peer:'peer',attemptId:'attempt',signalType:'rtc_offer',current:()=>current,complete:()=>ready,routes:['direct','bridge','relay'].map((name,i)=>({name,send:()=>{calls.push(name);return available[i];}}))});
 return {router,send,calls,events,timers,ready:()=>{ready=true;},retire:()=>{current=false;},tick(){at+=750;const work=[...timers.values()];timers.clear();for(const fn of work)fn();}};
}
test('direct then bridge then relay are submitted sequentially with bounded grace',()=>{
 const f=fixture();assert.equal(f.send(),true);assert.deepEqual(f.calls,['direct']);f.tick();assert.deepEqual(f.calls,['direct','bridge']);f.tick();assert.deepEqual(f.calls,['direct','bridge','relay']);assert.equal(f.router.pending,0);assert.equal(f.events.at(-1).elapsedMs,1500);
});
test('unavailable paths skip grace and bootstrap reaches relay immediately',()=>{
 const f=fixture();f.send([false,false,true]);assert.deepEqual(f.calls,['direct','bridge','relay']);assert.equal(f.timers.size,0);
});
test('native readiness and retired attempts cancel delayed fallback',()=>{
 for(const action of ['ready','retire']){const f=fixture();f.send();f[action]();f.tick();assert.deepEqual(f.calls,['direct']);assert.equal(f.router.pending,0);assert.equal(f.events.at(-1).action,'cancelled');}
});
test('failed submissions fall through and session teardown releases timers',()=>{
 const f=fixture();f.send([false,true,true]);assert.deepEqual(f.calls,['direct','bridge']);f.router.clear();f.tick();assert.deepEqual(f.calls,['direct','bridge']);assert.equal(f.router.pending,0);
});
test('bounded queue rejects additional payloads without bypassing preferred routes',()=>{
 const f=fixture(1);f.send();assert.equal(f.send(),false);assert.deepEqual(f.calls,['direct']);assert.equal(f.router.pending,1);f.router.clear();assert.equal(f.timers.size,0);
});
test('closing from submission telemetry cannot install a timer after teardown',()=>{
 for(const closeOn of ['submitted','waiting']){
 const timers=new Set();let router;
 router=createSequentialSignaling({setTimeout:fn=>{timers.add(fn);return fn;},clearTimeout:id=>timers.delete(id),status:e=>{if(e.action===closeOn)router.clear();}});
 router.send({peer:'peer',attemptId:'a',signalType:'rtc_offer',current:()=>true,complete:()=>false,routes:[{name:'direct',send:()=>true},{name:'relay',send:()=>true}]});
 assert.equal(router.pending,0);assert.equal(timers.size,0);
 }
});

test('offer and ICE share one route and one fallback clock for the native attempt',()=>{
 const f=fixture();f.send();f.send();f.send();assert.equal(f.timers.size,1);assert.equal(f.router.pending,3);
 f.tick();assert.deepEqual(f.calls,['direct','direct','direct','bridge','bridge','bridge']);assert.equal(f.timers.size,1);
 f.send();assert.equal(f.calls.at(-1),'bridge');assert.equal(f.timers.size,1);
 f.tick();assert.deepEqual(f.calls.slice(-4),['relay','relay','relay','relay']);assert.equal(f.router.pending,0);
 f.send();assert.equal(f.calls.at(-1),'relay');assert.equal(f.timers.size,0);
});
test('explicit attempt teardown releases all retained signals immediately',()=>{
 const f=fixture();f.send();f.send();f.router.cancel('peer');assert.equal(f.timers.size,0);assert.equal(f.router.pending,0);f.tick();assert.deepEqual(f.calls,['direct','direct']);
});
