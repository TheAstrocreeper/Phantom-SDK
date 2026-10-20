import test from 'node:test';
import assert from 'node:assert/strict';
import {waitForDataChannelCapacity} from '../packages/phantom-core/src/data-channel-capacity.js';
class Channel extends EventTarget{
 constructor(amount){super();this.bufferedAmount=amount;this.readyState='open';this.handlers=new Map();}
 addEventListener(type,fn,...args){super.addEventListener(type,fn,...args);if(!this.handlers.has(type))this.handlers.set(type,new Set());this.handlers.get(type).add(fn);}
 removeEventListener(type,fn,...args){super.removeEventListener(type,fn,...args);this.handlers.get(type)?.delete(fn);}
 get listenerCount(){return [...this.handlers.values()].reduce((n,s)=>n+s.size,0);}
 drain(amount){this.bufferedAmount=amount;this.dispatchEvent(new Event('bufferedamountlow'));}
}
test('capacity within high water does not install listeners or schedule a poll',async()=>{
 const dc=new Channel(1024);await waitForDataChannelCapacity(dc,{bytes:65536,setTimeout(){assert.fail('no timer needed');}});assert.equal(dc.listenerCount,0);
});
test('full channel resumes only on a low-water event and releases listeners and failure timer',async()=>{
 const dc=new Channel(16*1024*1024),active=new Set();let done=false;
 const p=waitForDataChannelCapacity(dc,{bytes:65536,setTimeout(fn){active.add(fn);return fn;},clearTimeout(fn){active.delete(fn);}}).then(()=>done=true);
 assert.equal(dc.bufferedAmountLowThreshold,4*1024*1024);assert.equal(active.size,1);
 dc.drain(8*1024*1024);await Promise.resolve();assert.equal(done,false);
 dc.drain(4*1024*1024);await p;assert.equal(done,true);assert.equal(active.size,0);assert.equal(dc.listenerCount,0);
});
for(const ending of ['close','error','abort'])test(`buffer wait rejects on ${ending} and cleans handlers`,async()=>{
 const dc=new Channel(20*1024*1024),controller=new AbortController();
 const p=waitForDataChannelCapacity(dc,{signal:controller.signal});const rejected=assert.rejects(p,/closed|aborted/);
 if(ending==='abort')controller.abort();else{dc.readyState='closed';dc.dispatchEvent(new Event(ending));}
 await rejected;assert.equal(dc.listenerCount,0);
});
test('timeout rejects a still-full buffer rather than granting capacity',async()=>{
 const dc=new Channel(20*1024*1024);let timeout;
 const p=waitForDataChannelCapacity(dc,{setTimeout(fn){timeout=fn;return 1;},clearTimeout(){}});const rejected=assert.rejects(p,/drain timed out/);timeout();await rejected;dc.drain(0);assert.equal(dc.listenerCount,0);
});
