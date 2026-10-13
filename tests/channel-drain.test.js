import test from 'node:test';
import assert from 'node:assert/strict';
import {waitForChannelDrain} from '../packages/phantom-core/src/channel-drain.js';
function fixture(){const dc=new EventTarget();dc.readyState='open';dc.bufferedAmount=3*1024*1024;let callback,count=0,cancelled=0;return {dc,get count(){return count;},get cancelled(){return cancelled;},fire(){callback();},timers:{schedule(fn){count++;callback=fn;return 1;},cancel(){cancelled++;}}};}
test('concurrent waits share one watchdog; drain cancels it once',async()=>{
 const f=fixture();const options=f.timers;const a=waitForChannelDrain(f.dc,options),b=waitForChannelDrain(f.dc,options);assert.equal(a,b);assert.equal(f.count,1);f.dc.bufferedAmount=0;f.dc.dispatchEvent(new Event('bufferedamountlow'));assert.equal(await a,true);assert.equal(f.cancelled,1);f.fire();assert.equal(f.cancelled,1);
});
test('close cancels the watchdog and releases the wait',async()=>{
 const f=fixture();const p=waitForChannelDrain(f.dc,f.timers);f.dc.readyState='closed';f.dc.dispatchEvent(new Event('close'));assert.equal(await p,false);assert.equal(f.cancelled,1);
});
test('deadline does not claim a still-buffered channel drained',async()=>{
 const f=fixture();const p=waitForChannelDrain(f.dc,f.timers);f.fire();assert.equal(await p,false);assert.equal(f.cancelled,1);
});

test('channel error releases ownership without claiming successful drain',async()=>{
 const f=fixture();const p=waitForChannelDrain(f.dc,f.timers);f.dc.bufferedAmount=0;f.dc.dispatchEvent(new Event('error'));assert.equal(await p,false);assert.equal(f.cancelled,1);
});
