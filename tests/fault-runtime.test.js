import test from 'node:test';import assert from 'node:assert/strict';
import {createFaultRuntime} from './fixtures/fault-runtime.js';
test('abrupt harness loss stops callbacks and closes RTC without engine disposal',()=>{
 const callbacks=new Map();let next=0,calls=0;const cleared=[];
 class PC{addEventListener(){}createDataChannel(){return{onclose:()=>calls++};}close(){this.closed=true;this.onconnectionstatechange?.();}}
 const f=createFaultRuntime({crypto:{},RTCPeerConnection:PC,setTimeout(fn){callbacks.set(++next,fn);return next;},clearTimeout(id){cleared.push(id);},setInterval(fn){callbacks.set(++next,fn);return next;},clearInterval(id){cleared.push(id);}});
 const pc=new f.runtime.RTCPeerConnection(),dc=pc.createDataChannel();pc.onconnectionstatechange=()=>calls++;
 f.runtime.setTimeout(()=>calls++,1);f.runtime.setInterval(()=>calls++,1);f.stop();
 for(const fn of callbacks.values())fn();assert.equal(calls,0);assert.equal(pc.closed,true);assert.equal(dc.onclose,null);assert.equal(cleared.length,2);
});
