import test from 'node:test';
import assert from 'node:assert/strict';
import {observeRTC} from './observed-rtc.mjs';
test('RTC observer releases closed objects even without native state events',()=>{
 class Native {constructor(fail){if(fail)throw Error('capacity');}addEventListener(){}close(){this.connectionState='closed';}}
 const observed=observeRTC(Native,()=>{});
 for(let i=0;i<500;i++){const pc=new observed.RTCPeerConnection();assert.equal(observed.active.size,1);pc.close();pc.close();assert.equal(observed.active.size,0);}
 assert.throws(()=>new observed.RTCPeerConnection(true),/capacity/);
 assert.deepEqual(observed.counts,{created:500,closed:500,constructionFailed:1});
});
