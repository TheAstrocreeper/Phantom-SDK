import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createReubenComparisonEndpoint} from '../packages/phantom-core/experimental/reuben-comparison.js';
import {impairedRTC} from '../tools/perf/reuben/impaired-rtc.mjs';
for(const engine of ['current','reuben','reuben-split','reuben-batches','reuben-trusted'])test('shared comparison endpoint verifies '+engine,{timeout:15000},async()=>{
 const model=impairedRTC({oneWayMs:2,mbps:50}),ids=['1111111111111111','2222222222222222'],admin=await createAdminIdentity();let endpoints=[],resolve,reject;
 const done=new Promise((a,b)=>{resolve=a;reject=b;});
 endpoints=ids.map((selfId,i)=>createReubenComparisonEndpoint({engine,role:i?'receiver':'sender',selfId,peerId:ids[1-i],networkId:'comparison-test',adminPublicKey:admin.publicKey,admin:i?undefined:admin,runtime:{crypto,RTCPeerConnection:model.RTC},bytes:2*1024*1024+17,onComplete:resolve,onError:reject,send:message=>queueMicrotask(()=>{void endpoints[1-i].handle(message).catch(reject);})}));
 try{await endpoints[0].start();const report=await done;assert.equal(report.verified,true);if(engine==='reuben-batches')assert.equal(report.requestBoundaryCount,2);if(engine==='reuben-trusted'){assert.equal(report.storage,'verified-inventory-memory');assert.ok(report.assemblyMs>=0);}if(engine==='reuben-split')assert.equal(report.wireFormat,'split');assert.equal(report.engine,engine);assert.ok(report.elapsedMs>0);assert.ok(report.peakWrites<=4);assert.ok(report.channels.length);assert.equal(report.bytes,2*1024*1024+17);assert.equal(JSON.stringify(report).includes('candidate:'),false);}finally{endpoints.forEach(e=>e.close());model.close();}
});
