import {workloadQueuePolicy} from '../packages/phantom-core/src/workload-queue-comparison.js';
import test from 'node:test';
import assert from 'node:assert/strict';
import {sendFileChannelPayload} from '../packages/phantom-core/src/file-channel-payload.js';
class Channel extends EventTarget{
 constructor(){super();this.readyState='open';this.bufferedAmount=0;this.peak=0;this.frames=[];}
 send(data){this.frames.push(data);this.bufferedAmount+=typeof data==='string'?new TextEncoder().encode(data).byteLength:data.byteLength;this.peak=Math.max(this.peak,this.bufferedAmount);setImmediate(()=>{this.bufferedAmount=0;this.dispatchEvent(new Event('bufferedamountlow'));});}
}
test('concurrent payloads reserve one channel in order without exceeding its queue policy',async()=>{
 const dc=new Channel(),pcs=[{sctp:{maxMessageSize:262144}}],queuePolicy={highWaterMark:1048576,lowWaterMark:524288};
 await Promise.all(Array.from({length:20},(_,index)=>sendFileChannelPayload({dc,pcs,bytes:new ArrayBuffer(262144),file:{index,bytes:262144},queuePolicy})));
 assert.ok(dc.peak<=queuePolicy.highWaterMark,`queue grew to ${dc.peak}`);assert.equal(dc.frames.length,40);
 assert.deepEqual(dc.frames.filter(data=>typeof data==='string').map(data=>JSON.parse(data).file.index),Array.from({length:20},(_,i)=>i));
});
test('fragmented payloads cannot interleave their reassembly streams on a shared channel',async()=>{
 const dc=new Channel();await Promise.all([0,1].map(index=>sendFileChannelPayload({dc,pcs:[{sctp:{maxMessageSize:16384}}],bytes:new ArrayBuffer(32768),file:{index,bytes:32768},fragments:true,messageBytes:16384})));
 assert.deepEqual(dc.frames.filter(data=>typeof data==='string').map(data=>JSON.parse(data).file.index),[0,0,1,1]);
});
test('an aborted queued send does not poison later work on the same channel',async()=>{
 const dc=new Channel(),controller=new AbortController();controller.abort();
 await assert.rejects(sendFileChannelPayload({dc,pcs:[{sctp:{maxMessageSize:65536}}],bytes:new ArrayBuffer(1024),file:{index:0},signal:controller.signal}),/aborted/);
 await sendFileChannelPayload({dc,pcs:[{sctp:{maxMessageSize:65536}}],bytes:new ArrayBuffer(1024),file:{index:1}});assert.equal(dc.frames.length,2);
});

test('equal-share peer queues stay within the total budget and a stalled lane cannot block siblings',async()=>{
 const channels=[],controller=new AbortController();let aggregatePeak=0;
 class Lane extends EventTarget{
  constructor(lane){super();this.lane=lane;this.readyState='open';this.bufferedAmount=0;this.frames=0;}
  send(data){this.frames++;this.bufferedAmount+=typeof data==='string'?new TextEncoder().encode(data).byteLength:data.byteLength;aggregatePeak=Math.max(aggregatePeak,channels.reduce((sum,dc)=>sum+dc.bufferedAmount,0));if(this.lane!==0)setImmediate(()=>{this.bufferedAmount=0;this.dispatchEvent(new Event('bufferedamountlow'));});}
 }
 channels.push(...[0,1,2].map(lane=>new Lane(lane)));
 const policy=workloadQueuePolicy({lanes:[0,1,2],queueTrial:'tight-1',queueBudgetScope:'peer'}),pcs=[{sctp:{maxMessageSize:262144}}];
 const sends=channels.map(dc=>Promise.all(Array.from({length:12},(_,index)=>sendFileChannelPayload({dc,pcs,bytes:new ArrayBuffer(262144),file:{index,bytes:262144},queuePolicy:policy,signal:controller.signal}))));
 const settled=Promise.allSettled(sends);
 await Promise.all(sends.slice(1));assert.ok(aggregatePeak<=1048576);assert.equal(channels[0].frames,2);assert.equal(channels[1].frames,24);assert.equal(channels[2].frames,24);
 controller.abort();const results=await settled;assert.equal(results[0].status,'rejected');assert.ok(results.slice(1).every(r=>r.status==='fulfilled'));
});
