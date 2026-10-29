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
