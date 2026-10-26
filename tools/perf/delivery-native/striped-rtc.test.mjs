import test from 'node:test';
import assert from 'node:assert/strict';
import {createStripedRTC} from '../../../packages/phantom-core/src/striped-distribution-rtc.js';
class FakeRTC {
 constructor(){this.connectionState='connected';this.sent=[];}
 createDataChannel(label){this.dc={label,readyState:'connecting',bufferedAmount:0,send:data=>{this.sent.push(data.buffer.slice(0));this.dc.bufferedAmount+=data.byteLength;},close:()=>{this.dc.readyState='closed';}};return this.dc;}
 close(){this.connectionState='closed';}
}
function pair(options){const RTC=createStripedRTC(FakeRTC,options),a=new RTC(),b=new RTC(),sender=a.createDataChannel('phantom-distribution'),receiver=b.createDataChannel('phantom-distribution');for(const adapter of [a,b])for(const pc of adapter.pcs){pc.dc.readyState='open';pc.dc.onopen();}return {a,b,sender,receiver};}
test('out-of-order associations preserve text/binary ordering and exact bytes',()=>{
 const {a,b,sender,receiver}=pair(),events=[];receiver.onmessage=e=>events.push(e.data);
 sender.send('metadata');sender.send(new Uint8Array([7,19,255]).buffer);sender.send('next');
 assert.equal(a.pcs[0].dc.phantomStripe.count,3);assert.equal(a.pcs[2].dc.phantomStripe.index,2);assert.equal(a.pcs[0].dc.phantomStripe.group,a.pcs[2].dc.phantomStripe.group);
 const frames=a.pcs.flatMap(p=>p.sent).sort((x,y)=>Number(new DataView(x).getBigUint64(0)-new DataView(y).getBigUint64(0)));
 b.receive(frames[2]);assert.deepEqual(events,[]);b.receive(frames[0]);assert.deepEqual(events,['metadata']);b.receive(frames[1]);assert.equal(events[2],'next');assert.deepEqual([...new Uint8Array(events[1])],[7,19,255]);assert.equal(b.pendingBytes,0);
});
test('a missing lane cannot open the aggregate early',()=>{
 const RTC=createStripedRTC(FakeRTC),a=new RTC(),dc=a.createDataChannel('phantom-distribution');a.pcs[2].dc.readyState='open';a.pcs[2].dc.onopen();assert.equal(dc.readyState,'connecting');
});
test('malformed and duplicate frames close all associations and clear pending bytes',()=>{
 for(const duplicate of [false,true]){const {a,b,sender}=pair();sender.send('x');const frame=a.pcs.flatMap(p=>p.sent)[0];if(duplicate)b.receive(frame);b.receive(duplicate?frame:new ArrayBuffer(8));assert.equal(b.connectionState,'closed');assert.ok(b.pcs.every(p=>p.connectionState==='closed'));assert.equal(b.pendingBytes,0);}
});
test('reordering is bounded and a native lane failure closes the complete adapter',()=>{
 const {a,b,sender}=pair({lanes:3,maxReorderBytes:16});sender.send('a');sender.send('payload beyond reorder limit');const frame=a.pcs.flatMap(p=>p.sent).find(f=>new DataView(f).getBigUint64(0)===1n);b.receive(frame);assert.equal(b.connectionState,'closed');assert.equal(b.pendingBytes,0);
 a.pcs[1].connectionState='failed';a.pcs[1].onconnectionstatechange();assert.ok(a.pcs.every(p=>p.connectionState==='closed'));
});

test('independent chunks deliver a complete later chunk while an earlier lane is delayed',()=>{
 const {a,b,sender,receiver}=pair({delivery:'chunks'}),events=[];receiver.onmessage=e=>events.push(e.data);
 const send=(index,bytes)=>{sender.send(JSON.stringify({kind:'binary',file:{id:'test',index,bytes:bytes.length}}));sender.send(Uint8Array.from(bytes).buffer);};
 send(0,[1,2]);send(1,[3,4,5]);
 const frames=a.pcs.flatMap(p=>p.sent).sort((x,y)=>Number(new DataView(x).getBigUint64(0)-new DataView(y).getBigUint64(0)));
 assert.equal(frames.length,2);assert.equal(new DataView(frames[1]).getUint8(8),2);
 b.receive(frames[1]);assert.equal(JSON.parse(events[0]).file.index,1);assert.deepEqual([...new Uint8Array(events[1])],[3,4,5]);assert.equal(b.pendingBytes,0);
 b.receive(frames[0]);assert.equal(JSON.parse(events[2]).file.index,0);assert.deepEqual([...new Uint8Array(events[3])],[1,2]);assert.equal(b.pairReceived.size,0);
});
test('independent chunks reject replay, missing metadata and corrupt envelope lengths',()=>{
 const {a,b,sender}=pair({delivery:'chunks'});assert.throws(()=>sender.send(new ArrayBuffer(1)),/metadata/);sender.send(JSON.stringify({kind:'binary',file:{index:0,bytes:1}}));sender.send(new Uint8Array([7]).buffer);const frame=a.pcs.flatMap(p=>p.sent)[0];b.receive(frame);b.receive(frame);assert.equal(b.connectionState,'closed');
 const {b:other}=pair({delivery:'chunks'}),bad=frame.slice(0);new DataView(bad).setUint32(9,65536);other.receive(bad);assert.equal(other.connectionState,'closed');
});
