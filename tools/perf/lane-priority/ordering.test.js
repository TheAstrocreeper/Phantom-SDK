import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createStripedRTC} from '../../../packages/phantom-core/src/striped-distribution-rtc.js';
import {createLaneScheduler} from './scheduler.js';
class FakeRTC {
 constructor(){this.connectionState='connected';this.sent=[];}
 createDataChannel(label){this.dc={label,readyState:'connecting',bufferedAmount:0,send:data=>{this.sent.push(data.buffer.slice(0));this.dc.bufferedAmount+=data.byteLength;},close:()=>{this.dc.readyState='closed';}};return this.dc;}
 close(){this.connectionState='closed';}
}
test('withholding first frame blocks real adapter while independent assignments can complete',()=>{
 const RTC=createStripedRTC(FakeRTC),sender=new RTC(),receiver=new RTC();
 const send=sender.createDataChannel('phantom-distribution'),recv=receiver.createDataChannel('phantom-distribution');
 for(const side of [sender,receiver])for(const pc of side.pcs){pc.dc.readyState='open';pc.dc.onopen();}
 const delivered=[];recv.onmessage=e=>delivered.push(e.data);
 send.send('zero');send.send('one');send.send('two');
 const frames=sender.pcs.flatMap(p=>p.sent).sort((a,b)=>Number(new DataView(a).getBigUint64(0)-new DataView(b).getBigUint64(0)));
 receiver.receive(frames[1]);receiver.receive(frames[2]);assert.deepEqual(delivered,[]);
 const model=createLaneScheduler({rangeCount:3}),a=model.next(0),b=model.next(1),c=model.next(2);
 assert.equal(model.finish(1,b),true);assert.equal(model.finish(2,c),true);assert.equal(model.snapshot().complete,2);
 receiver.receive(frames[0]);assert.deepEqual(delivered,['zero','one','two']);assert.equal(model.finish(0,a),true);
});
