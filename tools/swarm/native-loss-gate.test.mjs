import test from 'node:test';
import assert from 'node:assert/strict';
import {createNativeLossGate} from './native-loss-gate.mjs';
class Channel extends EventTarget{
 constructor(label){super();this.label=label;this.state='open';this.sent=[];}
 get readyState(){return this.state;}
 send(data){if(this.state!=='open')throw Error('native closed');this.sent.push(data);}
 close(){this.state='closed';this.dispatchEvent(new Event('close'));}
}
class Native extends EventTarget{
 constructor(){super();this.state='connected';this.ice='connected';this.channels=[];}
 get connectionState(){return this.state;}
 get iceConnectionState(){return this.ice;}
 createDataChannel(label){const dc=new Channel(label);this.channels.push(dc);return dc;}
 fail(){this.state=this.ice='failed';for(const dc of this.channels)dc.close();this.dispatchEvent(new Event('connectionstatechange'));this.dispatchEvent(new Event('iceconnectionstatechange'));}
 close(){this.state=this.ice='closed';}
}
test('loss gate delays only local SYS observations and releases native terminal states',()=>{
 const gate=createNativeLossGate(Native,{systemChannel:'sys'}),pc=new gate.RTCPeerConnection(),dc=pc.createDataChannel('sys'),seen=[];
 pc.addEventListener('connectionstatechange',()=>seen.push(pc.connectionState));dc.addEventListener('close',()=>seen.push(dc.readyState));
 assert.equal(gate.hold().held,1);pc.fail();
 assert.equal(pc.state,'failed','native state still changes');assert.equal(pc.connectionState,'connected');assert.equal(dc.readyState,'open');assert.deepEqual(seen,[]);
 dc.send('held send');assert.deepEqual(dc.sent,[]);
 assert.equal(gate.release().released,1);assert.equal(pc.connectionState,'failed');assert.equal(dc.readyState,'closed');assert.deepEqual(seen,['closed','failed']);
 assert.throws(()=>dc.send('native send'),/native closed/);pc.close();assert.equal(gate.release().released,0);
});
test('loss gate refuses ambiguous SYS selection and ignores distribution-only connections',()=>{
 const gate=createNativeLossGate(Native,{systemChannel:'sys'}),distribution=new gate.RTCPeerConnection();distribution.createDataChannel('phantom-distribution');
 assert.throws(()=>gate.hold(),/found 0/);
 const a=new gate.RTCPeerConnection(),b=new gate.RTCPeerConnection();a.createDataChannel('sys');b.createDataChannel('sys');assert.throws(()=>gate.hold(),/found 2/);
 for(const pc of [distribution,a,b])pc.close();
});
