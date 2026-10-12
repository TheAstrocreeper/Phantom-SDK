import test from 'node:test';
import assert from 'node:assert/strict';
import { createDoorway, bridgeDoorway } from '../packages/phantom-doorways/src/index.js';
test('bridge preserves room, identity and complete native control packets', async()=>{
 let context, sent;
 const d=createDoorway({open(c){context=c;c.onOpen();return{send(p){sent=p;},close(){}};}});
 const b=bridgeDoorway(d);const s=new b.WebSocket('ws://phantom-adapter.invalid/ws?v=test&to=peer');
 let received;s.onmessage=e=>received=JSON.parse(e.data);
 const packet={type:'rtc_offer',relationshipCycleId:'cycle',sdp:{type:'offer',sdp:'native'},sysTxId:'tx'};
 s.onopen=()=>s.send(JSON.stringify(packet));
 await new Promise(resolve=>queueMicrotask(resolve));
 assert.equal(context.roomId,'test');assert.equal(context.peerId,'peer');assert.deepEqual(sent,packet);
 context.onPacket(packet);assert.deepEqual(received,packet);s.close();context.onPacket(packet);assert.equal(s.readyState,3);
});
test('closing before open cancels adapter creation',async()=>{
 let called=false;const b=bridgeDoorway(createDoorway({open(){called=true;}}));const s=new b.WebSocket('ws://x/ws?v=a&to=b');s.close();await new Promise(resolve=>queueMicrotask(resolve));assert.equal(called,false);
});
test('typed doorway separates announcements from signaling and normalizes discovery',async()=>{
 let c,announcement,signal;
 const b=bridgeDoorway(createDoorway({discovery:true,open(context){c=context;c.onOpen();return{announce:a=>announcement=a,reconcile(){},sendSignal:p=>signal=p,close(){}};}}));
 const s=new b.WebSocket('ws://x/ws?v=room&to=peer');let received;
 s.onopen=()=>s.send(JSON.stringify({type:'concierge_connect',videoId:'room',from:'peer',bootId:'boot',t:42}));s.onmessage=e=>received=JSON.parse(e.data);
 await new Promise(resolve=>queueMicrotask(resolve));
 assert.deepEqual(announcement,{roomId:'room',peerId:'peer',bootId:'boot',t:42});
 const packet={type:'rtc_ice',cand:{candidate:'native'},relationshipCycleId:'cycle'};s.send(JSON.stringify(packet));assert.deepEqual(signal,packet);
 c.onDiscovery({kind:'concierge_route',roomId:'room',epoch:2,leader:'peer',mirror:'other',peers:[]});assert.equal(received.syscon1,'peer');assert.equal(received.epoch,2);
 assert.throws(()=>c.onDiscovery({kind:'roster',roomId:'wrong',peers:[]}),/room mismatch/);s.close();
});

test('presence capability survives bridging without treating hints as live sockets',async()=>{
 const {createDoorway,bridgeDoorway,adaptWebSocketDoorway:adaptLegacyDoorway}=await import('../packages/phantom-doorways/src/interface.js');
 assert.equal(bridgeDoorway(createDoorway({open(){}})).presence,'discovery-hints');
 assert.equal(bridgeDoorway(createDoorway({presence:'live-sockets',open(){}})).presence,'live-sockets');
 assert.equal(adaptLegacyDoorway({protocol:'six7-concierge-v1',presence:'discovery-hints'}).presence,'discovery-hints');
 assert.equal(adaptLegacyDoorway({protocol:'six7-concierge-v1'}).presence,'live-sockets');
 assert.throws(()=>createDoorway({presence:'unknown',open(){}}));
});

test('bridge exposes publication receipts and fences callbacks after close',async()=>{
 let context;const b=bridgeDoorway(createDoorway({open(c){context=c;c.onOpen();return{send(){},close(){}};}}));
 const s=new b.WebSocket('ws://x/?v=room&to=peer');const receipts=[],states=[],errors=[];
 s.onpublication=r=>receipts.push(r);s.onstatus=r=>states.push(r);s.onerror=e=>errors.push(e);
 await new Promise(r=>queueMicrotask(r));
 const receipt={id:'packet',state:'rejected',reason:'blocked'};
 context.onPublication(receipt);context.onStatus({type:'open'});assert.deepEqual(receipts,[receipt]);
 s.close();context.onPublication(receipt);context.onStatus({type:'closed'});context.onError(new Error('late'));
 assert.equal(receipts.length,1);assert.equal(states.length,1);assert.equal(errors.length,0);
});

import {validateDoorway,DoorwayConfigurationError} from '../packages/phantom-doorways/src/interface.js';
test('doorway validation gives stable field and code before opening a service',()=>{
 let opened=false;
 assert.throws(()=>bridgeDoorway({protocol:'phantom-doorway-v2',presence:'unknown',open(){opened=true}}),e=>e instanceof DoorwayConfigurationError && e.code==='DOORWAY_PRESENCE' && e.field==='presence');
 assert.equal(opened,false);
 assert.throws(()=>validateDoorway({protocol:'phantom-doorway-v2'}),e=>e.code==='DOORWAY_OPEN_REQUIRED');
 assert.throws(()=>createDoorway({discovery:'yes',open(){}}),e=>e.code==='DOORWAY_DISCOVERY');
});
test('invalid typed session reports missing methods and releases its transport',async()=>{
 let closed=0,error;
 const adapter=createDoorway({discovery:true,open(){return {close(){closed++}}}});
 const {WebSocket}=bridgeDoorway(adapter);const socket=new WebSocket('ws://fixture/?v=room&to=peer');
 socket.onerror=e=>{error=e};
 await Promise.resolve();
 assert.equal(closed,1);assert.equal(socket.readyState,3);
 assert.equal(error.code,'DOORWAY_SESSION_METHODS');assert.match(error.message,/announce, reconcile, sendSignal/);
});
