import test from 'node:test';
import assert from 'node:assert/strict';
import { createPhantom } from '../packages/phantom-core/src/index.js';
import { createLegacyDoorway } from '../packages/phantom-doorways/src/index.js';
class Socket {
  static CONNECTING=0; static OPEN=1; static CLOSING=2; static CLOSED=3;
  static instances=[];
  constructor(url) { this.url=url; this.readyState=0; this.sent=[]; Socket.instances.push(this); }
  send(data) { if(this.readyState!==1) throw new Error('closed'); this.sent.push(JSON.parse(data)); }
  close() { this.readyState=3; }
  open() { this.readyState=1; this.onopen?.(); }
  receive(message) { this.onmessage?.({data:JSON.stringify(message)}); }
}
function setup() { return createPhantom({doorway:createLegacyDoorway({url:'wss://example.test/',WebSocket:Socket})}); }
test('no Chrome, document or YouTube needed; supplied room and doorway',async()=>{
 const core=setup(); try {
 const identity=await core.join({roomId:'my-game-room',handle:'@astro'});
 const socket=Socket.instances.at(-1);
 assert.equal(new URL(socket.url).searchParams.get('v'),'my-game-room');
 assert.equal(new URL(socket.url).hostname,'example.test'); socket.open();
 assert.equal(socket.sent[0].type,'concierge_connect');
 assert.equal(socket.sent[0].from,identity.hKey);
 assert.equal(core.snapshot().videoId,'my-game-room');
 } finally {core.dispose();}
});
test('instances isolate identity, room and disposal',async()=>{
 const a=setup(),b=setup();try {
 await a.join({roomId:'a',handle:'@alpha'});await b.join({roomId:'b',handle:'@beta'});
 const sa=Socket.instances.at(-2),sb=Socket.instances.at(-1);sa.open();sb.open();
 assert.notEqual(a.snapshot().self.hKey,b.snapshot().self.hKey);
 a.dispose();assert.equal(sa.readyState,3);assert.equal(sb.readyState,1);assert.equal(b.snapshot().videoId,'b');
 } finally {a.dispose();b.dispose();}
});
test('disposed instances reject commands',async()=>{
 const core=setup();await core.join({roomId:'stable',handle:'@astro'});
 const socket=Socket.instances.at(-1);socket.open();core.dispose();
 assert.throws(()=>core.command('CORE_CTX',{roomId:'wrong'}),/disposed/);assert.equal(socket.readyState,3);
});
test('doorway roster cannot erase singleton authority',async()=>{
 const core=setup();try {
 const identity=await core.join({roomId:'birth',handle:'@astro'});
 const socket=Socket.instances.at(-1);socket.open();
 socket.receive({type:'concierge_route',videoId:'birth',epoch:1,syscon1:identity.hKey,syscon2:null,list:[{hKey:identity.hKey,handle:'@astro'}]});
 assert.equal(core.snapshot().concierge.syscon1,identity.hKey);
 socket.receive({type:'roster',videoId:'birth',list:[]});
 assert.equal(core.snapshot().concierge.syscon1,identity.hKey);
 }finally {core.dispose();}
});

test('browser timers retain their Window receiver through injected runtime', async () => {
 const runtime = { crypto: globalThis.crypto };
 for (const name of ['setTimeout','clearTimeout','setInterval','clearInterval']) {
   runtime[name] = function(...args) {
     assert.equal(this, globalThis, `${name} must retain the browser receiver`);
     return globalThis[name](...args);
   };
 }
 const core = createPhantom({runtime,doorway:createLegacyDoorway({url:'wss://example.test/',WebSocket:Socket})});
 try {
   await core.join({roomId:'browser-brand-check',handle:'@astro'});
   Socket.instances.at(-1).open();
 } finally {core.dispose();}
});

test('cutting local doorway closes its transport and prevents reopening', async () => {
 const {createLocalDemoDoorway}=await import('./fixtures/local-doorway.js');
 const doorway=createLocalDemoDoorway();
 const socket=new doorway.WebSocket('ws://local-demo.invalid/ws?v=cut-test&to=astro');
 await new Promise(resolve=>queueMicrotask(resolve));
 let close;
 socket.onclose=event=>{close=event;};
 doorway.cut();
 assert.equal(socket.readyState,3);
 assert.equal(close.code,1006);
 assert.equal(doorway.offline,true);
 assert.throws(()=>new doorway.WebSocket('ws://local-demo.invalid/ws?v=cut-test&to=astro'),/offline/);
});

test('local fixture advances its topology epoch on leader departure', async () => {
 const {createLocalDemoDoorway}=await import('./fixtures/local-doorway.js');
 const doorway=createLocalDemoDoorway(),socket=new doorway.WebSocket('ws://local-demo.invalid/ws?v=epoch-test&to=a');
 await new Promise(resolve=>queueMicrotask(resolve));
 const routes=[];socket.onmessage=({data})=>{const msg=JSON.parse(data);if(msg.type==='concierge_route')routes.push(msg);};
 try {
 socket.send(JSON.stringify({type:'concierge_connect',handle:'@a',bootId:'a'}));
 socket.receive({kind:'presence',row:{hKey:'older',roomJoinedAt:1}});
 const before=routes.at(-1);assert.equal(before.syscon1,'older');
 socket.receive({kind:'leave',hKey:'older'});
 const after=routes.at(-1);assert.equal(after.syscon1,'a');assert.ok(after.epoch>before.epoch);
 } finally {socket.close();}
});
test('same-target epoch refresh preserves an in-flight SYS datachannel',async()=>{
 const channels=[];
 class RTC {
  signalingState='stable';connectionState='new';iceConnectionState='new';
  createDataChannel(){const dc={readyState:'connecting',close(){this.readyState='closed'},send(){}};channels.push(dc);return dc;}
  async createOffer(){return {type:'offer',sdp:'test'};}
  async setLocalDescription(value){this.localDescription=value;this.signalingState='have-local-offer';}
  close(){this.signalingState='closed';}
 }
 const core=createPhantom({doorway:createLegacyDoorway({url:'wss://example.test/',WebSocket:Socket}),runtime:{crypto:globalThis.crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval}});
 try{
  const self=await core.join({roomId:'epoch-refresh',name:'Astro'});
  const other='bbbbbbbbbbbbbbbb';const socket=Socket.instances.at(-1);socket.open();
  const route=epoch=>({type:'concierge_route',videoId:'epoch-refresh',epoch,syscon1:self.hKey,syscon2:other,list:[{hKey:self.hKey,handle:'Astro'},{hKey:other,handle:'Astra'}]});
  socket.receive(route(2));for(let i=0;i<12;i++)await Promise.resolve();
  assert.ok(channels.length>0,'SYS setup was started');const dc=channels.at(-1);
  socket.receive(route(3));for(let i=0;i<12;i++)await Promise.resolve();
  assert.equal(dc.readyState,'connecting','same-target refresh must not close SCTP channel');
 }finally{core.dispose();}
});
