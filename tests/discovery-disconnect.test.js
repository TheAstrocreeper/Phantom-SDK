import test from 'node:test';
import assert from 'node:assert/strict';
import {createPhantom} from '../packages/phantom-core/src/index.js';
import {createRelayMonitor} from '../demos/phantom-breach/src/relay-monitor.js';
import {createFileDistribution} from '../packages/phantom-core/src/files.js';

test('real core intentionally closes discovery, blocks later bootstrap, and remains alive',async()=>{
 const sockets=[];
 class Socket{static OPEN=1;static CONNECTING=0;constructor(){this.readyState=0;this.closed=false;sockets.push(this);queueMicrotask(()=>{this.readyState=1;this.onopen?.();});}send(){}close(){this.closed=true;this.readyState=3;this.onclose?.({code:1000,wasClean:true});}}
 const core=createPhantom({doorway:{protocol:'phantom-concierge-v1',url:'ws://test.invalid/',WebSocket:Socket},runtime:globalThis});
 try{await core.join({roomId:'a'.repeat(64),name:'Tester'});await new Promise(r=>setImmediate(r));const before=core.snapshot();assert.equal(sockets.length,1);core.disconnectDiscovery();assert.equal(sockets[0].closed,true);assert.equal(core.snapshot().bootstrap.doorwayState,'closed');assert.equal(core.snapshot().self.hKey,before.self.hKey);core.command('CORE_BOOTSTRAP',{});await new Promise(r=>setImmediate(r));assert.equal(sockets.length,1);core.disconnectDiscovery();assert.equal(sockets.length,1);}finally{core.dispose();}
});

test('relay monitor waits for socket close and forbids every later constructor',async()=>{
 class Socket extends EventTarget{static CLOSED=3;constructor(){super();this.readyState=1;}close(){this.readyState=3;this.dispatchEvent(new Event('close'));}}
 const m=createRelayMonitor(Socket);new m.WebSocket('wss://one');new m.WebSocket('wss://two');assert.equal(m.count(),2);await m.disconnect();assert.equal(m.count(),0);assert.throws(()=>new m.WebSocket('wss://three'),/deliberately disconnected/);
});

test('actual SDK file service transfers and verifies bytes while simulated signaling is unavailable',async()=>{
 // This is a file-service integration test, not a native WebRTC/NAT test.
 const ids=['a'.repeat(16),'b'.repeat(16)],listeners=[new Set(),new Set()],events=[[],[]];let signaling=false;
 const cores=ids.map((id,i)=>({onEvent(fn){listeners[i].add(fn);return()=>listeners[i].delete(fn);},command(type,p){assert.equal(signaling,false);const j=ids.indexOf(p.to);if(j<0)throw Error('Unknown peer');const deliver=e=>{for(const fn of listeners[j])fn(e);};if(type==='SEND_DM')queueMicrotask(()=>deliver({type:'INCOMING_MSG',payload:{from:id,...p}}));else if(type==='SEND_FILE_BIN_CHUNK'){queueMicrotask(()=>deliver({type:'FILE_BIN_CHUNK',payload:{from:id,file:p.file,bytes:p.bytes}}));queueMicrotask(()=>{for(const fn of listeners[i])fn({type:'FILE_BIN_CHUNK_SENT',payload:{to:p.to,file:p.file,ok:true}});});}else throw Error('Unexpected command');}}));
 let offerResolve;const offered=new Promise(r=>offerResolve=r);
 const services=cores.map((core,i)=>createFileDistribution({core,emit:(type,data)=>{events[i].push({type,data});if(i===1&&type==='fileOffer')offerResolve(data);}}));
 try{const bytes=crypto.getRandomValues(new Uint8Array(12000)),blob=new Blob([bytes]);const m=await services[0].publish(blob,{chunkSize:1024});await services[0].share(ids[1],m.id);await offered;const got=await services[1].request(m.id,{peers:[ids[0]]});assert.deepEqual(new Uint8Array(await got.arrayBuffer()),bytes);const progress=events[1].filter(e=>e.type==='fileProgress');assert.equal(progress.length,m.total);assert.deepEqual(progress.map(e=>e.data.index),Array.from({length:m.total},(_,i)=>i));assert.equal(progress.reduce((n,e)=>n+e.data.bytes,0),bytes.length);}finally{services.forEach(s=>s.close());}
});
