import test from 'node:test';import assert from 'node:assert/strict';
import {Socket,RTC,rtc} from './fixtures/native-network.js';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createAdminIdentity,createMemoryChunkStore} from '../packages/phantom-core/src/index.js';
const runtime={crypto:globalThis.crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval};
function storage(){const data=new Map();return {getItem:k=>data.get(k)??null,setItem:(k,v)=>data.set(k,v)};}
async function until(fn,label,timeout=8000){const deadline=Date.now()+timeout;while(!fn()){if(Date.now()>deadline)throw Error(label+' timed out');await new Promise(r=>setTimeout(r,5));}}
test('actual SDK transfers binary chunks, propagates revocation over SYS, and retains retirement on rejoin',async()=>{
 const admin=await createAdminIdentity(),clients=[];
 function make(name,signer){const p={name,network:null,events:[],storage:storage()};p.sdk=createBrowserPhantom({doorways:[{type:'websocket',url:'wss://cf.fixture/'}],adminIdentity:signer,storage:p.storage,WebSocket:Socket,runtime,files:{chunkStore:createMemoryChunkStore(),requestTimeoutMs:1000}});p.options={name,timeoutMs:8000,onEvent:e=>{p.events.push(e);if(e.type==='VEIL_INVITE_RECEIVED')queueMicrotask(()=>p.network?.acceptPeer(e.payload.from));}};clients.push(p);return p;}
 const key=p=>p.network.getState().peerId;
 const ready=(a,b)=>a.network.snapshot()?.chatPairs?.some(r=>r.hKey===key(b)&&r.dcState==='open'&&r.chatState==='ready');
 async function connect(a,b){a.network.invitePeer(key(b));await until(()=>ready(a,b)&&ready(b,a),'chat');}
 try{
 const a=make('A',admin);a.network=await a.sdk.create(a.options);
 const b=make('B');b.network=await b.sdk.join(a.network.invite,b.options);
 const c=make('C',admin);c.network=await c.sdk.join(a.network.invite,c.options);
 await until(()=>clients.every(p=>p.network.getState().state==='admitted'),'membership');assert.equal(c.network.getState().role,'peer');
 await connect(a,b);await connect(a,c);await connect(b,c);
 const file=new Blob([Uint8Array.from({length:128*1024},(_,i)=>i%251)]);
 const manifest=await a.network.publishFile(file,{name:'patch.bin',chunkSize:4096,distributionId:'patch-42'});
 let offerB=false,offerC=false;b.network.on('fileOffer',()=>offerB=true);c.network.on('fileOffer',()=>offerC=true);
 await a.network.shareFile(key(b),manifest.id);await until(()=>offerB,'B manifest');await b.network.requestChunks(manifest.id,[0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30],{peers:[key(a)]});
 await b.network.shareFile(key(c),manifest.id);await until(()=>offerC,'C manifest');const sources=new Set();c.network.on('fileProgress',e=>sources.add(e.from));const received=await c.network.requestFile(manifest.id,{peers:[key(b),key(a)]});assert.deepEqual(new Uint8Array(await received.arrayBuffer()),new Uint8Array(await file.arrayBuffer()));assert.equal(sources.size,2);
 // Hold a binary send at backpressure, then revoke while it is waiting.
 const stalled=await a.network.publishFile(new Blob([new Uint8Array(4096)]),{distributionId:'patch-42'});
 offerC=false;await a.network.shareFile(key(c),stalled.id);await until(()=>offerC,'stalled manifest');
 const drains=new Set();for(const pc of rtc.values())for(const channel of pc.channels)if(channel.label==='six7'){
  channel.bufferedAmount=2*1024*1024;
  channel.addEventListener=(type,fn)=>{if(type==='bufferedamountlow')drains.add(fn);};
  channel.removeEventListener=(type,fn)=>drains.delete(fn);
 }
 const stalledDownload=c.network.requestFile(stalled.id,{peers:[key(a)]});const rejected=assert.rejects(stalledDownload,/revoked/);
 await until(()=>drains.size>0,'backpressure');
 const revoked=await c.network.revoke('patch-42');await until(()=>clients.every(p=>p.network.isRevoked('patch-42')),'revoke convergence');await rejected;
 for(const pc of rtc.values())for(const channel of pc.channels)if(channel.label==='six7')channel.bufferedAmount=0;
 for(const drain of [...drains])drain();
 await until(()=>a.events.some(e=>e.type==='FILE_BIN_CHUNK_SENT'&&e.payload.file.id===stalled.id),'cancelled send');
 assert.ok(a.events.filter(e=>e.type==='FILE_BIN_CHUNK_SENT'&&e.payload.file.id===stalled.id).every(e=>!e.payload.ok));
 await assert.rejects(a.network.shareFile(key(b),manifest.id),/revoked/);
 // Remove chat channels: retirement must still propagate over native SYS tethers.
 for(const pc of rtc.values())for(const channel of pc.channels)if(channel.label==='six7')channel.close();
 assert.ok([...rtc.values()].flatMap(pc=>pc.channels).filter(ch=>ch.label==='six7').every(ch=>ch.readyState==='closed'));
 assert.equal(await b.network.applyControl({...revoked,target:'patch-43'}),false);
 const invite=a.network.invite;const retirement=await c.network.retire();await until(()=>clients.every(p=>p.network.getState().state==='retired'),'retire convergence');assert.ok(retirement.signature);for(const p of clients)assert.throws(()=>p.network.send(key(a),'revive'),/retired/);
 const archive=c.network.getControls();for(const p of clients)p.network.close();
 const fresh=createBrowserPhantom({doorways:[{type:'websocket',url:'wss://cf.fixture/'}],storage:storage(),controlStore:{load:async()=>archive,append:async()=>{}},WebSocket:Socket,runtime});await assert.rejects(fresh.join(invite,{name:'Never connected',timeoutMs:1000}),/permanently retired/);
 await assert.rejects(b.sdk.join(invite,b.options),/permanently retired/);
 }finally{for(const p of clients)p.network?.close();}
});
