import test from 'node:test';
import assert from 'node:assert/strict';
import {normalizeBulkControl} from '../packages/phantom-core/src/bulk-control.js';
import {createBulkProbeControl} from '../packages/phantom-tunnel/src/control.js';
import {bulkNetworkFixture} from './fixtures/bulk-network.js';
const wait=()=>new Promise(r=>setImmediate(r));
async function until(fn){for(let n=0;n<1000;n++){if(fn())return;await new Promise(r=>setTimeout(r,5));}assert.fail('Bulk condition did not settle');}
const id='ab'.repeat(16),a='a'.repeat(16),b='b'.repeat(16);
function offer(from,to,sessionId=id){return {protocol:'phantom-bulk-quic-v1',sessionId,from,to,certificateDer:'aa',iceSdp:'a=ice-pwd:SECRET_SENTINEL\r\na=candidate:test\r\n'};}
function request(sessionId=id){return {version:1,type:'request',sessionId,purpose:'probe',probeVersion:2,bytes:1024};}
function frame(from,to,sessionId=id){return {version:1,type:'offer',sessionId,offer:offer(from,to,sessionId)};}
function deferred(){let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});promise.catch(()=>{});return {promise,resolve,reject};}
function fixture(){
 const networks=new Map(),sent=[];
 function network(local,remote){const listeners=new Map(),bulk=new Set();let ready=true;
  const n={getState:()=>({peerId:local}),getPeerConnections:()=>ready?[{peerId:remote,state:'ready'}]:[],
   on(t,fn){if(!listeners.has(t))listeners.set(t,new Set());listeners.get(t).add(fn);return()=>listeners.get(t).delete(fn);},
   onBulkControl(fn){bulk.add(fn);return()=>bulk.delete(fn);},
   sendBulkControl(peer,body){if(!ready)return false;sent.push({from:local,to:peer,body});queueMicrotask(()=>networks.get(peer)?.receive(local,body));return true;},
   receive(peer,body){for(const fn of bulk)fn(peer,body);},
   emit(t,body){for(const fn of [...listeners.get(t)??[]])fn(body);},
   revoke(){ready=false;n.emit('peerConnection',{peerId:remote,state:'blocked'});},
   close(){n.emit('close',{});},
  };networks.set(local,n);return n;
 }
 const na=network(a,b),nb=network(b,a);return {na,nb,sent};
}
function adapter(){const handles=[];return {handles,async prepare(scope){
  const result=deferred();const handle={scope,offer:offer(scope.localPeer,scope.remotePeer,scope.sessionId),result,closed:0,connections:0,
   async connect(remote){handle.connections++;return {close:()=>handle.closed++,result:result.promise};},close(){handle.closed++;}};
  handles.push(handle);return handle;
 }};}

test('wire validation scopes identities, rejects hidden fields and bounds secrets',()=>{
 assert(normalizeBulkControl(frame(a,b),a,b));
 assert.equal(normalizeBulkControl(frame(a,b),b,a),null);
 assert.equal(normalizeBulkControl({...request(),bytes:Infinity}),null);
 assert.equal(normalizeBulkControl({...request(),secret:'hidden'}),null);
 const oversized=frame(a,b);oversized.offer.iceSdp='x'.repeat(4096);assert.equal(normalizeBulkControl(oversized),null);
 const wrong=frame(a,b);wrong.offer.sessionId='cd'.repeat(16);assert.equal(normalizeBulkControl(wrong),null);
});

test('SDK routes private control on verified RTC, never chat or generic diagnostic events',async()=>{
 const f=await bulkNetworkFixture();try{
  await until(()=>f.networks.every(n=>n.getPeerConnections()[0]?.state==='ready'));
  const from=f.a.getState().peerId,to=f.b.getState().peerId,received=[],messages=[];
  f.b.onBulkControl((peer,body)=>received.push({peer,body}));f.b.on('message',m=>messages.push(m));
  assert.equal(f.a.sendBulkControl(to,frame(from,to)),true);await until(()=>received.length===1);
  assert.equal(received[0].peer,from);assert.equal(received[0].body.offer.iceSdp,offer(from,to).iceSdp);
  assert.deepEqual(messages,[]);assert(!JSON.stringify(f.events).includes('SECRET_SENTINEL'));
  assert.equal(f.a.sendBulkControl(to,frame(to,from)),false);
  f.denied.add(from);f.denied.add(to);
  assert.equal(f.a.sendBulkControl(to,request()),false);
  const channel=f.packets.find(p=>p.frame.t==='PHANTOM_BULK').channel;
  channel.send(JSON.stringify({t:'PHANTOM_BULK',frame:frame(from,to)}));await wait();assert.equal(received.length,1);
 }finally{f.close();}
});

test('manual/unaccepted peers cannot send or queue tunnel requests',async()=>{
 const f=await bulkNetworkFixture('manual');try{
  assert.equal(f.a.sendBulkControl(f.b.getState().peerId,request()),false);
  assert.equal(f.packets.some(p=>p.frame.t==='PHANTOM_BULK'),false);
 }finally{f.close();}
});

test('coordinator handles offer arriving during native prepare, duplicates and peer closure',async()=>{
 const f=fixture(),aa=adapter(),ba=adapter(),states=[];
 let release;const original=aa.prepare;aa.prepare=scope=>new Promise(resolve=>{release=()=>original(scope).then(resolve);});
 const ac=createBulkProbeControl({network:f.na,native:aa,onState:s=>states.push(s)});
 const bc=createBulkProbeControl({network:f.nb,native:ba,allowIncoming:()=>true});
 try{
  const ready=ac.open(b,{bytes:1024});await until(()=>ba.handles.length===1);
  assert.equal(aa.handles.length,0);release();const tunnel=await ready;
  await until(()=>ba.handles[0].connections===1);
  assert.equal(aa.handles[0].connections,1);
  f.na.receive(b,frame(b,a,aa.handles[0].scope.sessionId));await wait();assert.equal(aa.handles[0].connections,1);
  assert(!JSON.stringify(states).includes('SECRET_SENTINEL'));
  f.na.revoke();assert.equal(ac.snapshot().length,0);assert.equal(aa.handles[0].closed,1);
  assert(tunnel.result);
 }finally{ac.close();bc.close();}
});

test('incoming resources require approval; decline allocates no worker',async()=>{
 const f=fixture(),aa=adapter(),ba=adapter();
 const ac=createBulkProbeControl({network:f.na,native:aa});const bc=createBulkProbeControl({network:f.nb,native:ba});
 try{await assert.rejects(ac.open(b),/declined/);assert.equal(ba.handles.length,0);assert.equal(ac.snapshot().length,0);}
 finally{ac.close();bc.close();}
});

test('revocation during pending prepare closes the late handle and ignores stale callbacks',async()=>{
 const f=fixture(),late=deferred(),aa=adapter(),ba=adapter();let started=false;
 const ac=createBulkProbeControl({network:f.na,native:{prepare:scope=>{started=true;return late.promise;}}});
 const bc=createBulkProbeControl({network:f.nb,native:ba,allowIncoming:()=>true});
 try{
  const ready=ac.open(b);await until(()=>started);f.na.revoke();await assert.rejects(ready,/unavailable/);
  const handle=await aa.prepare({localPeer:a,remotePeer:b,sessionId:id});late.resolve(handle);await wait();
  assert.equal(handle.closed,1);assert.equal(handle.connections,0);assert.equal(ac.snapshot().length,0);
 }finally{ac.close();bc.close();}
});

test('setup deadline settles the request without retrying or leaking an approved worker',async()=>{
 const f=fixture(),aa=adapter();const ac=createBulkProbeControl({network:f.na,native:aa,setupMs:25});
 try{
  const hold=setTimeout(()=>{},1000);try{await assert.rejects(ac.open(b),/timeout/);}finally{clearTimeout(hold);}
  assert.equal(aa.handles.length,1);assert.equal(aa.handles[0].closed,1);assert.equal(ac.snapshot().length,0);
  assert.equal(f.sent.filter(p=>p.body.type==='request').length,1);
 }finally{ac.close();}
});

test('close settles setup; replay cannot allocate another incoming session',async()=>{
 const f=fixture(),ba=adapter();const bc=createBulkProbeControl({network:f.nb,native:ba,allowIncoming:()=>true});
 f.nb.receive(a,request());await until(()=>ba.handles.length===1);bc.dropPeer(a);
 f.nb.receive(a,request());await wait();assert.equal(ba.handles.length,1);
 bc.close();assert.equal(ba.handles[0].closed,1);
 const aa=adapter(),ac=createBulkProbeControl({network:f.na,native:aa});const pending=ac.open(b);f.na.close();await assert.rejects(pending,/closed/);ac.close();
});

test('revoking a peer while incoming approval is pending prevents native allocation',async()=>{
 const f=fixture(),approval=deferred(),native=adapter();let signal;
 const control=createBulkProbeControl({network:f.nb,native,allowIncoming:(peer,request)=>{signal=request.signal;return approval.promise;}});
 try{f.nb.receive(a,request());await until(()=>signal);f.nb.revoke();assert(signal.aborted);approval.resolve(true);await wait();assert.equal(native.handles.length,0);assert.equal(control.snapshot().length,0);}
 finally{control.close();}
});

test('simultaneous requests fail explicitly without automatic retries or orphan handles',async()=>{
 const f=fixture(),aa=adapter(),ba=adapter(),ac=createBulkProbeControl({network:f.na,native:aa,allowIncoming:()=>true}),bc=createBulkProbeControl({network:f.nb,native:ba,allowIncoming:()=>true});
 try{const results=await Promise.allSettled([ac.open(b),bc.open(a)]);assert(results.every(r=>r.status==='rejected'));await wait();assert.equal(ac.snapshot().length+bc.snapshot().length,0);assert(aa.handles.concat(ba.handles).every(h=>h.closed===1));assert.equal(f.sent.filter(p=>p.body.type==='request').length,2);}
 finally{ac.close();bc.close();}
});
