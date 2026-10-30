import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {createHelperProbeAdapter} from '../src/browser-helper.js';
import {createBulkProbeControl} from '../src/control.js';
import {bulkNetworkFixture} from '../../../tests/fixtures/bulk-network.js';
import {helperPort} from './helper-test-port.mjs';
const nonce=()=>randomBytes(16).toString('hex');
async function until(fn){for(let n=0;n<1000;n++){if(fn())return;await new Promise(r=>setTimeout(r,5));}assert.fail('Helper did not settle');}
function rpc(port,body){const id=nonce();return new Promise(resolve=>{const off=port.subscribe(value=>{if(value.id===id){off();resolve(value);}});port.send({...body,id});});}
function scope(jobId=nonce()){return {action:'prepare',jobId,localPeer:'a'.repeat(16),remotePeer:'b'.repeat(16),sessionId:nonce(),bytes:1024,initiator:true};}

test('SDK negotiates two native messaging hosts and verifies 32 MiB without bridge payload bytes',{timeout:30000},async t=>{
 const f=await bulkNetworkFixture();const pa=helperPort(),pb=helperPort();t.after(()=>{pa.kill();pb.kill();f.close();});
 await until(()=>f.networks.every(n=>n.getPeerConnections()[0]?.state==='ready'));
 const aa=createHelperProbeAdapter({port:pa}),ba=createHelperProbeAdapter({port:pb});
 let arrived;const incoming=new Promise(resolve=>{arrived=resolve;});
 const ac=createBulkProbeControl({network:f.a,native:aa}),bc=createBulkProbeControl({network:f.b,native:ba,allowIncoming:()=>true,onTunnel:(peer,tunnel)=>arrived(tunnel)});
 try{
  const source=await ac.open(f.b.getState().peerId),sink=await incoming;
  const [sent,received]=await Promise.all([source.result,sink.result]);assert.equal(sent.bytes,32*1024*1024);assert.equal(sent.sha256,received.sha256);
  assert(pa.frames.every(frame=>!('data' in frame)&&!('payload' in frame)));assert(!JSON.stringify(f.events).includes('certificateDer'));
 }finally{ac.close();bc.close();aa.close();ba.close();}
 assert.equal(await pa.done,0);assert.equal(await pb.done,0);
});

test('native host rejects a caller other than the pinned extension',{timeout:5000},async()=>{
 const port=helperPort({origin:'chrome-extension://'+ 'a'.repeat(32)+'/'});assert.equal(await port.done,2);assert.equal(port.frames.length,0);
});

test('native host bounds scope, rejects extra commands, caps jobs and forbids job replay',{timeout:10000},async t=>{
 const port=helperPort();t.after(()=>port.kill());
 assert.equal((await rpc(port,{...scope(),bytes:32*1024*1024+1})).error,'invalid_scope');
 assert.equal((await rpc(port,{...scope(),command:'sh'})).error,'invalid_request');
 const jobs=Array.from({length:4},()=>scope());for(const job of jobs)assert.equal((await rpc(port,job)).ok,true);
 assert.equal((await rpc(port,scope())).error,'busy');
 for(const job of jobs)assert.equal((await rpc(port,{action:'close',jobId:job.jobId})).ok,true);
 assert.equal((await rpc(port,jobs[0])).error,'busy');port.close();assert.equal(await port.done,0);
});

test('EOF cancels an in-progress native connection and framing is bounded',{timeout:5000},async t=>{
 const port=helperPort();t.after(()=>port.kill());const request=scope();const reply=await rpc(port,request);
 const remote={...reply.offer,from:request.remotePeer,to:request.localPeer};
 port.send({action:'connect',id:nonce(),jobId:request.jobId,offer:remote});
 port.close();assert.equal(await port.done,0);
 const malformed=helperPort();t.after(()=>malformed.kill());malformed.child.stdin.write(Buffer.from([255,255,255,127]));assert.equal(await malformed.done,0);assert.equal(malformed.frames.length,0);
});
