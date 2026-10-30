import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {bulkNetworkFixture} from '../../../tests/fixtures/bulk-network.js';
import {createBulkProbeControl} from '../src/control.js';
import {createNativeProbeAdapter} from '../src/node-probe.js';
const binary=process.env.PHANTOM_NATIVE_PROBE??fileURLToPath(new URL('./target/debug/examples/native_probe'+(process.platform==='win32'?'.exe':''),import.meta.url));
async function until(fn){for(let n=0;n<1000;n++){if(fn())return;await new Promise(r=>setTimeout(r,5));}assert.fail('Native condition did not settle');}

test('SDK-controlled native workers stream 32 MiB; offers never surface as messages/log events', {timeout:30000}, async()=>{
 const f=await bulkNetworkFixture();let ac,bc;
 try{
  await until(()=>f.networks.every(n=>n.getPeerConnections()[0]?.state==='ready'));
  const from=f.a.getState().peerId,to=f.b.getState().peerId,bytes=32*1024*1024,states=[],messages=[];
  f.a.on('message',m=>messages.push(m));f.b.on('message',m=>messages.push(m));
  let received;const receiving=new Promise(resolve=>{received=resolve;});
  const native=createNativeProbeAdapter({binary,bindAddress:'127.0.0.1'});
  ac=createBulkProbeControl({network:f.a,native,onState:s=>states.push(s)});
  bc=createBulkProbeControl({network:f.b,native,allowIncoming:(peer,request)=>peer===from&&request.bytes===bytes,onTunnel:(peer,tunnel)=>received(tunnel),onState:s=>states.push(s)});
  const sender=await ac.open(to,{bytes}),receiver=await receiving;
  const [sent,got]=await Promise.all([sender.result,receiver.result]);
  assert.equal(sent.bytes,bytes);assert.equal(got.bytes,bytes);assert.equal(sent.sha256,got.sha256);
  const hash=createHash('sha256');for(let n=0;n<bytes;n+=65536)hash.update(Buffer.alloc(65536,0xa7));assert.equal(sent.sha256,hash.digest('hex'));
  const controls=f.packets.filter(p=>p.frame.t==='PHANTOM_BULK');
  assert.equal(controls.filter(p=>p.frame.frame.type==='request').length,1);
  assert.equal(controls.filter(p=>p.frame.frame.type==='offer').length,2);
  assert.deepEqual(messages,[]);
  assert(!JSON.stringify(f.events).includes('certificateDer'));
  assert(!JSON.stringify(states).includes('iceSdp'));
  await until(()=>ac.snapshot().length===0&&bc.snapshot().length===0);
 }finally{ac?.close();bc?.close();f.close();}
});

test('aborting the desktop native adapter settles setup rather than orphaning it', {timeout:5000}, async()=>{
 const native=createNativeProbeAdapter({binary,bindAddress:'127.0.0.1'}),abort=new AbortController();
 const pending=native.prepare({localPeer:'a'.repeat(16),remotePeer:'b'.repeat(16),sessionId:'ab'.repeat(16),bytes:1024,initiator:true,signal:abort.signal});
 abort.abort();await assert.rejects(pending,/Native probe failed/);
});
