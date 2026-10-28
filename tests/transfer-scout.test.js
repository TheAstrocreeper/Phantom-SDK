import test from 'node:test';
import assert from 'node:assert/strict';
import {createTransferScout,selectTransferSetup} from '../packages/phantom-core/src/transfer-scout.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
const ids=['1111111111111111','2222222222222222'];
const samples=times=>[1,3,3,1].map((connections,i)=>({connections,bytes:786432,elapsedMs:times[i]}));
test('selection compares repeated equal-byte deliveries and preserves ambiguous defaults',()=>{
 assert.equal(selectTransferSetup(samples([10,30,31,11])).connections,1);
 assert.equal(selectTransferSetup(samples([30,10,11,31])).connections,3);
 assert.deepEqual(selectTransferSetup(samples([10,11,12,13])).status,'inconclusive');
 for(const input of [[],samples([0,10,10,1]),samples([1,2,2,1]).map((s,i)=>({...s,bytes:s.bytes+i}))])assert.equal(selectTransferSetup(input).status,'inconclusive');
});
function fixture({drop=false,maxMessageSize=262144}={}){
 const frames=[],reports=[],pcs=[],payloads=[];
 const Base=memoryRTC({maxMessageSize,onSend(data,ch){if(data instanceof ArrayBuffer){payloads.push({bytes:data.byteLength,label:ch.label});if(drop)return false;}return true;}});
 class RTC extends Base{constructor(){super();pcs.push(this);}}
 const nodes=ids.map((id,i)=>{
  let clock=0,calls=0;
  return createTransferScout({self:()=>id,allowed:p=>p===ids[1-i],runtime:{crypto,RTCPeerConnection:RTC,performance:{now(){const call=calls++;clock+=call%2?[5,10,30,31,11][Math.floor(call/2)]:1;return clock;}}},
   send(peer,body){frames.push({from:id,peer,body});queueMicrotask(()=>nodes[1-i].receive(id,body));},newId:()=> 'scout-0000',sampleBytes:3*32768,onReport:r=>reports.push({i,...r})});
 });
 return {nodes,frames,reports,pcs,payloads,close:()=>nodes.forEach(n=>n.close())};
}
async function settle(predicate){for(let i=0;i<1000;i++){if(predicate())return;await Promise.resolve();}assert.fail('scout did not settle');}
test('measures both directions on three real-style associations with a fixed byte budget and no timers',async()=>{
 const f=fixture({maxMessageSize:16384});const original=globalThis.setTimeout;
 try{
  globalThis.setTimeout=()=>{throw Error('scout added a timer');};
  assert.equal(f.nodes[1].start(ids[0]),false);assert.equal(f.nodes[0].start(ids[1]),true);
  await settle(()=>f.frames.some(f=>f.body.action==='complete'));
  for(const [i,node]of f.nodes.entries()){
   const report=node.get(ids[1-i]);assert.equal(report.connections,1);assert.equal(report.status,'measured');
   assert.equal(report.samples.length,4);assert.deepEqual(report.samples.map(s=>s.bytes),[98304,98304,98304,98304]);
   assert.equal(f.reports.filter(r=>r.i===i&&r.phase==='preflight-sample'&&r.warmup).length,1);
  }
  assert.equal(f.pcs.length,6);assert.ok(f.pcs.every(pc=>pc.connectionState==='closed'));
  assert.equal(f.payloads.reduce((n,p)=>n+p.bytes,0),10*98304);assert.ok(f.payloads.every(p=>p.bytes<=16384));
  assert.equal(f.nodes[0].start(ids[1]),false,'does not loop or repeat a completed scout');
 }finally{globalThis.setTimeout=original;f.close();}
});
test('a silent receiver stays byte-bounded and file selection cancels and cleans up',async()=>{
 const f=fixture({drop:true});try{
  f.nodes[0].start(ids[1]);await settle(()=>f.payloads.length>0);
  f.nodes[0].suspend();await settle(()=>f.pcs.every(pc=>pc.connectionState==='closed'));
  assert.equal(f.nodes[0].get(ids[1]),null);assert.equal(f.nodes[0].start(ids[1]),false);
  assert.ok(f.payloads.reduce((n,p)=>n+p.bytes,0)<=98304);
  assert.ok(f.reports.some(r=>r.phase==='preflight'&&r.reason==='file_selected'));
 }finally{f.close();}
});
test('foreign, duplicate, oversized and unadmitted control messages do not allocate extra connections',async()=>{
 const f=fixture({drop:true});try{
  const init={type:'transferScout',version:1,id:'foreign',action:'init',sampleBytes:98304,reverse:false};
  assert.equal(f.nodes[1].receive('3333333333333333',init),false);
  assert.equal(f.nodes[1].receive(ids[0],{...init,sampleBytes:4*1024*1024}),false);
  assert.equal(f.pcs.length,0);f.nodes[0].start(ids[1]);await settle(()=>f.pcs.length===6);
  assert.equal(f.nodes[1].receive(ids[0],init),false);assert.equal(f.pcs.length,6);
  f.nodes.forEach((n,i)=>n.drop(ids[1-i]));await settle(()=>f.pcs.every(pc=>pc.connectionState==='closed'));
  assert.equal(f.nodes[0].get(ids[1]),null);
 }finally{f.close();}
});
