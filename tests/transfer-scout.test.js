import test from 'node:test';
import assert from 'node:assert/strict';
import {createTransferScout,selectTransferSetup} from '../packages/phantom-core/src/transfer-scout.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
import {scoutClock} from './fixtures/scout-clock.js';
const ids=['1111111111111111','2222222222222222'];
const sample=(connections,bytes,elapsedMs,stage,midpointMs=elapsedMs/2)=>({connections,bytes,elapsedMs,stage,delivery:[{bytes:0,elapsedMs:0},{bytes:bytes/2,elapsedMs:midpointMs},{bytes,elapsedMs}]});
const samples=(small=[10,30,31,11],large=[120,40,44,124])=>[
 ...[1,3,3,1].map((c,i)=>sample(c,786432,small[i],'screen')),
 ...[3,1,1,3].map((c,i)=>sample(c,3145728,large[i],'confirm')),
];
test('a repeated winner must survive a fourfold payload increase and late delivery',()=>{
 const picked=selectTransferSetup(samples());assert.equal(picked.connections,1);assert.equal(picked.status,'measured');assert.equal(picked.lateDelivery.winner,1);
 assert.equal(selectTransferSetup(samples([30,10,11,31],[40,120,124,44])).connections,3);
 assert.equal(selectTransferSetup(samples([10,11,12,13])).reason,'screen_ranges_overlap');
 assert.equal(selectTransferSetup(samples([10,30,31,11],[40,120,124,44])).reason,'winner_changed_with_sample_size');
 assert.equal(selectTransferSetup(samples([10,30,31,11],[120,40,130,124])).reason,'confirmation_ranges_overlap');
 for(const input of [[],samples().slice(0,4),samples().map(s=>({...s,elapsedMs:0})),samples().map((s,i)=>({...s,bytes:s.bytes+i}))])assert.equal(selectTransferSetup(input).status,'inconclusive');
});
test('a burst advantage cannot hide slower late delivery or missing progress evidence',()=>{
 const slowTail=samples();for(const s of slowTail.filter(s=>s.stage==='confirm'))s.delivery[1].elapsedMs=s.connections===1?1:s.elapsedMs-10;
 assert.equal(selectTransferSetup(slowTail).reason,'late_delivery_does_not_confirm_winner');
 const absent=samples();delete absent[5].delivery;assert.equal(selectTransferSetup(absent).reason,'late_delivery_unavailable');
 const reordered=samples();reordered[5].delivery[1].elapsedMs=999;assert.equal(selectTransferSetup(reordered).status,'inconclusive');
});
function fixture({drop=false,maxMessageSize=262144,sampleBytes=98304,queueModel=false,freezeDrain=false}={}){
 const frames=[],reports=[],pcs=[],payloads=[],peaks=[];
 const upgrade=ch=>{if(ch.addEventListener)return;const events=new EventTarget();ch.addEventListener=events.addEventListener.bind(events);ch.removeEventListener=events.removeEventListener.bind(events);ch.dispatchEvent=events.dispatchEvent.bind(events);};
 const Base=memoryRTC({maxMessageSize,onSend(data,ch){
  if(queueModel){upgrade(ch);const n=typeof data==='string'?data.length:data.byteLength;ch.bufferedAmount+=n;peaks.push(ch.bufferedAmount);queueMicrotask(()=>{if(freezeDrain)return;const before=ch.bufferedAmount;ch.bufferedAmount-=n;if(before>(ch.bufferedAmountLowThreshold??0)&&ch.bufferedAmount<=(ch.bufferedAmountLowThreshold??0))ch.dispatchEvent(new Event('bufferedamountlow'));});}
  if(data instanceof ArrayBuffer){payloads.push({bytes:data.byteLength,label:ch.label});if(drop)return false;}return true;
 }});
 class RTC extends Base{constructor(){super();pcs.push(this);}fire(type,event={}){if(queueModel&&type==='datachannel')upgrade(event.channel);super.fire(type,event);}}
 const nodes=ids.map((id,i)=>{
  const clock=queueModel?performance:scoutClock({maxMessageSize,sampleBytes});
  return createTransferScout({self:()=>id,allowed:p=>p===ids[1-i],runtime:{crypto,RTCPeerConnection:RTC,performance:clock},
   send(peer,body){frames.push({from:id,peer,body});queueMicrotask(()=>nodes[1-i].receive(id,body));},newId:()=> 'scout-0000',sampleBytes,onReport:r=>{clock.report?.(r);reports.push({i,...r});}});
 });
 return {nodes,frames,reports,pcs,payloads,peaks,close:()=>nodes.forEach(n=>n.close())};
}
async function settle(predicate){for(let i=0;i<20000;i++){if(predicate())return;await Promise.resolve();}assert.fail('scout did not settle');}
test('measures both directions on three real-style associations with a fixed byte budget and no timers',async()=>{
 const f=fixture({maxMessageSize:16384});const original=globalThis.setTimeout;
 try{
  globalThis.setTimeout=()=>{throw Error('scout added a timer');};
  assert.equal(f.nodes[1].start(ids[0]),false);assert.equal(f.nodes[0].start(ids[1]),true);
  await settle(()=>f.frames.some(f=>f.body.action==='complete'));
  for(const [i,node]of f.nodes.entries()){
   const report=node.get(ids[1-i]);assert.equal(report.connections,1);assert.equal(report.status,'measured');
   assert.equal(report.samples.length,8);assert.deepEqual(report.samples.map(s=>s.bytes),[98304,98304,98304,98304,393216,393216,393216,393216]);assert.ok(report.samples.every(s=>s.delivery.length>=3));
   assert.equal(f.reports.filter(r=>r.i===i&&r.phase==='preflight-sample'&&r.warmup).length,1);
  }
  assert.equal(f.pcs.length,6);assert.ok(f.pcs.every(pc=>pc.connectionState==='closed'));
  assert.equal(f.payloads.reduce((n,p)=>n+p.bytes,0),42*98304);assert.ok(f.payloads.every(p=>p.bytes<=16384));
  assert.equal(f.nodes[0].start(ids[1]),false,'does not loop or repeat a completed scout');
 }finally{globalThis.setTimeout=original;f.close();}
});
test('larger confirmations stream through bounded native queues using drain events and no timers',async()=>{
 const f=fixture({sampleBytes:786432,queueModel:true}),original=globalThis.setTimeout;
 try{
  globalThis.setTimeout=()=>{throw Error('unexpected timer');};f.nodes[0].start(ids[1]);
  await settle(()=>f.frames.some(f=>f.body.action==='complete'));
  assert.ok(Math.max(...f.peaks)<=786432);assert.ok(f.pcs.some(pc=>pc.channels.some(dc=>dc.bufferedAmountLowThreshold===393216))); assert.equal(f.payloads.reduce((n,p)=>n+p.bytes,0),42*786432);
  for(const [i,node] of f.nodes.entries()){
   const result=node.get(ids[1-i]);assert.equal(result.samples.filter(s=>s.stage==='confirm').length,4);
   assert.ok(result.samples.filter(s=>s.stage==='confirm').every(s=>s.bytes===3145728&&s.delivery.at(-1).bytes===s.bytes));
  }
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
  const init={type:'transferScout',version:2,id:'foreign',action:'init',sampleBytes:98304,reverse:false};
  assert.equal(f.nodes[1].receive('3333333333333333',init),false);
  assert.equal(f.nodes[1].receive(ids[0],{...init,sampleBytes:4*1024*1024}),false);
  assert.equal(f.pcs.length,0);f.nodes[0].start(ids[1]);await settle(()=>f.pcs.length===6);
  assert.equal(f.nodes[1].receive(ids[0],init),false);assert.equal(f.pcs.length,6);
  f.nodes.forEach((n,i)=>n.drop(ids[1-i]));await settle(()=>f.pcs.every(pc=>pc.connectionState==='closed'));
  assert.equal(f.nodes[0].get(ids[1]),null);
 }finally{f.close();}
});

test('cancellation settles a confirmation waiting for native queue capacity',async()=>{
 const f=fixture({sampleBytes:786432,queueModel:true,freezeDrain:true});try{
  f.nodes[0].start(ids[1]);await settle(()=>f.pcs.some(pc=>pc.channels.some(dc=>dc.bufferedAmountLowThreshold===393216)));
  f.nodes[0].suspend();await settle(()=>f.pcs.every(pc=>pc.connectionState==='closed'));
  assert.equal(f.nodes[0].get(ids[1]),null);
 }finally{f.close();}
});
test('v1 control packets cannot start a v2 byte schedule',()=>{
 const f=fixture();try{
  assert.equal(f.nodes[1].receive(ids[0],{type:'transferScout',version:1,id:'old-peer',action:'init',sampleBytes:98304,reverse:false}),false);
  assert.equal(f.pcs.length,0);
 }finally{f.close();}
});
test('a peer that already selected a file declines explicitly without a hanging probe',async()=>{
 const f=fixture();try{
  f.nodes[1].suspend();f.nodes[0].start(ids[1]);
  await settle(()=>f.reports.some(r=>r.i===0&&r.reason==='peer_busy'));
  assert.ok(f.pcs.every(pc=>pc.connectionState==='closed'));assert.equal(f.nodes[0].get(ids[1]),null);
  assert.equal(f.nodes[0].start(ids[1]),false);
 }finally{f.close();}
});
