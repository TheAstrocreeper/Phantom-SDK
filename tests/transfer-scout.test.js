import test from 'node:test';
import assert from 'node:assert/strict';
import {createTransferScout,selectTransferSetup,screenTransferCandidates} from '../packages/phantom-core/src/transfer-scout.js';
import {memoryRTC} from './fixtures/memory-rtc.js';
import {scoutClock} from './fixtures/scout-clock.js';
const ids=['1111111111111111','2222222222222222'];
const sample=(lanes,bytes,elapsedMs,stage,midpointMs=elapsedMs/2)=>({lanes:[...lanes],connections:lanes.length,bytes,elapsedMs,stage,delivery:[{bytes:0,elapsedMs:0},{bytes:bytes/2,elapsedMs:midpointMs},{bytes,elapsedMs}]});
const candidates=[[0],[1],[2],[0,1],[0,2],[1,2],[0,1,2]],key=lanes=>lanes.join(',');
function samples(winner=[0],screenTimes={},confirmTimes={}){
 const finalists=[1,2,3].map(count=>winner.length===count?winner:candidates.find(lanes=>lanes.length===count));
 return [
  ...candidates.flatMap((lanes,i)=>(screenTimes[key(lanes)]??(key(lanes)===key(winner)?[10,11]:[40+10*i,41+10*i])).map(ms=>sample(lanes,786432,ms,'screen'))),
  ...finalists.flatMap((lanes,i)=>(confirmTimes[key(lanes)]??(key(lanes)===key(winner)?[40,44]:[160+20*i,164+20*i])).map(ms=>sample(lanes,3145728,ms,'confirm'))),
 ];
}
test('one, two or three connections require a confirmed exact lane set',()=>{
 for(const lanes of candidates){const picked=selectTransferSetup(samples(lanes));assert.equal(picked.status,'measured');assert.equal(picked.connections,lanes.length);assert.deepEqual(picked.lanes,lanes);assert.deepEqual(picked.lateDelivery.lanes,lanes);}
 assert.equal(selectTransferSetup(samples([0],{'0':[40,41],'0,1':[40,41]})).reason,'screen_ranges_overlap');
 assert.equal(selectTransferSetup(samples([0],{}, {'0':[200,201],'0,1':[40,44]})).reason,'winner_changed_with_sample_size');
 assert.equal(selectTransferSetup(samples([0],{}, {'0':[120,170],'0,1':[150,180]})).reason,'confirmation_ranges_overlap');
 for(const input of [[],samples().slice(0,14),samples().map(s=>({...s,elapsedMs:0})),samples().map((s,i)=>({...s,bytes:s.bytes+i})),samples().map(s=>({...s,lanes:[0,0]}))])assert.equal(selectTransferSetup(input).status,'inconclusive');
});
test('screening ranks repetitions conservatively and does not call a tied finalist a winner',()=>{
 const rows=samples([1,2]).filter(s=>s.stage==='screen');assert.deepEqual(screenTransferCandidates(rows),[[0],[1,2],[0,1,2]]);
 const tied=samples([0],{'0':[10,11],'1':[10,11]});assert.deepEqual(screenTransferCandidates(tied.filter(s=>s.stage==='screen'))[0],[0]);
 assert.equal(selectTransferSetup(tied).status,'inconclusive');assert.deepEqual(selectTransferSetup(tied).lanes,[0,1,2]);
 const malformed=samples([0]);malformed.at(-1).lanes=[1,2];assert.equal(selectTransferSetup(malformed).reason,'insufficient_comparable_samples');
});
test('a burst advantage cannot hide slower late delivery or missing progress evidence',()=>{
 const slowTail=samples();for(const s of slowTail.filter(s=>s.stage==='confirm'))s.delivery[1].elapsedMs=key(s.lanes)==='0'?1:s.elapsedMs-10;
 assert.equal(selectTransferSetup(slowTail).reason,'late_delivery_does_not_confirm_winner');
 const absent=samples();delete absent[14].delivery;assert.equal(selectTransferSetup(absent).reason,'late_delivery_unavailable');
 const reordered=samples();reordered[14].delivery[1].elapsedMs=999;assert.equal(selectTransferSetup(reordered).status,'inconclusive');
});
function fixture({drop=false,maxMessageSize=262144,sampleBytes=98304,queueModel=false,freezeDrain=false}={}){
 const frames=[],reports=[],pcs=[],payloads=[],peaks=[],messages=[],channelRound=new Map();
 const upgrade=ch=>{if(ch.addEventListener)return;const events=new EventTarget();ch.addEventListener=events.addEventListener.bind(events);ch.removeEventListener=events.removeEventListener.bind(events);ch.dispatchEvent=events.dispatchEvent.bind(events);};
 const Base=memoryRTC({maxMessageSize,onSend(data,ch){
  if(queueModel){upgrade(ch);const n=typeof data==='string'?data.length:data.byteLength;ch.bufferedAmount+=n;peaks.push(ch.bufferedAmount);queueMicrotask(()=>{if(freezeDrain)return;const before=ch.bufferedAmount;ch.bufferedAmount-=n;if(before>(ch.bufferedAmountLowThreshold??0)&&ch.bufferedAmount<=(ch.bufferedAmountLowThreshold??0))ch.dispatchEvent(new Event('bufferedamountlow'));});}
  if(typeof data==='string'){const body=JSON.parse(data),lane=pcs.findIndex(pc=>pc.channels.includes(ch))%3;messages.push({lane,...body});if(body.action==='sample')channelRound.set(ch,body.round);}
  if(data instanceof ArrayBuffer){payloads.push({bytes:data.byteLength,label:ch.label,lane:pcs.findIndex(pc=>pc.channels.includes(ch))%3,round:channelRound.get(ch)});if(drop)return false;}return true;
 }});
 class RTC extends Base{constructor(){super();pcs.push(this);}fire(type,event={}){if(queueModel&&type==='datachannel')upgrade(event.channel);super.fire(type,event);}}
 const nodes=ids.map((id,i)=>{
  let serial=0;
  const clock=queueModel?performance:scoutClock({maxMessageSize,sampleBytes});
  return createTransferScout({self:()=>id,allowed:p=>p===ids[1-i],runtime:{crypto,RTCPeerConnection:RTC,performance:clock},
   send(peer,body){frames.push({from:id,peer,body});queueMicrotask(()=>nodes[1-i].receive(id,body));},newId:()=> `scout-${i}-${serial++}-0`,sampleBytes,onReport:r=>{clock.report?.(r);reports.push({i,...r});}});
 });
 return {nodes,frames,reports,pcs,payloads,peaks,messages,close:()=>nodes.forEach(n=>n.close())};
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
   assert.equal(report.samples.length,20);assert.deepEqual(report.samples.map(s=>s.bytes),[...Array(14).fill(98304),...Array(6).fill(393216)]);assert.ok(report.samples.every(s=>s.delivery.length>=3));
   assert.equal(f.reports.filter(r=>r.i===i&&r.phase==='preflight-sample'&&r.warmup).length,1);
  }
  assert.equal(f.pcs.length,6);assert.ok(f.pcs.every(pc=>pc.connectionState==='connected'));
  assert.equal(f.payloads.reduce((n,p)=>n+p.bytes,0),78*98304);assert.ok(f.payloads.every(p=>p.bytes<=16384));
  assert.equal(f.nodes[0].start(ids[1]),false,'does not loop or repeat a completed scout');
 }finally{globalThis.setTimeout=original;f.close();}
});
test('larger confirmations stream through bounded native queues using drain events and no timers',async()=>{
 const f=fixture({sampleBytes:786432,queueModel:true}),original=globalThis.setTimeout;
 try{
  globalThis.setTimeout=()=>{throw Error('unexpected timer');};f.nodes[0].start(ids[1]);
  await settle(()=>f.frames.some(f=>f.body.action==='complete'));
  assert.ok(Math.max(...f.peaks)<=786432);assert.ok(f.pcs.some(pc=>pc.channels.some(dc=>dc.bufferedAmountLowThreshold===393216))); assert.equal(f.payloads.reduce((n,p)=>n+p.bytes,0),78*786432);
  for(const [i,node] of f.nodes.entries()){
   const result=node.get(ids[1-i]);assert.equal(result.samples.filter(s=>s.stage==='confirm').length,6);
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
  const init={type:'transferScout',version:4,id:'foreign',action:'init',sampleBytes:98304,reverse:false,oneWay:false};
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
test('older control packets cannot start a v4 byte schedule',()=>{
 const f=fixture();try{
  for(const version of [1,2,3])assert.equal(f.nodes[1].receive(ids[0],{type:'transferScout',version,id:'old-peer',action:'init',sampleBytes:98304,reverse:false,oneWay:false}),false);
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

test('receiver summaries preserve each lane and bounded local arrival gaps in retained completion',async()=>{
 const f=fixture({maxMessageSize:4096});try{
  f.nodes[0].start(ids[1]);await settle(()=>f.frames.some(f=>f.body.action==='complete'));
  for(const i of [0,1]){
   const completed=f.reports.find(r=>r.i===i&&r.phase==='preflight'&&r.status==='complete');
   assert.equal(completed.incomingSamples.length,21);
   for(const sample of completed.incomingSamples){
    assert.equal(sample.clock,'receiver_monotonic');assert.equal(sample.complete,true);
    assert.equal(sample.lanes.length,sample.connections);
    assert.equal(sample.lanes.reduce((n,l)=>n+l.bytes,0),sample.bytes);
    for(const lane of sample.lanes){
     assert.equal(lane.bytes,sample.perLane);assert.equal(lane.messages,sample.perLane/4096);
     assert.equal(lane.completeAtMs,lane.lastAtMs);assert.ok(lane.firstAtMs<=lane.lastAtMs);
     assert.ok(lane.largestGaps.length<=4);assert.equal(lane.maxGapMs,lane.largestGaps[0].gapMs);
     assert.ok(lane.largestGaps.every(g=>g.gapMs===g.toMs-g.fromMs&&g.afterBytes>0));
    }
   }
   const result=f.nodes[i].get(ids[1-i]);
   for(const sample of result.samples){
    assert.equal(sample.clock,'sender_monotonic');assert.equal(sample.queues.length,sample.connections);
    assert.equal(sample.queues.reduce((n,q)=>n+q.payloadBytes,0),sample.bytes);
    assert.ok(sample.queues.every(q=>q.enqueuedAtMs<=sample.elapsedMs&&q.waitingSinceMs===null));
   }
  }
 }finally{f.close();}
});
test('cancelling preserves unfinished send and receive evidence without promoting it to a selection',async()=>{
 const f=fixture({drop:true});try{
  f.nodes[0].start(ids[1]);await settle(()=>f.payloads.length>0);f.nodes[0].suspend();
  await settle(()=>f.reports.some(r=>r.i===1&&r.phase==='preflight'&&r.status==='cancelled'));
  const outgoing=f.reports.find(r=>r.i===0&&r.reason==='file_selected');
  const incoming=f.reports.find(r=>r.i===1&&r.phase==='preflight'&&r.status==='cancelled');
  assert.equal(outgoing.partial.outgoing.round,0);assert.equal(outgoing.samples.length,0);
  assert.ok(outgoing.partial.outgoing.queues.some(q=>q.payloadBytes>0));
  assert.equal(incoming.partial.incoming.complete,false);assert.equal(incoming.partial.incoming.lanes.reduce((n,l)=>n+l.bytes,0),0);
  assert.equal(f.nodes[0].get(ids[1]),null);
  const captured=structuredClone(outgoing.partial);await Promise.resolve();assert.deepEqual(outgoing.partial,captured);
 }finally{f.close();}
});

test('the intended sender can own a one-way scout regardless of peer ID and retain its associations',async()=>{
 const f=fixture();try{
  const result=await f.nodes[1].forSend(ids[0]);await settle(()=>f.reports.filter(r=>r.phase==='preflight'&&r.status==='complete').length===2);
  assert.equal(result.direction,'outgoing');assert.equal(result.samples.length,20);assert.equal(result.payloadBudgetBytes,39*98304);
  assert.equal(f.payloads.reduce((n,p)=>n+p.bytes,0),39*98304);
  assert.equal(f.nodes[0].get(ids[1]),null,'the receiver did not pretend to measure its outgoing direction');
  assert.equal(f.reports.filter(r=>r.i===0&&r.phase==='preflight-sample').length,0);
  assert.equal(f.pcs.length,6);assert.ok(f.pcs.every(pc=>pc.connectionState==='connected'));
  const source=f.nodes[1].take(ids[0],result.id,0,'asset-A');
  const target=f.nodes[0].take(ids[1],result.id,0,'asset-A');
  assert.equal(source.pc.other,target.pc);assert.equal(f.nodes[1].take(ids[0],result.id,0,'asset-A'),null);
  assert.equal(f.nodes[1].take(ids[0],result.id,1,'asset-B'),null,'a pool cannot be borrowed by another asset');
  assert.equal(f.nodes[1].get(ids[0]).transportAvailable,false);
  source.pc.close();
 }finally{f.close();}
});
test('sender intent replaces unfinished background exploration and skip releases the wait',async()=>{
 const f=fixture({drop:true});try{
  f.nodes[0].start(ids[1]);await settle(()=>f.payloads.length>0);
  const waiting=f.nodes[1].forSend(ids[0]);await settle(()=>f.reports.some(r=>r.i===1&&r.phase==='preflight'&&r.intent==='send'));
  f.nodes[1].suspend('send_without_waiting');assert.equal(await waiting,null);
  await settle(()=>f.pcs.every(pc=>pc.connectionState==='closed'));
  assert.equal(f.nodes[1].get(ids[0]),null);
 }finally{f.close();}
});
test('an unused retained pool is released on generation invalidation on both peers',async()=>{
 const f=fixture();try{
  await f.nodes[1].forSend(ids[0]);await settle(()=>f.reports.filter(r=>r.phase==='preflight'&&r.status==='complete').length===2);
  f.nodes[1].clear();await settle(()=>f.pcs.every(pc=>pc.connectionState==='closed'));
  assert.equal(f.nodes[0].take(ids[1],null,0,'asset'),null);
 }finally{f.close();}
});
test('screening rotates all seven exact lane sets and acknowledgments use a participating lane',async()=>{
 const f=fixture();try{
  await f.nodes[0].forSend(ids[1]);await settle(()=>f.reports.filter(r=>r.phase==='preflight'&&r.status==='complete').length===2);
  const rounds=f.reports.filter(r=>r.i===0&&r.phase==='preflight-sample'&&r.stage==='screen');
  assert.equal(rounds.length,14);
  assert.deepEqual(rounds.slice(0,7).map(r=>r.lanes),rounds.slice(7).reverse().map(r=>r.lanes));
  for(const lanes of candidates){
   const repeated=rounds.filter(r=>key(r.lanes)===key(lanes));assert.equal(repeated.length,2);
   for(const sample of repeated){
    assert.deepEqual([...new Set(f.payloads.filter(p=>p.round===sample.round).map(p=>p.lane))].sort(),lanes);
    const ack=f.messages.find(m=>m.action==='ack'&&m.round===sample.round);assert.equal(ack.lane,lanes[0]);
    assert.equal(f.payloads.filter(p=>p.round===sample.round).reduce((n,p)=>n+p.bytes,0),98304);
   }
  }
 }finally{f.close();}
});
