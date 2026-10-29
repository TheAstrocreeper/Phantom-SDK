import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const root=resolve(fileURLToPath(new URL('../../',import.meta.url)));
const require=createRequire(new URL('../swarm/package.json',import.meta.url)),{chromium}=require('playwright');
const server=createServer(async(req,res)=>{try{
 const path=new URL(req.url,'http://localhost').pathname;if(path==='/'){res.setHeader('content-type','text/html');res.end('<!doctype html>');return;}
 const target=resolve(root,'.'+path);if(!target.startsWith(root+sep)||!(path.startsWith('/packages/')||['/tests/fixtures/scout-clock.js','/tests/fixtures/scout-receiver-model.js'].includes(path))||!path.endsWith('.js')){res.writeHead(403).end();return;}
 res.setHeader('content-type','text/javascript');res.end(await readFile(target));
}catch{res.writeHead(404).end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));let browser;
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE,args:['--no-sandbox']});
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.stack??e.message));await page.goto(`http://127.0.0.1:${server.address().port}/`);
 for(const mode of ['complete','cancel','send','pair','baseline','queue','message','tradeoff','workload','repair','repeat','peer-budget','progressive']){
  const result=await page.evaluate(async mode=>{
   const {createNetworkDistribution}=await import('/packages/phantom-core/src/network-distribution.js');
   const {createAdminIdentity}=await import('/packages/phantom-core/src/authorization.js');
   const ids=['1111111111111111','2222222222222222','3333333333333333'],leader=ids[2],admin=await createAdminIdentity();
   const {scoutClock}=await import('/tests/fixtures/scout-clock.js');
   const {modelReceiverEvidence}=await import('/tests/fixtures/scout-receiver-model.js');
   const clocks=ids.map(()=>scoutClock({winner:mode==='tradeoff'?[]:mode==='baseline'?[0,1,2]:[1,2],queueWinner:mode==='queue',messageWinner:mode==='message',uniformProfile:mode==='tradeoff'}));
   const callbacks=new Map(),events=[],pcs=[],scouted=new Set(),filePCs=new Set();let peak=0,fileQueuePeak=0,filePayloadPeak=0;const source=mode==='send'?1:0,target=1-source;
   class RTC extends RTCPeerConnection{
    constructor(config){super(config);pcs.push(this);this.addEventListener('datachannel',e=>this.track(e.channel.label,e.channel));}
    track(label,dc){if(mode==='tradeoff'&&label==='phantom-distribution-scout')dc.addEventListener('message',event=>{if(typeof event.data==='string'&&JSON.parse(event.data).action==='ack')clocks[this.scoutNode]?.ack();});if(label==='phantom-distribution-scout')scouted.add(this);if(mode==='repair'&&label==='phantom-distribution-scout'&&this.scoutNode===source){const lane=pcs.filter(pc=>pc.scoutNode===source&&scouted.has(pc)).indexOf(this),nativeSend=dc.send.bind(dc);dc.send=data=>{let binary=false;if(typeof data==='string'){const frame=JSON.parse(data);binary=['binary','binary-fragment'].includes(frame.kind);}if(lane===1&&(data instanceof ArrayBuffer||binary))return;return nativeSend(data);};}if(label==='phantom-distribution')filePCs.add(this);}
    createDataChannel(label,options){const dc=super.createDataChannel(label,options);this.track(label,dc);if(label==='phantom-distribution'){const send=dc.send.bind(dc);dc.send=data=>{const result=send(data);fileQueuePeak=Math.max(fileQueuePeak,dc.bufferedAmount);if(typeof data!=='string')filePayloadPeak=Math.max(filePayloadPeak,data.byteLength??0);return result;};}return dc;}
   }
   const services=ids.map((id,i)=>createNetworkDistribution({networkId:'worm-browser',adminPublicKey:admin.publicKey,
    runtime:{crypto,performance:(mode==='pair'||mode==='baseline'||mode==='queue'||mode==='message'||mode==='tradeoff')?clocks[i]:performance,RTCPeerConnection:class extends RTC{constructor(config){super(config);this.scoutNode=i;}}},lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'worm-browser',action,target,...fields,operationId:crypto.randomUUID()})},
    core:{getState:()=>({peerId:id,leader,mirror:ids[1],role:i===2?'syscon1':i===1?'syscon2':'peer',state:'admitted',admitted:true}),
     onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{to,frame}){if(['pair','baseline','queue','message','tradeoff'].includes(mode))frame=modelReceiverEvidence(frame,{winner:mode==='tradeoff'?[]:mode==='baseline'?[0,1,2]:[1,2],queueWinner:mode==='queue',messageWinner:mode==='message'});if(type==='PHANTOM_DIST_SEND')queueMicrotask(()=>callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));}},
    options:{intervalMs:10,requestTimeoutMs:mode==='repair'?2000:10000,batchSize:['queue','message','workload','repair','repeat','peer-budget','progressive'].includes(mode)?32:2,requestWindow:['queue','message','workload','repair','repeat','peer-budget','progressive'].includes(mode)?32:8,maxChunkBytes:262144,assignedRanges:true,transferLanes:3,capabilityExchange:true,transferScout:true,scoutComparison:mode==='message'?'message':'queue',fileTimeline:true,scoutBeforeSend:['send','workload','repair','repeat','peer-budget','progressive'].includes(mode),scoutProgressiveAdmission:mode==='progressive',scoutQueueComparison:['repeat','peer-budget','progressive'].includes(mode),scoutQueueBudgetScope:mode==='peer-budget'?'peer':'association',rtcConfiguration:{iceServers:[]}},
    emit(type,data){if(type==='distributionScout')clocks[i].report(data);events.push({i,type,data});if(i===2&&type==='distributionJob')peak=Math.max(peak,services[2].snapshot().jobs.length);}}));
   for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
   const wait=async predicate=>{const end=performance.now()+45000;while(!predicate()){const error=events.find(e=>e.type==='distributionError');if(error)throw Error(JSON.stringify(error));if(performance.now()>end)throw Error('browser test deadline: '+JSON.stringify(events.slice(-6)));await new Promise(r=>setTimeout(r,10));}};
   try{
    services[0].requestCapabilities(ids[1]);services[1].requestCapabilities(ids[0]);
    await wait(()=>services[0].getCapabilities(ids[1])&&services[1].getCapabilities(ids[0]));
    if(mode==='repeat'){
     const selections=[];
     for(const [round,from]of [0,0,1].entries()){
      const to=1-from,begin=events.length;
      const plan=services[from].proposePlan(ids[to],{maxPayloadBytes:262144,allowFragmentation:true});
      await wait(()=>services[from].getPlan(ids[to])?.id===plan);
      const bytes=Uint8Array.from({length:8*1024*1024+17},(_,i)=>(i+round)%251),file=new File([bytes],'repeat-'+round);
      const prepared=await services[from].prepareAgreed(file,ids[to]);
      const verified=events.slice(begin).filter(e=>e.i===from&&e.type==='distributionScout'&&e.data.phase==='preflight-file-workload'&&e.data.status==='complete');
      const qualified=services[from].getScout(ids[to]);
      if(!verified.length||selections.includes(verified[0].data.id)||qualified?.fileWorkloadSelection?.status!=='verified')throw Error('file did not receive fresh qualification');
      if(qualified.fileWorkloadSelection.reason==='verified_reduced_file_workload'){
       if(qualified.fileWorkload.profile.queueTrial!=='reverify'||qualified.fileWorkloadQueueComparison.status!=='incomplete')throw Error('repaired file skipped subset qualification');
      }else if(verified.length<4)throw Error('clean file skipped its queue comparison');
      selections.push(verified[0].data.id);
      await services[from].publish(file,{prepared,recipients:[ids[to]]});
      await wait(()=>events.some(e=>e.i===to&&e.type==='distributionComplete'&&e.data.id===prepared.id));
      const blob=events.find(e=>e.i===to&&e.type==='distributionComplete'&&e.data.id===prepared.id).data.blob,received=new Uint8Array(await blob.arrayBuffer());
      if(received.length!==bytes.length||!received.every((v,i)=>v===bytes[i]))throw Error('repeat file mismatch');
      await wait(()=>services.every(service=>service.snapshot().connections.length===0)&&services[2].snapshot().jobs.length===0);
      const handoffs=events.slice(begin).filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-handoff');
      if(!handoffs.some(e=>e.i===from&&e.data.status==='claimed')||!handoffs.some(e=>e.i===to&&e.data.status==='bound')||handoffs.some(e=>e.data.id!==selections.at(-1)))throw Error('repeat file did not use its qualified associations');
     }
     return {mode,selections,identical:true,allFileAssociationsScouted:[...filePCs].every(pc=>scouted.has(pc)),coordinatorReceivedFile:events.some(e=>e.i===2&&e.type==='distributionComplete')};
    }
    if(mode==='complete'||mode==='pair'||mode==='baseline'||mode==='queue'||mode==='message'||mode==='tradeoff')await wait(()=>services[0].getScout(ids[1])&&services[1].getScout(ids[0]));
    let selection=services[source].getScout(ids[target]);
    services[source].proposePlan(ids[target],{maxPayloadBytes:262144,allowFragmentation:true});await wait(()=>services[source].getPlan(ids[target]));
    const scoutPCs=[...pcs];
    const bytes=Uint8Array.from({length:(['workload','repair','peer-budget','progressive'].includes(mode)?32:['queue','message'].includes(mode)?8:4)*1024*1024+17},(_,i)=>i%251),file=new File([bytes],'worm-fixture.bin');
    const prepared=await services[source].prepareAgreed(file,ids[target],{sourceReadBlockBytes:4*1024*1024});
    selection=services[source].getScout(ids[target]);
    await services[source].publish(file,{prepared,recipients:[ids[target]]});
    await wait(()=>events.some(e=>e.i===target&&e.type==='distributionComplete'));
    const blob=events.find(e=>e.i===target&&e.type==='distributionComplete').data.blob,received=new Uint8Array(await blob.arrayBuffer());
    if(mode==='cancel')await wait(()=>scoutPCs.every(pc=>pc.connectionState==='closed'));
    const applied=events.find(e=>e.i===source&&e.type==='distributionScout'&&e.data.phase==='preflight-applied');
    const timeline=events.filter(e=>e.type==='distributionTimeline');
    const sourceStages=timeline.filter(e=>e.i===source&&e.data.index===0),receiverStages=timeline.filter(e=>e.i===target&&e.data.index===0);
    const traced=sourceStages.some(e=>e.data.stage==='submit-complete')&&receiverStages.some(e=>e.data.stage==='write-complete')&&sourceStages.some(s=>receiverStages.some(r=>s.data.jobId===r.data.jobId&&s.data.batch===r.data.batch));
    return {mode,traced,clock:(mode==='pair'||mode==='baseline'||mode==='queue'||mode==='message'||mode==='tradeoff')?'synthetic_selection_only':'native',selection,peak,fileQueuePeak,filePayloadPeak,messageApplications:events.filter(e=>e.i===source&&e.data.phase==='preflight-message-applied').map(e=>e.data),queueApplications:events.filter(e=>e.i===source&&e.data.phase==='preflight-queue-applied').map(e=>e.data),handoffs:events.filter(e=>e.i===source&&e.data.phase==='preflight-handoff'&&e.data.status==='claimed').map(e=>e.data.lane),fileAssociations:filePCs.size,allFileAssociationsScouted:[...filePCs].every(pc=>scouted.has(pc)),scoutAssociations:scoutPCs.length,scoutClosed:scoutPCs.every(pc=>pc.connectionState==='closed'),
     applied:applied?.data??null,samples:events.filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-sample').length,
     incoming:events.filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-receive-sample').map(e=>e.data),
     transportEvidence:events.filter(e=>e.type==='distributionScout'&&e.data.transport).map(e=>({endpoint:e.i,phase:e.data.phase,...e.data.transport})),
     cancelled:events.some(e=>e.type==='distributionScout'&&e.data.status==='cancelled'),
     identical:received.length===bytes.length&&received.every((v,i)=>v===bytes[i]),receivedBytes:received.length,
     coordinatorReceivedFile:events.some(e=>e.i===2&&e.type==='distributionComplete')};
   }finally{services.forEach(s=>s.close());if(pcs.some(pc=>pc.connectionState!=='closed'))throw Error('RTC association leaked after session close');}
  },mode);
  if(mode==='repeat'){assert.equal(result.selections.length,3);assert.equal(new Set(result.selections).size,3);assert.equal(result.identical,true);assert.equal(result.allFileAssociationsScouted,true);assert.equal(result.coordinatorReceivedFile,false);console.log(JSON.stringify(result));continue;}
  console.log(JSON.stringify({mode,allFileAssociationsScouted:result.allFileAssociationsScouted,scoutAssociations:result.scoutAssociations,fileAssociations:result.fileAssociations,selection:result.selection?{connections:result.selection.connections,reason:result.selection.reason,plan:result.selection.connectionPlan}:null,applied:result.applied,handoffs:result.handoffs}));
  assert.equal(result.identical,true);assert.equal(result.traced,true);assert.equal(result.coordinatorReceivedFile,false);if(mode==='cancel')assert.equal(result.scoutClosed,true);else{assert.equal(result.allFileAssociationsScouted,true);assert.ok(result.fileAssociations>=2);}
  if(mode==='complete'||mode==='pair'||mode==='baseline'||mode==='queue'||mode==='message'||mode==='tradeoff'){assert.ok(result.incoming.length>=14&&result.incoming.length<=50);assert.equal(result.incoming.length,result.samples);assert.ok(result.incoming.every(s=>s.complete&&s.lanes.length===s.connections&&s.lanes.reduce((n,l)=>n+l.bytes,0)===s.bytes));assert.ok(result.selection.samples.every(s=>s.queues.reduce((n,q)=>n+q.payloadBytes,0)===s.bytes));assert.equal(result.scoutAssociations,6);assert.ok(result.samples<=50);assert.ok(result.selection.samples.length<=20);assert.ok(result.selection.samples.filter(s=>s.stage==='confirm').every(s=>s.bytes===3145728&&s.delivery.length>=3&&s.peakBufferedBytes.every(n=>n<=786432)));assert.ok(result.applied);assert.equal(result.applied.connections,result.selection.connections);assert.ok(result.peak<=result.selection.connections);}
  else if(mode==='cancel')assert.equal(result.cancelled,true);
  else{assert.equal(result.selection.version,8);assert.ok(result.selection.samples.length<=20);assert.equal(result.selection.payloadBudgetBytes,result.selection.connectionOnly?0:87*786432);assert.ok(result.applied);assert.equal(result.applied.connections,result.selection.connections);}
  if(mode==='repair'){
   const first=result.selection.fileWorkloads[0],last=result.selection.fileWorkload,lanes=result.selection.lanes;
   assert.equal(result.selection.fileWorkloads.length,2);assert.equal(first.status,'failed');assert.equal(last.status,'complete');
   // Native Chrome can also report socket errors on an otherwise undropped
   // lane. Require the exact verified/clean subset, not a fixed lane count.
   const eligible=first.receiver.lanes.filter(r=>r.verified&&r.bytes===first.profile.bytes/first.profile.lanes.length&&first.sourceSubmission.find(s=>s.lane===r.lane).finalQueue.bufferedBytes===0&&first.receiver.queues.find(s=>s.lane===r.lane).bufferedBytes===0).map(r=>r.lane);
   assert.ok(lanes.length>0);assert.ok(!lanes.includes(1));assert.deepEqual(lanes,eligible);
   assert.deepEqual(last.profile.lanes,lanes);assert.ok(last.receiver.lanes.every(r=>r.verified&&r.bytes===last.profile.bytes/lanes.length));
   assert.deepEqual(result.applied.lanes,lanes);assert.deepEqual(result.handoffs.sort(),lanes);assert.equal(result.fileAssociations,lanes.length*2);assert.equal(result.peak,lanes.length);
   assert.deepEqual(result.selection.fileWorkloadSelection.rejected,[0,1,2].filter(l=>!lanes.includes(l)));
   if(first.reason==='local_send_discards_after_verified_lane')assert.ok(first.sourcePressure.observations.every(r=>r.bufferedBytes>0&&r.change.counters.packetsDiscardedOnSend.status==='observed'&&r.change.counters.packetsDiscardedOnSend.value>0));
  }
  if(mode==='progressive'){
   const trials=result.selection.fileWorkloads;
   assert.equal(trials[0].status,'complete');assert.deepEqual(trials[0].profile.lanes,[0]);assert.equal(trials[0].profile.laneAdmission,true);
   assert.deepEqual(trials[1].profile.lanes,[0,1]);
   for(let i=1;i<trials.length;i++)if(trials[i].profile.lanes.length>trials[i-1].profile.lanes.length){assert.equal(trials[i-1].status,'complete');assert.equal(trials[i-1].profile.laneAdmission,true);assert.equal(trials[i].profile.lanes.length,trials[i-1].profile.lanes.length+1);}
   assert.equal(result.selection.fileWorkload.status,'complete');assert.deepEqual(result.handoffs.sort(),result.selection.lanes);
   assert.equal(result.fileAssociations,result.selection.lanes.length*2);assert.ok(result.queueApplications.every(r=>r.policy.highWaterMark===result.selection.fileWorkload.profile.queuePolicy.highWaterMark));
  }
  if(mode==='peer-budget'){
   const p=result.selection.fileWorkload.profile,q=p.queuePolicy;
   assert.equal(p.queueBudgetScope,'peer');assert.equal(result.selection.fileWorkload.status,'complete');assert.equal(result.applied.connections,p.lanes.length);
   assert.equal(q.highWaterMark,Math.floor((p.queueTrial.startsWith('baseline')?16777216:1048576)/p.lanes.length));
   assert.equal(result.queueApplications.length,p.lanes.length);assert.ok(result.queueApplications.every(r=>r.policy.highWaterMark===q.highWaterMark&&r.policy.lowWaterMark===q.lowWaterMark));
   assert.ok(result.fileQueuePeak<=q.highWaterMark);assert.equal(result.selection.connectionPlan.queuePolicy.status,'workload_verified');
  }
  if(mode==='workload'){assert.equal(result.selection.connectionOnly,true);assert.equal(result.selection.samples.length,0);assert.equal(result.selection.fileWorkload.status,'complete');assert.equal(result.selection.fileWorkload.profile.chunkSize,262144);assert.equal(result.selection.fileWorkload.profile.sourceReadBlockBytes,4194304);assert.equal(result.selection.fileWorkload.profile.chunksPerLane,32);assert.equal(result.selection.fileWorkload.profile.bytes,result.selection.connections*8388608);assert.equal(result.selection.fileWorkload.receiver.requestWindow,32);assert.equal(result.selection.fileWorkload.receiver.lanes.reduce((n,l)=>n+l.bytes,0),result.selection.fileWorkload.profile.bytes);}
  if(mode==='queue'){assert.equal(result.selection.queuePolicy.status,'measured');assert.equal(result.selection.queuePolicy.highWaterMark,1048576);assert.equal(result.queueApplications.length,result.selection.connections);assert.ok(result.queueApplications.every(r=>r.policy.highWaterMark===1048576&&r.id===result.selection.id));assert.ok(result.fileQueuePeak<=1048576+32768);}
  if(mode==='pair'){assert.equal(result.selection.samples.length,20);assert.equal(result.selection.stoppedEarly,false);assert.equal(result.selection.connections,2);assert.deepEqual(result.selection.lanes,[1,2]);assert.deepEqual(result.handoffs.sort(),[1,2]);assert.equal(result.fileAssociations,4);assert.equal(result.peak,2);}
  if(mode==='baseline'){assert.equal(result.selection.stoppedEarly,false);assert.equal(result.selection.status,'measured');assert.equal(result.selection.samples.filter(s=>s.stage==='confirm').length,6);}
  if(!['cancel','message'].includes(mode)&&!result.selection.connectionOnly){const rounds=result.selection.profileComparison.status==='skipped'?0:4;assert.equal(result.selection.queueSamples.length,rounds);assert.equal(result.selection.queueComparisonPayloadBytes,rounds*9*1024*1024);assert.ok(result.selection.queueSamples.every(s=>s.bytes===9437184&&s.lanes.join(',')===result.selection.lanes.join(',')));}
  if(mode!=='cancel'&&!result.selection.connectionOnly){
   assert.ok(result.transportEvidence.some(r=>r.phase==='preflight-receive-stats'));
   assert.equal(result.selection.receiverEvidence.status,'complete');
   assert.equal(result.selection.receiverEvidence.receivedRounds,result.selection.completedSamples+1+result.selection.queueSamples.length+result.selection.messageSamples.length);
   assert.ok(result.selection.receiverEvidence.observations.every(r=>r.clock==='receiver_monotonic'&&r.lanes.reduce((n,l)=>n+l.bytes,0)===r.bytes));
   for(const endpoint of [0,1])assert.ok(result.transportEvidence.some(r=>r.endpoint===endpoint&&r.current?.packetsReceived!==null&&r.current?.packetsReceived!==undefined));
   assert.ok(result.transportEvidence.every(r=>r.change.kernelReceiveDrops.status==='unavailable'));
  }
  if(mode==='tradeoff'){assert.equal(result.selection.status,'bounded_tradeoff');assert.deepEqual(result.selection.lanes,[0]);assert.deepEqual(result.applied.lanes,[0]);assert.equal(result.peak,1);assert.equal(result.fileAssociations,2);}
  if(mode==='message'){assert.equal(result.selection.messageSamples.length,4);assert.equal(result.selection.messageComparisonPayloadBytes,36*1024*1024);assert.equal(result.selection.queueSamples.length,0);assert.equal(result.selection.messagePolicy.status,'measured');assert.equal(result.selection.messagePolicy.maxPayloadBytes,16384);assert.equal(result.messageApplications.length,result.selection.connections);assert.ok(result.filePayloadPeak>0&&result.filePayloadPeak<=16384);}
  console.log(JSON.stringify(result));
 }
 assert.deepEqual(errors,[]);
}finally{await browser?.close();await new Promise(r=>server.close(r));}
