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
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto(`http://127.0.0.1:${server.address().port}/`);
 for(const mode of ['complete','cancel','send','pair','early','queue']){
  const result=await page.evaluate(async mode=>{
   const {createNetworkDistribution}=await import('/packages/phantom-core/src/network-distribution.js');
   const {createAdminIdentity}=await import('/packages/phantom-core/src/authorization.js');
   const ids=['1111111111111111','2222222222222222','3333333333333333'],leader=ids[2],admin=await createAdminIdentity();
   const {scoutClock}=await import('/tests/fixtures/scout-clock.js');
   const {modelReceiverEvidence}=await import('/tests/fixtures/scout-receiver-model.js');
   const clocks=ids.map(()=>scoutClock({winner:mode==='early'?[0,1,2]:[1,2],queueWinner:mode==='queue'}));
   const callbacks=new Map(),events=[],pcs=[],scouted=new Set(),filePCs=new Set();let peak=0,fileQueuePeak=0;const source=mode==='send'?1:0,target=1-source;
   class RTC extends RTCPeerConnection{
    constructor(config){super(config);pcs.push(this);this.addEventListener('datachannel',e=>this.track(e.channel.label));}
    track(label){if(label==='phantom-distribution-scout')scouted.add(this);if(label==='phantom-distribution')filePCs.add(this);}
    createDataChannel(label,options){this.track(label);const dc=super.createDataChannel(label,options);if(label==='phantom-distribution'){const send=dc.send.bind(dc);dc.send=data=>{const result=send(data);fileQueuePeak=Math.max(fileQueuePeak,dc.bufferedAmount);return result;};}return dc;}
   }
   const services=ids.map((id,i)=>createNetworkDistribution({networkId:'worm-browser',adminPublicKey:admin.publicKey,
    runtime:{crypto,performance:(mode==='pair'||mode==='early'||mode==='queue')?clocks[i]:performance,RTCPeerConnection:RTC},lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'worm-browser',action,target,...fields,operationId:crypto.randomUUID()})},
    core:{getState:()=>({peerId:id,leader,mirror:ids[1],role:i===2?'syscon1':i===1?'syscon2':'peer',state:'admitted',admitted:true}),
     onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{to,frame}){if(['pair','early','queue'].includes(mode))frame=modelReceiverEvidence(frame,{winner:mode==='early'?[0,1,2]:[1,2],queueWinner:mode==='queue'});if(type==='PHANTOM_DIST_SEND')queueMicrotask(()=>callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));}},
    options:{intervalMs:10,batchSize:mode==='queue'?32:2,requestWindow:mode==='queue'?32:8,maxChunkBytes:262144,assignedRanges:true,transferLanes:3,capabilityExchange:true,transferScout:true,fileTimeline:true,scoutBeforeSend:mode==='send',rtcConfiguration:{iceServers:[]}},
    emit(type,data){if(type==='distributionScout')clocks[i].report(data);events.push({i,type,data});if(i===2&&type==='distributionJob')peak=Math.max(peak,services[2].snapshot().jobs.length);}}));
   for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
   const wait=async predicate=>{const end=performance.now()+45000;while(!predicate()){const error=events.find(e=>e.type==='distributionError');if(error)throw Error(JSON.stringify(error));if(performance.now()>end)throw Error('browser test deadline: '+JSON.stringify(events.slice(-6)));await new Promise(r=>setTimeout(r,10));}};
   try{
    services[0].requestCapabilities(ids[1]);services[1].requestCapabilities(ids[0]);
    await wait(()=>services[0].getCapabilities(ids[1])&&services[1].getCapabilities(ids[0]));
    if(mode==='complete'||mode==='pair'||mode==='early'||mode==='queue')await wait(()=>services[0].getScout(ids[1])&&services[1].getScout(ids[0]));
    let selection=services[source].getScout(ids[target]);
    services[source].proposePlan(ids[target],{maxPayloadBytes:262144,allowFragmentation:true});await wait(()=>services[source].getPlan(ids[target]));
    const scoutPCs=[...pcs];
    const bytes=Uint8Array.from({length:(mode==='queue'?8:4)*1024*1024+17},(_,i)=>i%251),file=new File([bytes],'worm-fixture.bin');
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
    return {mode,traced,clock:(mode==='pair'||mode==='early'||mode==='queue')?'synthetic_selection_only':'native',selection,peak,fileQueuePeak,queueApplications:events.filter(e=>e.i===source&&e.data.phase==='preflight-queue-applied').map(e=>e.data),handoffs:events.filter(e=>e.i===source&&e.data.phase==='preflight-handoff'&&e.data.status==='claimed').map(e=>e.data.lane),fileAssociations:filePCs.size,allFileAssociationsScouted:[...filePCs].every(pc=>scouted.has(pc)),scoutAssociations:scoutPCs.length,scoutClosed:scoutPCs.every(pc=>pc.connectionState==='closed'),
     applied:applied?.data??null,samples:events.filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-sample').length,
     incoming:events.filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-receive-sample').map(e=>e.data),
     transportEvidence:events.filter(e=>e.type==='distributionScout'&&e.data.transport).map(e=>({endpoint:e.i,phase:e.data.phase,...e.data.transport})),
     cancelled:events.some(e=>e.type==='distributionScout'&&e.data.status==='cancelled'),
     identical:received.length===bytes.length&&received.every((v,i)=>v===bytes[i]),receivedBytes:received.length,
     coordinatorReceivedFile:events.some(e=>e.i===2&&e.type==='distributionComplete')};
   }finally{services.forEach(s=>s.close());if(pcs.some(pc=>pc.connectionState!=='closed'))throw Error('RTC association leaked after session close');}
  },mode);
  console.log(JSON.stringify({mode,allFileAssociationsScouted:result.allFileAssociationsScouted,scoutAssociations:result.scoutAssociations,fileAssociations:result.fileAssociations,selection:result.selection?{connections:result.selection.connections,reason:result.selection.reason,plan:result.selection.connectionPlan}:null,applied:result.applied,handoffs:result.handoffs}));
  assert.equal(result.identical,true);assert.equal(result.traced,true);assert.equal(result.coordinatorReceivedFile,false);if(mode==='cancel')assert.equal(result.scoutClosed,true);else{assert.equal(result.allFileAssociationsScouted,true);assert.ok(result.fileAssociations>=2);}
  if(mode==='complete'||mode==='pair'||mode==='early'||mode==='queue'){assert.ok(result.incoming.length>=14&&result.incoming.length<=50);assert.equal(result.incoming.length,result.samples);assert.ok(result.incoming.every(s=>s.complete&&s.lanes.length===s.connections&&s.lanes.reduce((n,l)=>n+l.bytes,0)===s.bytes));assert.ok(result.selection.samples.every(s=>s.queues.reduce((n,q)=>n+q.payloadBytes,0)===s.bytes));assert.equal(result.scoutAssociations,6);assert.ok(result.samples<=50);assert.ok(result.selection.samples.length<=20);assert.ok(result.selection.samples.filter(s=>s.stage==='confirm').every(s=>s.bytes===3145728&&s.delivery.length>=3&&s.peakBufferedBytes.every(n=>n<=786432)));assert.ok(result.applied);assert.equal(result.applied.connections,result.selection.connections);assert.ok(result.peak<=result.selection.connections);}
  else if(mode==='cancel')assert.equal(result.cancelled,true);
  else{assert.equal(result.selection.version,6);assert.ok(result.selection.samples.length<=20);assert.equal(result.selection.payloadBudgetBytes,87*786432);assert.ok(result.applied);assert.equal(result.applied.connections,result.selection.connections);}
  if(mode==='queue'){assert.equal(result.selection.queuePolicy.status,'measured');assert.equal(result.selection.queuePolicy.highWaterMark,1048576);assert.equal(result.queueApplications.length,result.selection.connections);assert.ok(result.queueApplications.every(r=>r.policy.highWaterMark===1048576&&r.id===result.selection.id));assert.ok(result.fileQueuePeak<=1048576+32768);}
  if(mode==='pair'){assert.equal(result.selection.samples.length,20);assert.equal(result.selection.stoppedEarly,false);assert.equal(result.selection.connections,2);assert.deepEqual(result.selection.lanes,[1,2]);assert.deepEqual(result.handoffs.sort(),[1,2]);assert.equal(result.fileAssociations,4);assert.equal(result.peak,2);}
  if(mode==='early'){assert.equal(result.selection.stoppedEarly,true);assert.equal(result.selection.status,'inconclusive');assert.equal(result.selection.samples.some(s=>s.stage==='confirm'),false);assert.ok(result.selection.directionPayloadAvoidedBytes>0);}
  if(mode!=='cancel'){assert.equal(result.selection.queueSamples.length,4);assert.equal(result.selection.queueComparisonPayloadBytes,36*1024*1024);assert.ok(result.selection.queueSamples.every(s=>s.bytes===9437184&&s.lanes.join(',')===result.selection.lanes.join(',')));}
  if(mode!=='cancel'){
   assert.ok(result.transportEvidence.some(r=>r.phase==='preflight-receive-stats'));
   assert.equal(result.selection.receiverEvidence.status,'complete');
   assert.equal(result.selection.receiverEvidence.receivedRounds,result.selection.completedSamples+5);
   assert.ok(result.selection.receiverEvidence.observations.every(r=>r.clock==='receiver_monotonic'&&r.lanes.reduce((n,l)=>n+l.bytes,0)===r.bytes));
   for(const endpoint of [0,1])assert.ok(result.transportEvidence.some(r=>r.endpoint===endpoint&&r.current?.packetsReceived!==null&&r.current?.packetsReceived!==undefined));
   assert.ok(result.transportEvidence.every(r=>r.change.kernelReceiveDrops.status==='unavailable'));
  }
  console.log(JSON.stringify(result));
 }
 assert.deepEqual(errors,[]);
}finally{await browser?.close();await new Promise(r=>server.close(r));}
