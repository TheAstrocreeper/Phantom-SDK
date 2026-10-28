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
 const target=resolve(root,'.'+path);if(!target.startsWith(root+sep)||!path.startsWith('/packages/')||!path.endsWith('.js')){res.writeHead(403).end();return;}
 res.setHeader('content-type','text/javascript');res.end(await readFile(target));
}catch{res.writeHead(404).end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));let browser;
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE,args:['--no-sandbox']});
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto(`http://127.0.0.1:${server.address().port}/`);
 for(const mode of ['complete','cancel']){
  const result=await page.evaluate(async mode=>{
   const {createNetworkDistribution}=await import('/packages/phantom-core/src/network-distribution.js');
   const {createAdminIdentity}=await import('/packages/phantom-core/src/authorization.js');
   const ids=['1111111111111111','2222222222222222','3333333333333333'],leader=ids[2],admin=await createAdminIdentity();
   const callbacks=new Map(),events=[],pcs=[];let peak=0;
   class RTC extends RTCPeerConnection{constructor(config){super(config);pcs.push(this);}}
   const services=ids.map((id,i)=>createNetworkDistribution({networkId:'worm-browser',adminPublicKey:admin.publicKey,
    runtime:{crypto,performance,RTCPeerConnection:RTC},lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'worm-browser',action,target,...fields,operationId:crypto.randomUUID()})},
    core:{getState:()=>({peerId:id,leader,mirror:ids[1],role:i===2?'syscon1':i===1?'syscon2':'peer',state:'admitted',admitted:true}),
     onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{to,frame}){if(type==='PHANTOM_DIST_SEND')queueMicrotask(()=>callbacks.get(to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));}},
    options:{intervalMs:10,batchSize:2,requestWindow:8,maxChunkBytes:262144,assignedRanges:true,transferLanes:3,capabilityExchange:true,transferScout:true,rtcConfiguration:{iceServers:[]}},
    emit(type,data){events.push({i,type,data});if(i===2&&type==='distributionJob')peak=Math.max(peak,services[2].snapshot().jobs.length);}}));
   for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
   const wait=async predicate=>{const end=performance.now()+45000;while(!predicate()){const error=events.find(e=>e.type==='distributionError');if(error)throw Error(JSON.stringify(error));if(performance.now()>end)throw Error('browser test deadline: '+JSON.stringify(events.slice(-6)));await new Promise(r=>setTimeout(r,10));}};
   try{
    services[0].requestCapabilities(ids[1]);services[1].requestCapabilities(ids[0]);
    await wait(()=>services[0].getCapabilities(ids[1])&&services[1].getCapabilities(ids[0]));
    if(mode==='complete')await wait(()=>services[0].getScout(ids[1])&&services[1].getScout(ids[0]));
    const selection=services[0].getScout(ids[1]);
    services[0].proposePlan(ids[1],{maxPayloadBytes:262144,allowFragmentation:true});await wait(()=>services[0].getPlan(ids[1]));
    const scoutPCs=[...pcs];
    const bytes=Uint8Array.from({length:4*1024*1024+17},(_,i)=>i%251),file=new File([bytes],'worm-fixture.bin');
    const prepared=await services[0].prepareAgreed(file,ids[1]);
    await services[0].publish(file,{prepared,recipients:[ids[1]]});
    await wait(()=>events.some(e=>e.i===1&&e.type==='distributionComplete'));
    const blob=events.find(e=>e.i===1&&e.type==='distributionComplete').data.blob,received=new Uint8Array(await blob.arrayBuffer());
    await wait(()=>scoutPCs.every(pc=>pc.connectionState==='closed'));
    const applied=events.find(e=>e.i===0&&e.type==='distributionScout'&&e.data.phase==='preflight-applied');
    return {mode,selection,peak,scoutAssociations:scoutPCs.length,scoutClosed:scoutPCs.every(pc=>pc.connectionState==='closed'),
     applied:applied?.data??null,samples:events.filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-sample').length,
     incoming:events.filter(e=>e.type==='distributionScout'&&e.data.phase==='preflight-receive-sample').map(e=>e.data),
     cancelled:events.some(e=>e.type==='distributionScout'&&e.data.status==='cancelled'),
     identical:received.length===bytes.length&&received.every((v,i)=>v===bytes[i]),receivedBytes:received.length,
     coordinatorReceivedFile:events.some(e=>e.i===2&&e.type==='distributionComplete')};
   }finally{services.forEach(s=>s.close());}
  },mode);
  assert.equal(result.identical,true);assert.equal(result.coordinatorReceivedFile,false);assert.equal(result.scoutClosed,true);
  if(mode==='complete'){assert.equal(result.incoming.length,18);assert.ok(result.incoming.every(s=>s.complete&&s.lanes.length===s.connections&&s.lanes.reduce((n,l)=>n+l.bytes,0)===s.bytes));assert.ok(result.selection.samples.every(s=>s.queues.reduce((n,q)=>n+q.payloadBytes,0)===s.bytes));assert.equal(result.scoutAssociations,6);assert.equal(result.samples,18);assert.equal(result.selection.samples.length,8);assert.ok(result.selection.samples.filter(s=>s.stage==='confirm').every(s=>s.bytes===3145728&&s.delivery.length>=3&&s.peakBufferedBytes.every(n=>n<=786432)));assert.ok(result.applied);assert.equal(result.applied.connections,result.selection.connections);assert.ok(result.peak<=result.selection.connections);}
  else assert.equal(result.cancelled,true);
  console.log(JSON.stringify(result));
 }
 assert.deepEqual(errors,[]);
}finally{await browser?.close();await new Promise(r=>server.close(r));}
