import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const root=resolve(fileURLToPath(new URL('../../',import.meta.url)));
const require=createRequire(new URL('../swarm/package.json',import.meta.url));
const {chromium}=require('playwright');
const server=createServer(async(req,res)=>{try{
 const path=new URL(req.url,'http://localhost').pathname;
 if(path==='/'){res.setHeader('content-type','text/html');res.end('<!doctype html><title>Production request window lab</title>');return;}
 const target=resolve(root,'.'+path);if(!target.startsWith(root+sep)||!path.startsWith('/packages/')||!path.endsWith('.js')){res.writeHead(403).end();return;}
 res.setHeader('content-type','text/javascript');res.end(await readFile(target));
}catch{res.writeHead(404).end();}});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
let browser;const results=[];
const windows={current:32,one:1,two:2};
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE,args:['--no-sandbox']});
 for(const [trial,policy]of ['current','one','two','two','one','current'].entries()){
  const contexts=await Promise.all([browser.newContext(),browser.newContext()]),pages=await Promise.all(contexts.map(c=>c.newPage()));
  const errors=[];for(const page of pages)page.on('pageerror',e=>errors.push(e.message));let watchdog;
  const config={trial:trial+1,policy,requestWindow:windows[policy],lanes:2,bytesPerLane:32*1024*1024,chunkSize:262144,batchSize:32,sourceReadBlockBytes:4194304,queuePolicy:{highWaterMark:1048576,lowWaterMark:524288}};
  const result={...config,laneRequestWindow:Math.min(windows[policy],16),topology:'same_host_separate_renderers',chrome:browser.version(),coverage:'production_file_service_and_channel_sender_verified_memory_sink'};
  try{
   await Promise.all(pages.map(p=>p.goto(`http://127.0.0.1:${server.address().port}/`)));
   await Promise.all(pages.map((page,index)=>page.evaluate(async({index,config})=>{
    let done,failed;window.lab={pcs:[],channels:[],opened:[],driver:null,received:[],rows:[],submissions:[],retries:[],done:new Promise((r,j)=>{done=r;failed=j;}),resolve:()=>done(),reject:e=>failed(e)};lab.done.catch(()=>{});
    for(let lane=0;lane<2;lane++){
     const pc=new RTCPeerConnection({iceServers:[]});lab.pcs[lane]=pc;
     const bind=dc=>{lab.channels[lane]=dc;dc.binaryType='arraybuffer';dc.onmessage=event=>{try{lab.driver.receive(lane,event.data);}catch(e){lab.reject(e);}};dc.onerror=()=>lab.reject(Error('data channel error'));};
     if(index===0){const dc=pc.createDataChannel('file-window',{ordered:true});bind(dc);lab.opened[lane]=new Promise(r=>dc.onopen=r);}
     else{lab.opened[lane]=new Promise(r=>pc.ondatachannel=event=>{bind(event.channel);event.channel.onopen=r;});lab.received[lane]=new Uint8Array(config.bytesPerLane);}
     lab.rows[lane]={lane,bytes:0,messages:0,firstAtMs:null,lastAtMs:null,maxGapMs:0};
     lab.submissions[lane]={lane,bytes:0,messages:0,peakBufferedBytes:0,bufferWaitMs:0};
    }
   },{index,config})));
   for(let lane=0;lane<2;lane++){
    const offer=await pages[0].evaluate(async lane=>{const pc=lab.pcs[lane];await pc.setLocalDescription(await pc.createOffer());if(pc.iceGatheringState!=='complete')await new Promise(r=>pc.onicegatheringstatechange=()=>{if(pc.iceGatheringState==='complete')r();});return pc.localDescription.toJSON();},lane);
    const answer=await pages[1].evaluate(async({lane,offer})=>{const pc=lab.pcs[lane];await pc.setRemoteDescription(offer);await pc.setLocalDescription(await pc.createAnswer());if(pc.iceGatheringState!=='complete')await new Promise(r=>pc.onicegatheringstatechange=()=>{if(pc.iceGatheringState==='complete')r();});return pc.localDescription.toJSON();},{lane,offer});
    await pages[0].evaluate(({lane,answer})=>lab.pcs[lane].setRemoteDescription(answer),{lane,answer});
   }
   await Promise.all(pages.map(p=>p.evaluate(()=>Promise.all(lab.opened))));
   await Promise.all(pages.map((page,index)=>page.evaluate(async({index,config})=>{
    const {createScoutFileWorkload}=await import('/packages/phantom-core/src/scout-file-workload.js');
    if(index===1)lab.expectedHash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',new Uint8Array(config.bytesPerLane)))].join(',');
    lab.driver=createScoutFileWorkload({peer:index?'1111111111111111':'2222222222222222',lanes:[0,1],channels:lab.channels,pcs:lab.pcs,bytes:config.bytesPerLane*2,chunkSize:config.chunkSize,batchSize:config.batchSize,sourceReadBlockBytes:config.sourceReadBlockBytes,requestWindow:config.requestWindow,requestTimeoutMs:10000,sendTimeoutMs:15000,maxAttemptsPerPeer:2,queuePolicy:config.queuePolicy,messageBytes:null,scopeId:'request-window-'+config.trial,
     onChunk(lane,data){const row=lab.rows[lane],at=performance.now();if(row.firstAtMs===null)row.firstAtMs=at;if(row.lastAtMs!==null)row.maxGapMs=Math.max(row.maxGapMs,at-row.lastAtMs);row.lastAtMs=at;if(row.bytes+data.byteLength>config.bytesPerLane)throw Error('receiver overflow');lab.received[lane].set(new Uint8Array(data),row.bytes);row.bytes+=data.byteLength;row.messages++;},
     onSend(lane,data){const row=lab.submissions[lane];row.bytes+=data.bytes??0;row.messages+=data.coalesced?0:1;row.peakBufferedBytes=Math.max(row.peakBufferedBytes,data.bufferedBytes??0);row.bufferWaitMs+=data.bufferWaitMs??0;},
     onRetry(lane,data){lab.retries.push({lane,index:data.index});},
     onComplete(){lab.completeAtMs=performance.now();lab.resolve();},
     onError(error){lab.reject(error);for(const dc of lab.channels)dc.close();}
    });
    if(index===0)await lab.driver.prepare();
   },{index,config})));
   const snapshot=()=>pages[0].evaluate(async()=>Promise.all(lab.pcs.map(async(pc,lane)=>{
    const stats=await pc.getStats(),transport=[...stats.values()].find(r=>r.type==='transport'&&r.selectedCandidatePairId),pair=transport&&stats.get(transport.selectedCandidatePairId);if(!pair)throw Error('selected route unavailable');
    return {lane,pairId:pair.id,packetsSent:pair.packetsSent,bytesSent:pair.bytesSent,packetsDiscardedOnSend:pair.packetsDiscardedOnSend,bytesDiscardedOnSend:pair.bytesDiscardedOnSend};
   })));
   const before=await snapshot(),started=performance.now();
   const receiving=pages[1].evaluate(async()=>{await lab.done;
    for(let lane=0;lane<2;lane++){const hash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',lab.received[lane]))].join(',');if(hash!==lab.expectedHash)throw Error('whole lane hash mismatch');lab.rows[lane].verified=true;}
    return {lanes:lab.rows,arrivalSpanMs:Math.max(...lab.rows.map(r=>r.lastAtMs))-Math.min(...lab.rows.map(r=>r.firstAtMs)),completeAtMs:lab.completeAtMs,retries:lab.retries,queues:lab.driver.snapshot()};
   });
   // A failure bound only; production request/drain events control all feeding.
   const timeout=new Promise((_,reject)=>{watchdog=setTimeout(()=>reject(Error('45 second test deadline')),45000);});
   await pages[0].evaluate(()=>lab.driver.start());
   result.receiver=await Promise.race([receiving,timeout]);result.elapsedMs=performance.now()-started;result.verifiedBytesPerSecond=config.bytesPerLane*2*1000/result.elapsedMs;
   result.source=await pages[0].evaluate(()=>({submissions:lab.submissions,queues:lab.driver.snapshot(),retries:lab.retries}));
   const after=await snapshot();result.transport=after.map((row,i)=>{if(row.pairId!==before[i].pairId)throw Error('selected route changed');return {lane:i,...Object.fromEntries(['packetsSent','bytesSent','packetsDiscardedOnSend','bytesDiscardedOnSend'].map(k=>{if(!Number.isFinite(row[k])||!Number.isFinite(before[i][k])||row[k]<before[i][k])throw Error('counter missing/reset: '+k);return [k,row[k]-before[i][k]];}))};});
   assert.deepEqual(errors,[]);assert.ok(result.receiver.lanes.every(r=>r.verified&&r.bytes===config.bytesPerLane&&r.messages===128));assert.ok(result.source.queues.every(q=>q.bufferedBytes===0));result.status='complete';
  }catch(error){result.status='failed';result.error=error.stack??String(error);}
  finally{clearTimeout(watchdog);await Promise.all(contexts.map(c=>c.close()));}
  results.push(result);console.log(JSON.stringify(result));
 }
 console.log(JSON.stringify({kind:'request-window-summary',results:results.map(r=>({trial:r.trial,policy:r.policy,window:r.laneRequestWindow,status:r.status,verifiedBytesPerSecond:r.verifiedBytesPerSecond,sendDiscards:r.transport?.reduce((n,row)=>n+row.packetsDiscardedOnSend,0),maxGapMs:r.receiver&&Math.max(...r.receiver.lanes.map(row=>row.maxGapMs)),retries:r.receiver?.retries.length}))}));
 assert.ok(results.every(r=>r.status==='complete'),'one or more production window trials failed');
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
