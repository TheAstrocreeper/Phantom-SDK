import {createServer} from 'node:http';
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const require=createRequire(new URL('../swarm/package.json',import.meta.url));
const {chromium}=require('playwright');
const server=createServer((req,res)=>{res.setHeader('content-type','text/html');res.end('<!doctype html><title>Bounded feed lab</title>');});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
let browser;
const profiles={separate:{connectionCount:2,messageBytes:262144,highWaterMark:1048576,lowWaterMark:524288},shared:{connectionCount:1,messageBytes:262144,highWaterMark:1048576,lowWaterMark:524288}};
const results=[];
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE,args:['--no-sandbox']});
 for(const [trial,policy]of ['separate','shared','shared','separate'].entries()){
  // Separate browser contexts give the peers separate renderers. They still
  // share one host/network service: this is a local mechanism test, not 5G.
  const contexts=await Promise.all([browser.newContext(),browser.newContext()]);
  const pages=await Promise.all(contexts.map(c=>c.newPage()));
  const errors=[];for(const page of pages)page.on('pageerror',e=>errors.push(e.message));
  let watchdog;
  const result={trial:trial+1,policy,...profiles[policy],lanes:2,bytesPerLane:32*1024*1024,topology:'same_host_separate_renderers',chrome:browser.version()};
  try{
   await Promise.all(pages.map(p=>p.goto(`http://127.0.0.1:${server.address().port}/`)));
   await Promise.all(pages.map((page,index)=>page.evaluate(async({index,bytes,connectionCount})=>{
    const expected=new Uint8Array(bytes);for(let i=0;i<bytes;i++)expected[i]=i%251;
    const digest=await crypto.subtle.digest('SHA-256',expected);
    window.lab={pcs:[],channels:[],expected,hash:[...new Uint8Array(digest)].join(','),received:[],receiver:[],source:[],opened:[],done:null};
    if(index===1){let done,reject;lab.done=new Promise((r,j)=>{done=r;reject=j;});lab.done.catch(()=>{});let finished=0;
     const openResolvers=[];
     for(let lane=0;lane<2;lane++){
      lab.received[lane]=new Uint8Array(bytes);
      lab.receiver[lane]={lane,bytes:0,messages:0,firstAtMs:null,lastAtMs:null,maxGapMs:0};
      lab.opened[lane]=new Promise(resolve=>openResolvers[lane]=resolve);
     }
     for(let connection=0;connection<connectionCount;connection++){
      const pc=new RTCPeerConnection({iceServers:[]});lab.pcs[connection]=pc;
      pc.ondatachannel=({channel:dc})=>{
       const lane=Number(dc.label.split('-').at(-1));if(!Number.isInteger(lane)||lane<0||lane>1||lab.channels[lane]){reject(Error('unexpected channel'));return;}
       lab.channels[lane]=dc;dc.binaryType='arraybuffer';const row=lab.receiver[lane];
       dc.onopen=openResolvers[lane];dc.onerror=()=>reject(Error('receiver channel error'));
       dc.onmessage=event=>{try{
        if(!(event.data instanceof ArrayBuffer)||row.bytes+event.data.byteLength>bytes)throw Error('invalid received payload');
        const at=performance.now();if(row.firstAtMs===null)row.firstAtMs=at;if(row.lastAtMs!==null)row.maxGapMs=Math.max(row.maxGapMs,at-row.lastAtMs);row.lastAtMs=at;
        lab.received[lane].set(new Uint8Array(event.data),row.bytes);row.bytes+=event.data.byteLength;row.messages++;
        if(row.bytes===bytes){row.arrivalSpanMs=row.lastAtMs-row.firstAtMs;finished++;if(finished===2){
         lab.arrivalSpanMs=Math.max(...lab.receiver.map(r=>r.lastAtMs))-Math.min(...lab.receiver.map(r=>r.firstAtMs));done();
        }}
       }catch(e){reject(e);}};
      };
     }
    }else{
     for(let lane=0;lane<2;lane++){
      const connection=lane%connectionCount;const pc=lab.pcs[connection]??new RTCPeerConnection({iceServers:[]});lab.pcs[connection]=pc;const dc=pc.createDataChannel('bounded-feed-'+lane,{ordered:true});lab.channels[lane]=dc;
      lab.opened[lane]=new Promise((r,j)=>{dc.onopen=r;dc.onerror=()=>j(Error('source channel error'));});
     }
    }
   },{index,bytes:result.bytesPerLane,connectionCount:profiles[policy].connectionCount})));
   for(let lane=0;lane<profiles[policy].connectionCount;lane++){
    const offer=await pages[0].evaluate(async lane=>{const pc=lab.pcs[lane];await pc.setLocalDescription(await pc.createOffer());if(pc.iceGatheringState!=='complete')await new Promise(r=>pc.onicegatheringstatechange=()=>{if(pc.iceGatheringState==='complete')r();});return pc.localDescription.toJSON();},lane);
    const answer=await pages[1].evaluate(async({lane,offer})=>{const pc=lab.pcs[lane];await pc.setRemoteDescription(offer);await pc.setLocalDescription(await pc.createAnswer());if(pc.iceGatheringState!=='complete')await new Promise(r=>pc.onicegatheringstatechange=()=>{if(pc.iceGatheringState==='complete')r();});return pc.localDescription.toJSON();},{lane,offer});
    await pages[0].evaluate(({lane,answer})=>lab.pcs[lane].setRemoteDescription(answer),{lane,answer});
   }
   await Promise.all(pages.map(p=>p.evaluate(()=>Promise.all(lab.opened))));
   const snapshot=()=>pages[0].evaluate(async()=>Promise.all(lab.pcs.map(async(pc,lane)=>{
    const stats=await pc.getStats(),transport=[...stats.values()].find(r=>r.type==='transport'&&r.selectedCandidatePairId),pair=transport&&stats.get(transport.selectedCandidatePairId);
    if(!pair)throw Error('selected candidate pair unavailable');
    return {lane,pairId:pair.id,packetsSent:pair.packetsSent,bytesSent:pair.bytesSent,packetsDiscardedOnSend:pair.packetsDiscardedOnSend,bytesDiscardedOnSend:pair.bytesDiscardedOnSend};
   })));
   const before=await snapshot();
   const started=performance.now();
   const receiving=pages[1].evaluate(async()=>{await lab.done;const verificationStart=performance.now();
    for(let lane=0;lane<2;lane++){const hash=[...new Uint8Array(await crypto.subtle.digest('SHA-256',lab.received[lane]))].join(',');if(hash!==lab.hash)throw Error('payload hash mismatch');lab.receiver[lane].verified=true;}
    return {arrivalSpanMs:lab.arrivalSpanMs,verificationMs:performance.now()-verificationStart,lanes:lab.receiver};
   });
   const sending=pages[0].evaluate(async profile=>{
    await Promise.all(lab.channels.map(async(dc,lane)=>{
     const row={lane,bytes:0,messages:0,peakBufferedBytes:0,bufferWaitMs:0};lab.source[lane]=row;dc.bufferedAmountLowThreshold=profile.lowWaterMark;
     while(row.bytes<lab.expected.length){
      const size=Math.min(profile.messageBytes,lab.expected.length-row.bytes);
      while(dc.bufferedAmount+size>profile.highWaterMark){const start=performance.now();
       await new Promise((resolve,reject)=>{
        const cleanup=()=>{dc.removeEventListener('bufferedamountlow',low);dc.removeEventListener('close',closed);dc.removeEventListener('error',closed);};
        const low=()=>{cleanup();resolve();},closed=()=>{cleanup();reject(Error('source closed waiting for drain'));};
        dc.addEventListener('bufferedamountlow',low);dc.addEventListener('close',closed);dc.addEventListener('error',closed);
        if(dc.readyState!=='open')closed();else if(dc.bufferedAmount<=profile.lowWaterMark)low();
       });row.bufferWaitMs+=performance.now()-start;
      }
      dc.send(lab.expected.subarray(row.bytes,row.bytes+size));row.bytes+=size;row.messages++;row.peakBufferedBytes=Math.max(row.peakBufferedBytes,dc.bufferedAmount);
     }
    }));return lab.source;
   },profiles[policy]);
   // A test failure bound only: no pacing or recovery timer participates in feed.
   const timeout=new Promise((_,reject)=>{watchdog=setTimeout(()=>reject(Error('45 second test deadline')),45000);});
   const [source,receiver]=await Promise.race([Promise.all([sending,receiving]),timeout]);
   result.elapsedMs=performance.now()-started;result.verifiedBytesPerSecond=result.bytesPerLane*2*1000/result.elapsedMs;
   result.source=source;result.receiver=receiver;
   const after=await snapshot();result.transport=after.map((row,i)=>{
    if(row.pairId!==before[i].pairId)throw Error('route changed during trial');
    return {lane:i,...Object.fromEntries(['packetsSent','bytesSent','packetsDiscardedOnSend','bytesDiscardedOnSend'].map(k=>{if(!Number.isFinite(row[k])||!Number.isFinite(before[i][k])||row[k]<before[i][k])throw Error('counter unavailable/reset: '+k);return [k,row[k]-before[i][k]];}))};
   });
   assert.ok(receiver.lanes.every(r=>r.verified&&r.bytes===result.bytesPerLane));assert.ok(source.every(r=>r.peakBufferedBytes<=profiles[policy].highWaterMark));assert.deepEqual(errors,[]);result.status='complete';
  }catch(error){result.status='failed';result.error=error.stack??String(error);}
  finally{clearTimeout(watchdog);await Promise.all(contexts.map(c=>c.close()));}
  results.push(result);console.log(JSON.stringify(result));
 }
 console.log(JSON.stringify({kind:'association-layout-summary',results:results.map(r=>({trial:r.trial,policy:r.policy,status:r.status,elapsedMs:r.elapsedMs,verifiedBytesPerSecond:r.verifiedBytesPerSecond,sendDiscards:r.transport?.reduce((n,r)=>n+r.packetsDiscardedOnSend,0),maxReceiveGapMs:r.receiver&&Math.max(...r.receiver.lanes.map(l=>l.maxGapMs))}))}));
 assert.ok(results.every(r=>r.status==='complete'),'one or more feed trials failed');
}finally{await browser?.close();await new Promise(resolve=>server.close(resolve));}
