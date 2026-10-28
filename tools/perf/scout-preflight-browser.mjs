import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const root=resolve(fileURLToPath(new URL('../../',import.meta.url)));
const require=createRequire(new URL('../swarm/package.json',import.meta.url)),{chromium}=require('playwright');
const server=createServer(async(req,res)=>{try{const path=new URL(req.url,'http://localhost').pathname;if(path==='/'){res.setHeader('content-type','text/html');res.end('<!doctype html>');return;}const target=resolve(root,'.'+path);if(!target.startsWith(root+sep)||!path.startsWith('/packages/')||!path.endsWith('.js')){res.writeHead(403).end();return;}res.setHeader('content-type','text/javascript');res.end(await readFile(target));}catch{res.writeHead(404).end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));let browser;
try{
 browser=await chromium.launch({headless:true,executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE,args:['--no-sandbox']});const page=await browser.newPage();const errors=[];page.on('pageerror',e=>errors.push(e.message));await page.goto(`http://127.0.0.1:${server.address().port}/`);
 const result=await page.evaluate(async({agreed})=>{
  const {createNetworkDistribution}=await import('/packages/phantom-core/src/network-distribution.js');
  const {createAdminIdentity}=await import('/packages/phantom-core/src/authorization.js');
  const {scoutConnection}=await import('/packages/phantom-core/src/connection-scout.js');
  const control=[new RTCPeerConnection({iceServers:[]}),new RTCPeerConnection({iceServers:[]})],pending=[[],[]];
  for(let i=0;i<2;i++)control[i].onicecandidate=e=>{if(!e.candidate)return;const other=control[1-i];if(other.remoteDescription)void other.addIceCandidate(e.candidate);else pending[1-i].push(e.candidate);};
  const channels=[control[0].createDataChannel('preflight'),null];const incoming=new Promise(resolve=>control[1].ondatachannel=e=>{channels[1]=e.channel;resolve();});
  const offer=await control[0].createOffer();await control[0].setLocalDescription(offer);await control[1].setRemoteDescription(offer);for(const c of pending[1].splice(0))await control[1].addIceCandidate(c);const answer=await control[1].createAnswer();await control[1].setLocalDescription(answer);await control[0].setRemoteDescription(answer);for(const c of pending[0].splice(0))await control[0].addIceCandidate(c);await incoming;await Promise.all(channels.map(dc=>dc.readyState==='open'?Promise.resolve():new Promise((resolve,reject)=>{dc.addEventListener('open',resolve,{once:true});dc.addEventListener('error',reject,{once:true});}))); 
  const ids=['1111111111111111','2222222222222222'],callbacks=new Map(),events=[],sent=[],preparedSockets=[];const admin=await createAdminIdentity();
  const wireLimit=agreed?16384:65536;
  class LimitedRTC extends RTCPeerConnection{
   get sctp(){const real=super.sctp;return real?{state:real.state,maxMessageSize:Math.min(real.maxMessageSize,wireLimit)}:null;}
   createDataChannel(...args){const dc=super.createDataChannel(...args),send=dc.send.bind(dc);dc.send=data=>{const size=typeof data==='string'?new TextEncoder().encode(data).length:data.byteLength;if(size>wireLimit)throw Error('test message limit exceeded');if(typeof data==='string'&&data.includes('binary-fragment'))sent.push('fragment');return send(data);};return dc;}
  }
  const services=ids.map((id,i)=>createNetworkDistribution({networkId:'scout-fragment-browser',adminPublicKey:admin.publicKey,allowed:()=>true,canUsePeer:()=>true,lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'scout-fragment-browser',action,target,...fields,operationId:crypto.randomUUID()})},runtime:{crypto,RTCPeerConnection:LimitedRTC},options:{prepareTransferTransport:async context=>{const stats=await context.pc.getStats();if(context.pc.connectionState!=='connected'||![...stats.values()].some(row=>row.type==='transport'&&row.selectedCandidatePairId))throw Error('fresh socket not connected');preparedSockets.push({id:context.id,role:context.role});return {report:{status:'browser_test_observed'},release:()=>({status:'browser_test_released'})};},intervalMs:10,batchSize:32,requestWindow:8,maxChunkBytes:262144,capabilityExchange:true,rtcConfiguration:{iceServers:[]}},core:{scoutPeer:()=>scoutConnection(control[i],{channel:channels[i]}),getState:()=>({peerId:id,leader:ids[0],mirror:ids[1],role:i?'syscon2':'syscon1',state:'admitted',admitted:true}),onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{frame}){if(type==='PHANTOM_DIST_SEND')queueMicrotask(()=>callbacks.get(frame.to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));}},emit:(type,data)=>events.push({i,type,data})}));
  for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
  const wait=async predicate=>{const end=performance.now()+30000;while(!predicate()){const error=events.find(e=>e.type==='distributionError');if(error)throw Error(error.data.error);if(performance.now()>end)throw Error('browser transfer timed out');await new Promise(r=>setTimeout(r,10));}};
  try{
   services[0].requestCapabilities(ids[1]);services[1].requestCapabilities(ids[0]);await wait(()=>services[0].getCapabilities(ids[1])&&services[1].getCapabilities(ids[0]));
   const capability=services[0].getCapabilities(ids[1]);if(capability.observationScope!=='control'||capability.messageLimit.status!=='known')throw Error('missing preflight control observation');if(JSON.stringify(capability).includes('address')||JSON.stringify(capability).includes('stats'))throw Error('raw stats escaped');
   const bytes=Uint8Array.from({length:262144+17},(_,i)=>i%251),file=new File([bytes],'fixture');let chunkSize=262144;
   if(agreed){const id=services[0].proposePlan(ids[1],{maxPayloadBytes:262144,allowFragmentation:true}),other=services[1].proposePlan(ids[0],{maxPayloadBytes:262144,allowFragmentation:true});await wait(()=>services[0].getPlan(ids[1])?.id===id&&services[1].getPlan(ids[0])?.id===other);for(const plan of [services[0].getPlan(ids[1]).plan,services[1].getPlan(ids[0]).plan])if(plan.observations.local.scope!=='control'||plan.observations.remote.scope!=='control')throw Error('missing plan observations');const prepared=await services[0].prepareAgreed(file,ids[1]);chunkSize=prepared.chunkSize;await services[0].publish(file,{prepared,recipients:[ids[1]]});}
   else await services[0].publish(file,{chunkSize,recipients:[ids[1]]});
   await wait(()=>events.some(e=>e.i===1&&e.type==='distributionComplete'));const blob=events.find(e=>e.i===1&&e.type==='distributionComplete').data.blob,received=new Uint8Array(await blob.arrayBuffer());
   if(!preparedSockets.some(row=>row.role==='source')||!preparedSockets.some(row=>row.role==='receiver'))throw Error('fresh socket preparation missing');
   return {preparedSockets:preparedSockets.length,agreed,chunkSize,wireLimit,expected:bytes.length,received:received.length,identical:received.every((v,i)=>v===bytes[i]),fragments:sent.length};
  }finally{services.forEach(s=>s.close());control.forEach(pc=>pc.close());}
 },{agreed:process.env.PHANTOM_SCOUT_AGREED==='1'});
 assert.deepEqual(errors,[]);assert.equal(result.expected,262161);assert.equal(result.received,result.expected);assert.equal(result.identical,true);assert.equal(result.chunkSize,262144);assert.ok(result.fragments>=4);console.log(JSON.stringify(result));
}finally{await browser?.close();await new Promise(r=>server.close(r));}
