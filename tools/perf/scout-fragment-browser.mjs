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
 const result=await page.evaluate(async()=>{
  const {createNetworkDistribution}=await import('/packages/phantom-core/src/network-distribution.js');
  const {createAdminIdentity}=await import('/packages/phantom-core/src/authorization.js');
  const ids=['1111111111111111','2222222222222222'],callbacks=new Map(),events=[],sent=[];const admin=await createAdminIdentity();
  class LimitedRTC extends RTCPeerConnection{
   get sctp(){const real=super.sctp;return real?{state:real.state,maxMessageSize:Math.min(real.maxMessageSize,65536)}:null;}
   createDataChannel(...args){const dc=super.createDataChannel(...args),send=dc.send.bind(dc);dc.send=data=>{const size=typeof data==='string'?new TextEncoder().encode(data).length:data.byteLength;if(size>65536)throw Error('test message limit exceeded');if(typeof data==='string'&&data.includes('binary-fragment'))sent.push('fragment');return send(data);};return dc;}
  }
  const services=ids.map((id,i)=>createNetworkDistribution({networkId:'scout-fragment-browser',adminPublicKey:admin.publicKey,allowed:()=>true,canUsePeer:()=>true,lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'scout-fragment-browser',action,target,...fields,operationId:crypto.randomUUID()})},runtime:{crypto,RTCPeerConnection:LimitedRTC},options:{intervalMs:10,batchSize:32,requestWindow:8,maxChunkBytes:262144,capabilityExchange:true,rtcConfiguration:{iceServers:[]}},core:{getState:()=>({peerId:id,leader:ids[0],mirror:ids[1],role:i?'syscon2':'syscon1',state:'admitted',admitted:true}),onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{frame}){if(type==='PHANTOM_DIST_SEND')queueMicrotask(()=>callbacks.get(frame.to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));}},emit:(type,data)=>events.push({i,type,data})}));
  for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
  const wait=async predicate=>{const end=performance.now()+30000;while(!predicate()){const error=events.find(e=>e.type==='distributionError');if(error)throw Error(error.data.error);if(performance.now()>end)throw Error('browser transfer timed out');await new Promise(r=>setTimeout(r,10));}};
  try{
   services[0].requestCapabilities(ids[1]);await wait(()=>services[0].getCapabilities(ids[1]));
   const bytes=Uint8Array.from({length:262144+17},(_,i)=>i%251),file=new File([bytes],'fixture');await services[0].publish(file,{chunkSize:262144,recipients:[ids[1]]});
   await wait(()=>events.some(e=>e.i===1&&e.type==='distributionComplete'));const blob=events.find(e=>e.i===1&&e.type==='distributionComplete').data.blob,received=new Uint8Array(await blob.arrayBuffer());
   return {expected:bytes.length,received:received.length,identical:received.every((v,i)=>v===bytes[i]),fragments:sent.length};
  }finally{services.forEach(s=>s.close());}
 });
 assert.deepEqual(errors,[]);assert.equal(result.expected,262161);assert.equal(result.received,result.expected);assert.equal(result.identical,true);assert.ok(result.fragments>=4);console.log(JSON.stringify(result));
}finally{await browser?.close();await new Promise(r=>server.close(r));}
