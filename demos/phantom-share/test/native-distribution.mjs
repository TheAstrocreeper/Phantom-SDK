import http from 'node:http';
import assert from 'node:assert/strict';
import {build} from 'esbuild';
const {chromium}=await import(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES+'/playwright/index.mjs':'playwright');
const bundle=await build({stdin:{contents:`
import {createNetworkDistribution} from '../../../packages/phantom-core/src/network-distribution.js';
import {createAdminIdentity} from '../../../packages/phantom-core/src/authorization.js';
window.runDistribution=async reuseConnections=>{
 const admin=await createAdminIdentity(),ids=['1111111111111111','2222222222222222'],callbacks=new Map(),events=[],failures=[],pcs=[];
 const bytes=Uint8Array.from({length:32*1024*1024},(_,i)=>i%251),source=new Blob([bytes]);let completed;
 const received=new Promise(resolve=>completed=resolve);
 const services=ids.map((id,i)=>createNetworkDistribution({networkId:'native-loopback',adminPublicKey:admin.publicKey,
  core:{getState:()=>({peerId:id,leader:ids[0],mirror:ids[1],role:i?'syscon2':'syscon1',state:'admitted',admitted:true}),onEvent(fn){callbacks.set(id,fn);return()=>callbacks.delete(id);},command(type,{frame}){if(type==='PHANTOM_DIST_SEND')queueMicrotask(()=>callbacks.get(frame.to)?.({type:'PHANTOM_DIST',payload:{from:id,frame}}));}},
  lifecycle:{issue:(action,target)=>admin.sign({networkId:'native-loopback',action,target,operationId:crypto.randomUUID()})},runtime:{crypto,RTCPeerConnection:class {constructor(...args){const pc=new RTCPeerConnection(...args);pcs.push(pc);return pc;}}},
  options:{intervalMs:10,batchSize:8,reuseConnections,jobTimeoutMs:15000},emit:(type,data)=>{events.push({peer:id,type});if(type==='distributionError')failures.push(data.error);if(i===1&&type==='distributionComplete')completed(data.blob);}}));
 for(const fn of callbacks.values())fn({type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});
 try{
  const prepared=await services[0].prepare(source,{chunkSize:65536}),start=performance.now();await services[0].publish(source,{prepared});
  let timer;const blob=await Promise.race([received,new Promise((_,reject)=>timer=setTimeout(()=>reject(Error('native timeout '+JSON.stringify({failures,events:events.slice(-12),snapshots:services.map(s=>{const v=s.snapshot();return {restoration:v.restoration,jobs:v.jobs.length,connections:v.connections,assets:v.assets.map(a=>({total:a.total,holders:a.holders.map(h=>({peer:h.peer,pieces:h.indexes.length}))}))};}),rtc:pcs.map(p=>({state:p.connectionState,ice:p.iceConnectionState,signaling:p.signalingState,local:!!p.localDescription,remote:!!p.remoteDescription}))}))),15000))]).finally(()=>clearTimeout(timer));
  const elapsedMs=performance.now()-start;
  const hash=async value=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',value)),v=>v.toString(16).padStart(2,'0')).join('');
  return {reuseConnections,bytes:blob.size,elapsedMs:Math.round(elapsedMs),verified:await hash(await blob.arrayBuffer())===await hash(bytes),opens:events.filter(e=>e.peer===ids[1]&&e.type==='distributionConnectionOpened').length,reused:events.filter(e=>e.type==='distributionConnectionReused').length,failures};
 }finally{services.forEach(s=>s.close());}
};`,resolveDir:new URL('../src/',import.meta.url).pathname},bundle:true,format:'esm',write:false});
const server=http.createServer((req,res)=>{res.setHeader('content-type',req.url==='/bundle.js'?'text/javascript':'text/html');res.end(req.url==='/bundle.js'?bundle.outputFiles[0].text:'<script type="module" src="/bundle.js"></script>');});
await new Promise(r=>server.listen(0,'127.0.0.1',r));let browser;
try{
 browser=await chromium.launch({headless:true,...(process.env.PHANTOM_CHROMIUM_EXECUTABLE?{executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE}:{}),args:['--no-sandbox','--disable-gpu','--disable-dev-shm-usage','--disable-features=WebRtcHideLocalIpsWithMdns','--allow-loopback-in-peer-connection']});
 const page=await browser.newPage();await page.goto('http://127.0.0.1:'+server.address().port);await page.waitForFunction(()=>!!window.runDistribution);
 for(const reuse of [false,true]){
  const r=await page.evaluate(reuse=>window.runDistribution(reuse),reuse);console.log(JSON.stringify(r));assert.equal(r.verified,true);assert.equal(r.bytes,32*1024*1024);assert.deepEqual(r.failures,[]);
  if(reuse){assert.equal(r.opens,1);assert.equal(r.reused,63);}else assert.equal(r.opens,64);
 }
 console.log('Native loopback RTC only; not phone/NAT throughput validation.');
}finally{await browser?.close();await new Promise(r=>server.close(r));}
