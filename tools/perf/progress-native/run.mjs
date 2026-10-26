/** Regression reproduction: actual native SCTP, large manifest, 1.2 Mbit/s kernel-shaped UDP. */
import {createServer} from 'node:http';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {readlinkSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const require=createRequire(new URL('../../swarm/package.json',import.meta.url)),{chromium}=require('playwright');
assert.equal(readlinkSync('/proc/self/ns/net'),process.env.PHANTOM_SHAPED_NET);assert.notEqual(process.env.PHANTOM_SHAPED_NET,process.env.PHANTOM_SHAPED_PARENT_NET);
const root=resolve(fileURLToPath(new URL('../../../',import.meta.url))),folder=resolve(process.argv[2]);await mkdir(folder,{recursive:true});
const before='e0ef47c9953aaaf977d27b3c9d9ad42cecf6bfad',old=new Map(['files.js','network-distribution.js'].map(name=>[name,execFileSync('git',['show',`${before}:packages/phantom-core/src/${name}`],{cwd:root,encoding:'utf8'})]));
const html=`<!doctype html><script type="module">
const prefix=new URL(location.href).searchParams.get('variant')==='before'?'/baseline':'';
const {createNetworkDistribution}=await import(prefix+'/packages/phantom-core/src/network-distribution.js');
const {createAdminIdentity}=await import('/packages/phantom-core/src/authorization.js');
const {createMemoryChunkStore}=await import('/packages/phantom-core/src/files.js');
window.makeAdmin=async()=>{window.admin=await createAdminIdentity();return admin.publicKey;};
window.configure=({selfId,peerId,publicKey})=>{const source=selfId==='1111111111111111';let callback;window.deliver=frame=>callback?.({type:'PHANTOM_DIST',payload:{from:peerId,frame}});window.record={offers:0,submittedChunks:0,verifiedChunks:0,batches:0,closes:[],retries:0};window.shutdown=false;
class RTC extends RTCPeerConnection{createDataChannel(...args){const dc=super.createDataChannel(...args),send=dc.send.bind(dc);dc.send=data=>{const result=send(data);if(data instanceof ArrayBuffer)record.submittedChunks++;else try{if(JSON.parse(data).sys==='FILE_OFFER')record.offers++;}catch{}return result;};this.fileChannel=dc;return dc;}close(){record.closes.push({queuedBytes:this.fileChannel?.bufferedAmount??0,harnessShutdown:window.shutdown});return super.close();}}
window.store=createMemoryChunkStore();window.d=createNetworkDistribution({networkId:'native-progress',adminPublicKey:publicKey,runtime:{crypto,RTCPeerConnection:RTC},allowed:()=>true,canUsePeer:()=>true,lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'native-progress',action,target,...fields,operationId:crypto.randomUUID()})},core:{getState:()=>({peerId:selfId,leader:'1111111111111111',mirror:'2222222222222222',role:source?'syscon1':'syscon2',state:'admitted',admitted:true}),onEvent(fn){callback=fn;return()=>callback=null;},command(type,{frame}){if(type!=='PHANTOM_DIST_SEND')return;if(frame.to===selfId)queueMicrotask(()=>callback?.({type:'PHANTOM_DIST',payload:{from:selfId,frame}}));else void bridge(frame);}},options:{chunkStore:store,batchSize:32,requestWindow:32,rtcConfiguration:{iceServers:[]}},emit:(type,data)=>{if(type==='distributionProgress')record.verifiedChunks=data.received;if(type==='distributionBatchComplete')record.batches++;if(type==='distributionFileEvent'&&data.fileEvent==='fileRetry')record.retries++;}});callback({type:'PRESENCE_ROSTER',payload:{list:[selfId,peerId].map(hKey=>({hKey}))}});};
window.publish=async()=>{const file={name:'large-manifest-fixture',type:'application/octet-stream',size:1200*1024*1024,slice(start,end){return new Blob([new Uint8Array(Math.min(end,this.size)-start)]);}};const prepared=await d.prepare(file,{chunkSize:65536,sourceReadBlockBytes:4194304});window.assetId=prepared.id;await d.publish(file,{prepared,recipients:['2222222222222222']});};window.ready=true;
</script>`;
const server=createServer(async(req,res)=>{try{const u=new URL(req.url,'http://localhost');if(u.pathname==='/'){res.setHeader('Content-Type','text/html');res.end(html);return;}const baseline=u.pathname.startsWith('/baseline/'),path=baseline?u.pathname.slice(9):u.pathname,target=resolve(root,'.'+path);if(!target.startsWith(root+sep)||!path.startsWith('/packages/')||!path.endsWith('.js')){res.writeHead(403).end();return;}res.setHeader('Content-Type','text/javascript');res.end(baseline&&old.has(path.split('/').at(-1))?old.get(path.split('/').at(-1)):await readFile(target));}catch(e){res.writeHead(404).end();}});await new Promise(r=>server.listen(8080,'10.211.0.1',r));
const browsers=[],results=[];let failed;
try{
 for(let n=1;n<=2;n++)browsers.push(await chromium.launch({headless:true,executablePath:resolve(root,'tools/perf/progress-native/chromium.sh'),env:{...process.env,PHANTOM_SHAPED_PEER:`shaped${n}`},args:['--no-sandbox','--disable-dev-shm-usage','--disable-features=WebRtcHideLocalIpsWithMdns']}));
 for(const variant of ['before','after']){
  const contexts=await Promise.all(browsers.map(b=>b.newContext())),pages=await Promise.all(contexts.map(c=>c.newPage())),errors=[];try{
   for(let i=0;i<2;i++){pages[i].on('pageerror',e=>errors.push(e.message));await pages[i].exposeFunction('bridge',frame=>pages[1-i].evaluate(frame=>deliver(frame),frame));}
   await Promise.all(pages.map(p=>p.goto(`http://127.0.0.1:8080/?variant=${variant}`)));await Promise.all(pages.map(p=>p.waitForFunction(()=>window.ready)));
   const key=await pages[0].evaluate(()=>makeAdmin()),ids=['1111111111111111','2222222222222222'];await Promise.all(pages.map((p,i)=>p.evaluate(options=>configure(options),{selfId:ids[i],peerId:ids[1-i],publicKey:key})));
   await pages[0].evaluate(()=>publish());const start=Date.now();
   try{await pages[1].waitForFunction(()=>record.batches>0,null,{timeout:35000});}catch(e){if(variant==='after')throw e;}
   const endpoints=await Promise.all(pages.map(p=>p.evaluate(()=>record))),id=await pages[0].evaluate(()=>assetId);let verifiedBytes=0;
   if(variant==='after')verifiedBytes=await pages[1].evaluate(async id=>{let total=0;for(let i=0;i<32;i++){const bytes=new Uint8Array(await store.get(id,i));if(bytes.length!==65536||bytes.some(b=>b!==0))throw Error('Native fixture byte mismatch');total+=bytes.length;}return total;},id);
   results.push({variant,elapsedMs:Date.now()-start,endpoints,independentlyVerifiedBytes:verifiedBytes});console.log('RESULT',JSON.stringify(results.at(-1)));
   assert.deepEqual(errors,[]);if(variant==='before'){assert.equal(endpoints[1].batches,0);assert.ok(endpoints[0].closes.some(c=>!c.harnessShutdown&&c.queuedBytes>0),'before must abandon queued data');}else{assert.ok(endpoints[1].batches>=1);assert.equal(endpoints[0].offers,1);assert.equal(endpoints[0].closes.filter(c=>!c.harnessShutdown).length,0);assert.equal(endpoints[1].retries,0);assert.equal(verifiedBytes,2097152);}
  }finally{for(const p of pages)await p.evaluate(()=>{window.shutdown=true;window.d?.close();}).catch(()=>{});await Promise.all(contexts.map(c=>c.close()));}
 }
 const qdiscs=[1,2].map(n=>JSON.parse(execFileSync('ip',['netns','exec',`shaped${n}`,'tc','-s','-j','qdisc','show','dev','eth0'],{encoding:'utf8'})));for(const qs of qdiscs){const q=qs.find(q=>q.kind==='netem');assert.ok(q.bytes>0);assert.equal(q.drops,0);}results.push({qdiscs});
}catch(error){failed=error;process.exitCode=1;console.error(error);}finally{await writeFile(resolve(folder,'results.json'),JSON.stringify({schema:1,before,profile:{addedRttMs:80,rateKbitPerSecond:1200,virtualFileBytes:1200*1024*1024,chunkBytes:65536,jobIdleBudgetMs:15000,scope:'Native Chrome/kernel-shaped UDP. Tests first verified batch of a virtual large file; does not claim full-file completion or phone-path throughput.'},passed:!failed,error:failed?.stack,results},null,2));await Promise.all(browsers.map(b=>b.close()));await new Promise(r=>server.close(r));}
