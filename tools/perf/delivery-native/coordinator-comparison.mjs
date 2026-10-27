import {createServer} from 'node:http';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {readlinkSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {resolve,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const require=createRequire(new URL('../../swarm/package.json',import.meta.url)),{chromium}=require('playwright');
assert.equal(readlinkSync('/proc/self/ns/net'),process.env.PHANTOM_SHAPED_NET);
assert.notEqual(process.env.PHANTOM_SHAPED_NET,process.env.PHANTOM_SHAPED_PARENT_NET);
const root=resolve(fileURLToPath(new URL('../../../',import.meta.url))),folder=resolve(process.argv[2]);await mkdir(folder,{recursive:true});
const server=createServer(async(req,res)=>{try{const path=new URL(req.url,'http://localhost').pathname;if(path==='/'){res.setHeader('Content-Type','text/html');res.end('<!doctype html><script type="module" src="/tools/perf/delivery-native/coordinator-comparison-fixture.js"></script>');return;}const target=resolve(root,'.'+path);if(!target.startsWith(root+sep)||!/^\/(packages|tools\/perf\/(delivery-native|continuous-prototype|lane-priority))\//.test(path)||!path.endsWith('.js')){res.writeHead(403).end();return;}res.setHeader('Content-Type','text/javascript');res.end(await readFile(target));}catch{res.writeHead(404).end();}});await new Promise(r=>server.listen(8080,'10.211.0.1',r));
const browsers=[],results=[];let failed;
try{
 for(let n=1;n<=2;n++)browsers.push(await chromium.launch({headless:true,executablePath:resolve(root,'tools/perf/delivery-native/chromium.sh'),env:{...process.env,PHANTOM_SHAPED_PEER:`shaped${n}`},args:['--no-sandbox','--disable-dev-shm-usage','--disable-features=WebRtcHideLocalIpsWithMdns']}));
 // Alternate the order in the repeat so startup/order effects do not always favor one case.
 const cases=[['sdk',65536,3],['assigned',65536,3],['mixed',65536,3]];
 for(let repeat=0;repeat<2;repeat++)for(const test of (repeat?[...cases].reverse():cases)){
  const [mode,messageBytes,parallel]=test,contexts=await Promise.all(browsers.map(b=>b.newContext())),pages=await Promise.all(contexts.map(c=>c.newPage())),pageErrors=[];let problem;
  try{
   for(let i=0;i<2;i++){pages[i].on('pageerror',e=>pageErrors.push(e.message));await pages[i].exposeFunction('bridge',frame=>pages[1-i].evaluate(frame=>deliver(frame),frame));await pages[i].exposeFunction('ice',(c,index)=>pages[1-i].evaluate(({c,index})=>addIce(c,index),{c,index}));}
   await Promise.all(pages.map(p=>p.goto('http://127.0.0.1:8080/')));await Promise.all(pages.map(p=>p.waitForFunction(()=>window.ready)));
   const publicKey=await pages[0].evaluate(()=>makeAdmin()),ids=['1111111111111111','2222222222222222'];await Promise.all(pages.map((p,i)=>p.evaluate(options=>configure(options),{selfId:ids[i],peerId:ids[1-i],publicKey,mode,messageBytes,parallel})));
   if(mode==='lanes'||mode==='lanes_fail'){for(let i=0;i<3;i++){const offer=await pages[0].evaluate(i=>window.offer(i),i),answer=await pages[1].evaluate(({offer,index})=>window.answer({offer,index}),{offer,index:i});await pages[0].evaluate(({answer,index})=>setRemote(answer,index),{answer,index:i});}await pages[0].waitForFunction(()=>rawChannels.length===3&&rawChannels.every(dc=>dc.readyState==='open'));}
   const start=Date.now();const publishing=pages[0].evaluate(()=>publish());publishing.catch(()=>{});
   let controlId;if(mode==='lanes'||mode==='lanes_fail'){await pages[0].waitForFunction(()=>typeof window.queueControl==='function',null,{timeout:30000});await pages[1].waitForFunction(()=>record.laneSeen[0]>=2,null,{timeout:60000});controlId=await pages[0].evaluate(()=>queueControl());if(mode==='lanes_fail')await pages[0].evaluate(()=>failLane(0));}
   try{await pages[1].waitForFunction(()=>record.complete||record.errors.length>0,null,{timeout:180000});}catch{problem='180-second receive deadline';}
   if (!problem){const receiver=await pages[1].evaluate(()=>record);if(receiver.errors.length)problem='Receiver failed: '+receiver.errors[0];}if (!problem) await publishing; else void publishing.catch(()=>{});
   const endpoints=await Promise.all(pages.map(p=>p.evaluate(async()=>{if(record.errors.length&&navigator.storage?.estimate)record.storageEstimate=await navigator.storage.estimate().catch(()=>null);return record;})));let verifiedBytes=0;
   if(!problem){const id=await pages[0].evaluate(()=>assetId);verifiedBytes=await pages[1].evaluate(id=>verify(id),id);assert.equal(verifiedBytes,64*1024*1024);if(mode==='lanes'||mode==='lanes_fail'){const control=endpoints[1].controls.find(c=>c.id===controlId);assert.ok(control,'control delivered');assert.ok(control.receivedAtBytes<64*1024*1024,'control arrived before file completed');if(mode==='lanes_fail')assert.equal(endpoints[0].reassignments,1);}}
   const r=endpoints[1],first=r.marks.find(m=>m.bytes===2*1024*1024),last=r.marks.at(-1),steadyMbps=first&&last&&last.bytes>first.bytes?(last.bytes-first.bytes)*8/(last.atMs-first.atMs)/1000:null;
   results.push({mode,messageBytes,parallel,repeat,elapsedMs:Date.now()-start,problem,verifiedBytes,steadyMbps,endpoints,pageErrors});console.log('RESULT',JSON.stringify({mode,messageBytes,parallel,repeat,problem,verifiedBytes,steadyMbps}));if(problem)throw Error(problem);assert.deepEqual(pageErrors,[]);assert.deepEqual(endpoints.flatMap(e=>e.errors),[]);
  }finally{for(const p of pages)await p.evaluate(()=>window.stop?.()).catch(()=>{});await Promise.all(contexts.map(c=>c.close()));}
 }
 const qdiscs=[1,2].map(n=>JSON.parse(execFileSync('ip',['netns','exec',`shaped${n}`,'tc','-s','-j','qdisc','show','dev','eth0'],{encoding:'utf8'})));for(const qs of qdiscs)assert.ok(qs.find(q=>q.kind==='netem')?.bytes>0);results.push({qdiscs});
}catch(error){failed=error;process.exitCode=1;console.error(error);}finally{await writeFile(resolve(folder,'results.json'),JSON.stringify({schema:1,source:process.env.GITHUB_SHA,profile:{addedRttMs:80,rateKbitPerSecond:15600,lossPercent:Number(process.env.PHANTOM_SHAPED_LOSS),fileBytes:64*1024*1024,scope:'Real SDK coordinator, 64 MiB verified. Compare current three-association striped jobs with experimental assigned jobs over separate connections. Two Chrome endpoints, simulated 80 ms RTT and independent random loss. No real-device claims. Two repetitions; excludes first 2 MiB from steady throughput.'},passed:!failed,error:failed?.stack,results},null,2));await Promise.all(browsers.map(b=>b.close()));await new Promise(r=>server.close(r));}

