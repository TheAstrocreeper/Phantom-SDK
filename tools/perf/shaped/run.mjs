/** Kernel-shaped native UDP, actual current SDK coordinator. No JS transfer-delay model. */
import {createServer} from 'node:http';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {readlinkSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {resolve,extname,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const require=createRequire(new URL('../../swarm/package.json',import.meta.url)),{chromium}=require('playwright');
assert.equal(readlinkSync('/proc/self/ns/net'),process.env.PHANTOM_SHAPED_NET);
assert.notEqual(process.env.PHANTOM_SHAPED_NET,process.env.PHANTOM_SHAPED_PARENT_NET);
const root=resolve(fileURLToPath(new URL('../../../',import.meta.url))),folder=resolve(process.argv[2]);
const rttMs=Number(process.env.PHANTOM_SHAPED_RTT_MS),baseline={batchSize:32,requestWindow:32};
const report={schema:1,scope:'Actual SDK current coordinator in separate native Chrome processes/network namespaces. Kernel netem shapes UDP only; TCP control bypasses impairment. Memory storage, no NAT, radio or imposed loss. Not a 5G capacity measurement.',revision:process.env.GITHUB_SHA??null,profile:{addedRttMs:rttMs,rateKbitPerSecondEachDirection:15600,queueLimitPackets:10000},rows:[],calibration:null,status:'running'};
await mkdir(folder,{recursive:true});
const save=()=>writeFile(resolve(folder,'results.json'),JSON.stringify(report,null,2)+'\n');
const server=createServer(async(req,res)=>{try{const path=decodeURIComponent(new URL(req.url,'http://localhost').pathname),target=resolve(root,'.'+path);if(!target.startsWith(root+sep)||!/^\/(packages\/|tools\/perf\/reuben\/)/.test(path)||!['.js','.mjs','.html'].includes(extname(target))){res.writeHead(403).end();return;}res.setHeader('Content-Type',extname(target)==='.html'?'text/html':'text/javascript');res.setHeader('Cache-Control','no-store');res.end(await readFile(target));}catch{res.writeHead(404).end();}});
await new Promise(r=>server.listen(8080,'10.211.0.1',r));
const browsers=[];
async function contextsFor(){const contexts=await Promise.all(browsers.map(b=>b.newContext())),pages=await Promise.all(contexts.map(c=>c.newPage()));await Promise.all(pages.map(p=>p.goto('http://127.0.0.1:8080/tools/perf/reuben/browser.html')));await Promise.all(pages.map(p=>p.waitForFunction(()=>window.ready)));return {contexts,pages};}
function qdiscs(){return [1,2].map(n=>JSON.parse(execFileSync('ip',['netns','exec',`shaped${n}`,'tc','-s','-j','qdisc','show','dev','eth0'],{encoding:'utf8'})));}
async function calibrate(){
 const {contexts,pages}=await contextsFor();
 try{
  for(const p of pages)await p.evaluate(()=>{window.probe={pc:new RTCPeerConnection({iceServers:[]}),candidates:[],dc:null};probe.pc.onicecandidate=e=>{if(e.candidate)probe.candidates.push(e.candidate.toJSON());};probe.pc.ondatachannel=e=>{probe.dc=e.channel;probe.dc.onmessage=m=>probe.dc.send(m.data);};});
  const offer=await pages[0].evaluate(async()=>{probe.dc=probe.pc.createDataChannel('kernel-shaping-calibration');await probe.pc.setLocalDescription(await probe.pc.createOffer());return probe.pc.localDescription.toJSON();});
  const answer=await pages[1].evaluate(async offer=>{await probe.pc.setRemoteDescription(offer);await probe.pc.setLocalDescription(await probe.pc.createAnswer());return probe.pc.localDescription.toJSON();},offer);
  await pages[0].evaluate(answer=>probe.pc.setRemoteDescription(answer),answer);
  for(const p of pages)await p.waitForFunction(()=>probe.pc.iceGatheringState==='complete');
  for(let i=0;i<2;i++){const candidates=await pages[i].evaluate(()=>probe.candidates);await pages[1-i].evaluate(async candidates=>{for(const c of candidates)await probe.pc.addIceCandidate(c);},candidates);}
  await pages[0].waitForFunction(()=>probe.dc.readyState==='open');
  const result=await pages[0].evaluate(async()=>{const samples=[];for(let i=0;i<7;i++){const at=performance.now();await new Promise(resolve=>{probe.dc.onmessage=resolve;probe.dc.send('ping');});samples.push(performance.now()-at);}const stats=await probe.pc.getStats();let selected;stats.forEach(s=>{if(s.type==='transport'&&s.selectedCandidatePairId){const pair=stats.get(s.selectedCandidatePairId);selected={local:stats.get(pair.localCandidateId),remote:stats.get(pair.remoteCandidateId)};}});return {samples,selected};});
  for(const candidate of [result.selected.local,result.selected.remote]){assert.equal(candidate.protocol,'udp');assert.match(candidate.address,/^10\.211\.0\.[23]$/);}
  const median=[...result.samples].sort((a,b)=>a-b)[3];assert.ok(median>=rttMs*0.8,`UDP echo did not experience configured RTT: ${median}`);
  report.calibration={...result,medianRttMs:median,qdiscs:qdiscs()};await save();console.log('CALIBRATION',JSON.stringify({rttMs,measuredMs:median}));
 }finally{await Promise.all(contexts.map(c=>c.close()));}
}
async function runCase(settings,bytes,stage,pass){
 const {contexts,pages}=await contextsFor(),errors=[];
 try{
  for(let i=0;i<2;i++){pages[i].on('pageerror',e=>errors.push(e.message));await pages[i].exposeFunction('bridge',message=>pages[1-i].evaluate(async message=>{try{await endpoint.handle(message);}catch(e){window.failure=e.message;}},message));}
  const key=await pages[0].evaluate(()=>createAdmin()),ids=['1111111111111111','2222222222222222'];
  await Promise.all(pages.map((p,i)=>p.evaluate(options=>configure(options),{engine:'current',currentSettings:settings,role:i?'receiver':'sender',selfId:ids[i],peerId:ids[1-i],networkId:`shaped-${report.rows.length}`,adminPublicKey:key,bytes,chunkSize:65536})));
  await pages[0].evaluate(()=>{void endpoint.start().catch(e=>window.failure=e.message);});
  await pages[1].waitForFunction(()=>window.result||window.failure,null,{timeout:240000});
  const result=await pages[1].evaluate(()=>({result:window.result,error:window.failure}));assert.equal(result.error,undefined);assert.equal(result.result.verified,true);
  const sender=await pages[0].evaluate(async()=>{await endpoint.sample();return {report:endpoint.report(),error:window.failure};});assert.equal(sender.error,undefined);assert.deepEqual(errors,[]);
  const receiver=result.result;assert.equal(receiver.requestWindow,settings.requestWindow);assert.equal(receiver.batchSize,settings.batchSize);
  const routes=[...sender.report.samples,...receiver.samples].filter(s=>s.protocol!=='unknown');assert.ok(routes.length);assert.ok(routes.every(s=>s.protocol==='udp'&&s.localCandidateType==='host'&&s.remoteCandidateType==='host'));
  report.rows.push({stage,pass,settings,bytes,receiver,sender:sender.report});await save();console.log('CASE',JSON.stringify({stage,pass,settings,bytes,elapsedMs:receiver.elapsedMs,verified:true}));return receiver.elapsedMs;
 }catch(error){report.failedCase={stage,pass,settings,bytes,error:error.message};throw error;}finally{for(const p of pages)await p.evaluate(()=>window.endpoint?.close()).catch(()=>{});await Promise.all(contexts.map(c=>c.close()));}
}
const key=s=>`${s.batchSize}/${s.requestWindow}`;
try{
 for(let n=1;n<=2;n++)browsers.push(await chromium.launch({headless:true,executablePath:resolve(root,'tools/perf/shaped/chromium.sh'),env:{...process.env,PHANTOM_SHAPED_PEER:`shaped${n}`},args:['--no-sandbox','--disable-dev-shm-usage','--disable-features=WebRtcHideLocalIpsWithMdns']}));
 report.browser=browsers[0].version();await calibrate();
 const settings=[8,32,64].flatMap(batchSize=>[8,16,32].map(requestWindow=>({batchSize,requestWindow})));
 for(let pass=0;pass<2;pass++)for(const s of pass?settings.toReversed():settings)await runCase(s,8*1024*1024+17,'screen',pass);
 const means=settings.map(settings=>{const rows=report.rows.filter(r=>key(r.settings)===key(settings));return {settings,meanMs:rows.reduce((sum,r)=>sum+r.receiver.elapsedMs,0)/rows.length};}).sort((a,b)=>a.meanMs-b.meanMs);
 const candidate=means.find(r=>key(r.settings)!==key(baseline)).settings;report.screenMeans=means;report.candidate=candidate;await save();
 for(let pass=0;pass<2;pass++)for(const s of pass?[candidate,baseline]:[baseline,candidate])await runCase(s,128*1024*1024+17,'confirmation',pass);
 const mean=s=>{const rows=report.rows.filter(r=>r.stage==='confirmation'&&key(r.settings)===key(s));return rows.reduce((sum,r)=>sum+r.receiver.elapsedMs,0)/rows.length;};
 report.confirmation={baseline,candidate,baselineMs:mean(baseline),candidateMs:mean(candidate),elapsedReductionPercent:(1-mean(candidate)/mean(baseline))*100};
 report.finalQdiscs=qdiscs();for(const peer of report.finalQdiscs){const q=peer.find(q=>q.kind==='netem');assert.ok(q&&q.bytes>0,'No shaped UDP bytes');assert.equal(q.drops,0,'Unexpected kernel queue drops confound latency-only profile');}
 report.status='passed';console.log('SUMMARY',JSON.stringify(report.confirmation));
}catch(error){report.status='failed';report.error=error.stack;process.exitCode=1;console.error(error);}finally{await save();await Promise.all(browsers.map(b=>b.close()));await new Promise(r=>server.close(r));}
