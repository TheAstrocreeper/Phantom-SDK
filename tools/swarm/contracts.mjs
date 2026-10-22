import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createConnection} from 'node:net';
import {chromium} from 'playwright';
import {build} from 'esbuild';
import {startServer} from './server.mjs';
import {mkdirSync,writeFileSync,appendFileSync,readFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {join,resolve} from 'node:path';
const root=fileURLToPath(new URL('../../',import.meta.url));
const output=resolve(process.argv[2]??join(root,'tools/swarm/reports',new Date().toISOString().replaceAll(':','-')+'-contracts'));
mkdirSync(output,{recursive:true});
const clients=[],steps=[],events=[];
const turnTransport=process.env.PHANTOM_TURN_TRANSPORT;
if(turnTransport!==undefined&&!['udp','tcp'].includes(turnTransport))throw Error('PHANTOM_TURN_TRANSPORT must be udp or tcp');
const rtcConfiguration=turnTransport?{iceTransportPolicy:'relay',iceServers:[{urls:`turn:127.0.0.1:34789?transport=${turnTransport}`,username:'phantom-test',credential:crypto.randomUUID()}]}:{iceServers:[]};
const summary={scenario:turnTransport?'turn-sdk-contracts':'sdk-contracts',turnTransport,status:'running',network:'single-host loopback; native WebRTC; no NAT/loss emulation',steps};
function hashes(directory){const result={};for(const e of readdirSync(directory,{withFileTypes:true})){const p=join(directory,e.name);if(e.isDirectory()&&e.name!=='node_modules')Object.assign(result,hashes(p));else if(e.isFile()&&/\.(js|mjs|json)$/.test(e.name))result[p.slice(root.length)]=createHash('sha256').update(readFileSync(p)).digest('hex');}return result;}
summary.sdkSourceHashes=hashes(join(root,'packages'));summary.harnessSourceHashes=Object.fromEntries(['contracts.mjs','contracts-peer.js','server.mjs','package-lock.json'].map(n=>[n,createHash('sha256').update(readFileSync(join(root,'tools/swarm',n))).digest('hex')]));
summary.sdkRevision=execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8'}).trim();summary.dirty=!!execFileSync('git',['status','--porcelain'],{cwd:root,encoding:'utf8'}).trim();
const save=()=>writeFileSync(join(output,'summary.json'),JSON.stringify(summary,null,2));
function log(peer,type,data){const row={at:new Date().toISOString(),peer,type,data};events.push(row);appendFileSync(join(output,'events.jsonl'),JSON.stringify(row)+'\n');}
async function until(check,label){const end=Date.now()+60000;let last;while(Date.now()<end){last=await check();if(last)return last;await new Promise(r=>setTimeout(r,100));}throw Error('Timed out: '+label);}
async function step(name,action){console.log('RUN',name);const start=Date.now();try{const evidence=await action();steps.push({name,status:'passed',durationMs:Date.now()-start,evidence});console.log('PASS',name);}catch(e){steps.push({name,status:'failed',error:e.stack});throw e;}finally{save();}}
let browser,server,turn,turnError;
const inspect=c=>c.page.evaluate(()=>contracts.inspect());
async function open(label,invite){const context=await browser.newContext(),page=await context.newPage(),c={label,context,page};clients.push(c);await page.exposeBinding('harnessLog',(_,e)=>log(label,e.type,e.data));page.on('pageerror',e=>log(label,'pageerror',{error:e.stack}));await page.goto(server.url);await page.waitForFunction(()=>!!globalThis.contracts);Object.assign(c,await page.evaluate(v=>contracts.open(v),{label,invite,rtcConfiguration}));return c;}
async function ready(){return until(async()=>{const rows=await Promise.all(clients.map(inspect));return rows.every(r=>r.state.state==='admitted'&&r.connections.length===2&&r.connections.every(p=>p.state==='ready'))&&rows;},'three peers admitted with automatic messaging ready');}
async function reload(c,invite){await c.page.reload();await c.page.waitForFunction(()=>!!globalThis.contracts);const info=await c.page.evaluate(v=>contracts.open(v),{label:c.label,invite,rtcConfiguration});assert.equal(info.peerId,c.peerId,'reloaded peer retains identity');return info;}
try{
 if(turnTransport){
  summary.turnVersion=execFileSync('turnserver',['--version'],{encoding:'utf8'}).trim();
  turn=spawn('turnserver',['-n','--listening-ip=127.0.0.1','--relay-ip=127.0.0.1','--listening-port=34789','--min-port=49160','--max-port=49260','--realm=phantom-test','--lt-cred-mech','--user=phantom-test:'+rtcConfiguration.iceServers[0].credential,'--fingerprint','--allow-loopback-peers','--cli-password='+crypto.randomUUID(),'--no-cli','--no-tls','--no-dtls','--no-multicast-peers','--log-file=stdout','--simple-log','--pidfile='+join(output,'coturn.pid')]);
  turn.stdout.on('data',data=>appendFileSync(join(output,'coturn.log'),data));turn.stderr.on('data',data=>appendFileSync(join(output,'coturn.log'),data));turn.on('error',error=>{turnError=error;});
  await until(()=>new Promise(resolve=>{
   if(turnError)throw turnError;if(turn.exitCode!==null)throw Error('Coturn exited: '+turn.exitCode);
   const socket=createConnection({host:'127.0.0.1',port:34789});socket.setTimeout(1000);
   socket.once('connect',()=>{socket.destroy();resolve(true);});socket.once('error',()=>{socket.destroy();resolve(false);});socket.once('timeout',()=>{socket.destroy();resolve(false);});
  }),'Coturn TCP listener ready');
 }
 const bundle=await build({entryPoints:[join(root,'tools/swarm/contracts-peer.js')],bundle:true,write:false,format:'esm',target:'es2022'});
 server=await startServer(bundle.outputFiles[0].text,(type,data)=>log('relay',type,data));
 browser=await chromium.launch({headless:true,...(process.env.PHANTOM_CHROMIUM_PATH?{executablePath:process.env.PHANTOM_CHROMIUM_PATH}:{}),args:['--no-sandbox','--disable-dev-shm-usage']});summary.browserVersion=browser.version();
 const source=await open('source'),receiver=await open('selected',source.invite),excluded=await open('excluded',source.invite);
 await step('automatic messaging delivers before and after native channel loss',async()=>{
  await ready();await source.page.evaluate(to=>contracts.send({to,text:'before-close'}),receiver.peerId);
  await until(async()=>(await inspect(receiver)).messages.some(m=>m.from===source.peerId&&m.text==='before-close'),'initial message');
  const prior=(await inspect(source)).connections.find(p=>p.peerId===receiver.peerId).attemptId;
  const closed=await source.page.evaluate(()=>contracts.breakMessaging());
  await until(async()=>{const r=await inspect(source),p=r.connections.find(p=>p.peerId===receiver.peerId);return p?.state==='ready'&&p.attemptId!==prior&&r.connectionEvents.some(e=>e.peerId===receiver.peerId&&e.state!=='ready');},'new native messaging attempt');
  await ready();await source.page.evaluate(to=>contracts.send({to,text:'after-close'}),receiver.peerId);
  await until(async()=>(await inspect(receiver)).messages.some(m=>m.from===source.peerId&&m.text==='after-close'),'recovered message');return {closed,oldAttempt:prior,newAttempt:(await inspect(source)).connections.find(p=>p.peerId===receiver.peerId).attemptId};
 });
 let fixture,retained;
 await step('selected receiver stops after one durably stored chunk',async()=>{
  await receiver.page.evaluate(()=>contracts.armPartial());fixture=await source.page.evaluate(recipients=>contracts.publish({recipients}),[receiver.peerId]);
  await until(async()=>(await inspect(receiver)).interrupted,'receiver storage interruption');
  retained=await receiver.page.evaluate(()=>contracts.chunkKeys());assert.deepEqual(retained,[fixture.id+':0']);
  assert.ok((await receiver.page.evaluate(id=>contracts.localFile(id),fixture.id)).error);
  return {id:fixture.id,retained,total:fixture.total};
 });
 await step('page reload restores publisher and resumes only missing chunks',async()=>{
  await source.page.evaluate(()=>contracts.close());
  await Promise.all([reload(source,source.invite),reload(receiver,source.invite)]);
  const restored=(await inspect(receiver)).distribution.restoreResults.find(r=>r.id===fixture.id);
  assert.equal(restored?.received,1);assert.equal(restored.complete,false);
  await ready();
  const complete=await until(async()=>{const f=await receiver.page.evaluate(id=>contracts.localFile(id),fixture.id);return !f.error&&f;},'verified resumed file');
  assert.equal(complete.size,fixture.size);assert.equal(complete.sha256,fixture.sha256);
  const sourceFile=await source.page.evaluate(id=>contracts.localFile(id),fixture.id);assert.deepEqual(sourceFile,complete);
  const traces=(await inspect(receiver)).trace.filter(t=>t.kind==='PHANTOM_FILE_REQUEST'&&t.direction==='send');
  assert.ok(traces.length>0);assert.ok(traces.every(t=>t.index!==0),'retained chunk is never requested again');
  return {restored,complete,requestedIndexes:traces.map(t=>t.index)};
 });
 await step('excluded peer receives no chunks or assigned receiver jobs',async()=>{
  const rows=await ready();
  await until(async()=>{const states=await Promise.all(clients.map(inspect));return states.every(s=>s.distribution.jobs.length===0&&s.distribution.connections.length===0);},'distribution quiescence');
  assert.deepEqual(await excluded.page.evaluate(()=>contracts.chunkKeys()),[]);
  assert.ok((await excluded.page.evaluate(id=>contracts.localFile(id),fixture.id)).error);
  assert.equal(events.some(e=>e.type==='distributionJob'&&e.data.receiver===excluded.peerId),false);
  const state=await inspect(excluded);assert.equal(state.trace.some(t=>t.direction==='receive'&&t.kind==='binary'),false);
  assert.equal(events.some(e=>e.type==='pageerror'),false,'no browser script errors');
  return {excluded:excluded.peerId,chunks:0,receiverJobs:0,allPeersReady:rows.length};
 });
 if(turnTransport&&process.env.PHANTOM_TURN_FAILURES==='1'){
  for(const fault of ['unavailable','credentials'])await step(`TURN ${fault} rejects admission, disposes RTC and permits a fresh join`,async()=>{
   const context=await browser.newContext(),page=await context.newPage(),label='fault-'+fault;
   await page.exposeBinding('harnessLog',(_,e)=>log(label,e.type,e.data));
   await page.goto(server.url);await page.waitForFunction(()=>!!globalThis.contracts);
   const bad=structuredClone(rtcConfiguration);
   if(fault==='unavailable')bad.iceServers[0].urls=`turn:127.0.0.1:34790?transport=${turnTransport}`;
   else bad.iceServers[0].credential=crypto.randomUUID();
   const start=events.length;
   try{
    const failure=await page.evaluate(async args=>{try{await contracts.open(args);return null;}catch(error){return {message:error.message,code:error.code,diagnostics:error.diagnostics};}},{label,invite:source.invite,rtcConfiguration:bad,timeoutMs:15000});
    assert.equal(failure.message,'Network admission timed out');
    assert.equal(failure.code,'ADMISSION_TIMEOUT');
    assert.ok(failure.diagnostics.rtc.connections.length>0,'rejected join retains native RTC diagnostics');
    const iceErrors=failure.diagnostics.rtc.connections.flatMap(c=>c.events.filter(e=>e.reason==='ice_candidate_error').map(e=>e.detail));
    if(fault==='credentials')assert.ok(iceErrors.some(e=>e.errorCode===401),'browser reports rejected TURN credentials');
    const failed=await page.evaluate(()=>contracts.inspect()),attempt=events.slice(start).filter(e=>e.peer===label);
    assert.ok(attempt.some(e=>e.type==='rtc.created'&&e.data.policy==='relay'),'failed attempt constructs real relay-only RTC');
    assert.equal(attempt.some(e=>e.type==='sdk.state'&&e.data.state==='admitted'),false,'failed TURN must never admit');
    assert.equal(attempt.some(e=>e.type==='rtc.selectedPair'),false,'failed TURN has no selected relay pair');
    assert.deepEqual(failed.nativeConnections,[],'rejected join disposes every native RTC connection');
    assert.ok(attempt.some(e=>e.type==='sdk.event'&&e.data.type==='SNAPSHOT'),'SDK diagnostics exist for failed attempt');
    await page.reload();await page.waitForFunction(()=>!!globalThis.contracts);
    const recovered=await page.evaluate(args=>contracts.open(args),{label,invite:source.invite,rtcConfiguration});
    assert.equal(recovered.peerId,failed.state.peerId,'fresh join retains failed identity');
    await until(async()=>{const r=await page.evaluate(()=>contracts.inspect());return r.state.state==='admitted'&&r.connections.some(p=>p.peerId===source.peerId&&p.state==='ready');},'failed TURN identity recovers with valid configuration');
    await page.evaluate(args=>contracts.send(args),{to:source.peerId,text:'recovered-'+fault});
    await until(async()=>(await inspect(source)).messages.some(m=>m.from===recovered.peerId&&m.text==='recovered-'+fault),'message from recovered TURN identity');
    await page.evaluate(()=>contracts.close());
    assert.deepEqual((await page.evaluate(()=>contracts.inspect())).nativeConnections,[]);
    return {failure,peerId:recovered.peerId,disposed:true,recoveredMessage:true};
   }finally{await context.close();}
  });
 }
 if(turnTransport)await step('selected native pairs use TURN for system, messaging and distribution',async()=>{
  await Promise.all(clients.map(c=>c.page.evaluate(()=>contracts.capturePairs())));
  const evidence=events.filter(e=>e.type==='rtc.selectedPair').map(e=>({peer:e.peer,...e.data}));
  for(const kind of ['system','messaging','distribution'])assert.ok(evidence.some(e=>e.kind===kind),'missing selected pair for '+kind);
  for(const e of evidence){assert.equal(e.policy,'relay');assert.equal(e.state,'succeeded');assert.equal(e.localType,'relay');assert.equal(e.remoteType,'relay');assert.equal(e.relayProtocol,turnTransport);}
  return evidence;
 });
 summary.status='passed';
}catch(e){summary.status='failed';summary.error=e.stack;console.error(e);process.exitCode=1;}
finally{
 summary.final=await Promise.all(clients.map(async c=>{try{return {peer:c.label,...await inspect(c)};}catch(e){return {peer:c.label,error:e.message};}}));save();
 await browser?.close();await server?.close();if(turn?.pid&&turn.exitCode===null&&turn.signalCode===null){turn.kill('SIGTERM');await new Promise(resolve=>turn.once('exit',resolve));}console.log('Report:',output);
}
