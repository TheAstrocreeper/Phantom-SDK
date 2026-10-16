import assert from 'node:assert/strict';
import {mkdirSync,writeFileSync,appendFileSync,readFileSync,readlinkSync} from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {join,resolve} from 'node:path';
import {chromium} from 'playwright';
import {build} from 'esbuild';
import {startServer} from '../server.mjs';
const root=fileURLToPath(new URL('../../../',import.meta.url));
const [mode,folder]=process.argv.slice(2),output=resolve(folder);
assert.ok(['direct','blocked','turn-udp','turn-tcp'].includes(mode));
mkdirSync(output,{recursive:true});
const steps=[],events=[],clients=[],browsers=[];
const summary={scenario:mode,status:'running',network:'three private subnets, separate Linux MASQUERADE gateways, isolated simulated WAN; IPv6 disabled',steps};
const save=()=>writeFileSync(join(output,'summary.json'),JSON.stringify(summary,null,2));
function log(peer,type,data){const e={at:new Date().toISOString(),peer,type,data};events.push(e);appendFileSync(join(output,'events.jsonl'),JSON.stringify(e)+'\n');}
async function step(name,action){const start=Date.now();console.log('RUN',name);try{const evidence=await action();steps.push({name,status:'passed',durationMs:Date.now()-start,evidence});console.log('PASS',name);}catch(e){steps.push({name,status:'failed',error:e.stack});throw e;}finally{save();}}
async function until(check,label){const end=Date.now()+60000;while(Date.now()<end){const result=await check();if(result)return result;await new Promise(r=>setTimeout(r,100));}throw Error('Timed out: '+label);}
const rtcConfiguration={iceServers:[{urls:'stun:198.18.0.1:3478'}]};
if(mode.startsWith('turn-'))rtcConfiguration.iceServers.push({urls:`turn:198.18.0.1:3478?transport=${mode==='turn-tcp'?'tcp':'udp'}`,username:'phantom',credential:'disposable-test-password'});
// Policy remains "all": firewall restrictions, rather than forced relay policy, cause fallback.
const inspect=c=>c.page.evaluate(()=>contracts.inspect());
let server;
async function pageFor(label){
 const index=browsers.length+1;
 const browser=await chromium.launch({headless:true,executablePath:join(root,'tools/swarm/nat/chromium.sh'),env:{...process.env,PHANTOM_PEER_NS:'pp'+index,PHANTOM_REAL_CHROMIUM:process.env.PHANTOM_CHROMIUM_PATH??chromium.executablePath()},args:['--no-sandbox','--disable-dev-shm-usage','--disable-features=WebRtcHideLocalIpsWithMdns']});
 browsers.push(browser);summary.browserVersion=browser.version();
 const context=await browser.newContext(),page=await context.newPage(),client={label,context,page};clients.push(client);
 page.on('pageerror',e=>log(label,'pageerror',{message:e.stack}));
 await page.exposeBinding('harnessLog',(_,e)=>log(label,e.type,e.data));
 await page.goto('http://127.0.0.1:8080');await page.waitForFunction(()=>!!globalThis.contracts);
 return client;
}
async function open(label,invite){const c=clients.find(c=>c.label===label)??await pageFor(label);Object.assign(c,await c.page.evaluate(args=>contracts.open(args),{label,invite,rtcConfiguration}));return c;}
async function ready(){return until(async()=>{const rows=await Promise.all(clients.map(inspect));return rows.every(r=>r.state.state==='admitted'&&r.connections.length===2&&r.connections.every(c=>c.state==='ready'))&&rows;},'three peers ready across NAT');}
async function reload(c,invite){await c.page.reload();await c.page.waitForFunction(()=>!!globalThis.contracts);const result=await c.page.evaluate(args=>contracts.open(args),{label:c.label,invite,rtcConfiguration});assert.equal(result.peerId,c.peerId);}
try{
 const files=['contracts-peer.js','server.mjs','nat/run.mjs','nat/topology.sh','nat/chromium.sh','nat/isolated.sh','nat/suite.sh','run-nat-tests.sh','package-lock.json'];
 summary.sourceHashes=Object.fromEntries(files.map(file=>[file,createHash('sha256').update(readFileSync(join(root,'tools/swarm',file))).digest('hex')]));
 try{summary.revision=execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8',stdio:['ignore','pipe','ignore']}).trim();}catch{summary.revision=process.env.PHANTOM_REVISION??'not supplied';}
 const bundle=await build({entryPoints:[join(root,'tools/swarm/contracts-peer.js')],bundle:true,write:false,format:'esm',target:'es2022'});
 summary.browserBundleSha256=createHash('sha256').update(bundle.outputFiles[0].contents).digest('hex');
 server=await startServer(bundle.outputFiles[0].text,(type,data)=>log('signaling',type,data),{host:'198.18.0.1',port:8080});
 if(mode!=='blocked')await step('plain native WebRTC validates the network before SDK assertions',async()=>{
  const a=await pageFor('source'),b=await pageFor('selected');
  let probeReady=false;const pendingCandidates=[],probeErrors=[];
  async function deliverCandidate(to,candidate){try{await to.page.evaluate(c=>rawProbe.pc.addIceCandidate(c),candidate);}catch(e){probeErrors.push(e.message);}}
  for(const [from,to] of [[a,b],[b,a]])await from.page.exposeBinding('rawProbeIce',async(_,candidate)=>{
   log(from.label,'nativeProbe.candidate',candidate);
   if(probeReady)await deliverCandidate(to,candidate);else pendingCandidates.push({to,candidate});
  });
  for(const c of [a,b])await c.page.evaluate(config=>{
   const pc=new RTCPeerConnection(config),candidates=[];
   globalThis.rawProbe={pc,candidates,received:[],channel:null};
   pc.onicecandidate=e=>{if(e.candidate)void rawProbeIce(e.candidate.toJSON());};
   pc.ondatachannel=e=>{rawProbe.channel=e.channel;e.channel.onmessage=m=>rawProbe.received.push(m.data);};
  },rtcConfiguration);
  try{
   const offer=await a.page.evaluate(async()=>{rawProbe.channel=rawProbe.pc.createDataChannel('network-probe');rawProbe.channel.onmessage=e=>rawProbe.received.push(e.data);await rawProbe.pc.setLocalDescription(await rawProbe.pc.createOffer());return rawProbe.pc.localDescription.toJSON();});
   const answer=await b.page.evaluate(async offer=>{await rawProbe.pc.setRemoteDescription(offer);await rawProbe.pc.setLocalDescription(await rawProbe.pc.createAnswer());return rawProbe.pc.localDescription.toJSON();},offer);
   await a.page.evaluate(answer=>rawProbe.pc.setRemoteDescription(answer),answer);
   probeReady=true;
   for(const {to,candidate} of pendingCandidates.splice(0))await deliverCandidate(to,candidate);
   const deadline=Date.now()+35000;let ready=false;
   while(Date.now()<deadline){
    ready=(await Promise.all([a,b].map(c=>c.page.evaluate(()=>rawProbe.channel?.readyState==='open')))).every(Boolean);
    if(ready)break;await new Promise(r=>setTimeout(r,100));
   }
   const stats=await Promise.all([a,b].map(c=>c.page.evaluate(async()=>({state:rawProbe.pc.connectionState,stats:[...await rawProbe.pc.getStats()].map(([,v])=>v)}))));
   summary.nativeProbePolicy='all';summary.nativeProbeErrors=probeErrors;summary.nativeNetworkProbe=stats;save();
   assert.deepEqual(probeErrors,[],'native control candidate application');
   assert.ok(ready,'Plain WebRTC failed: investigate topology/native ICE before the SDK');
   await a.page.evaluate(()=>rawProbe.channel.send('network-probe'));
   await until(()=>b.page.evaluate(()=>rawProbe.received.includes('network-probe')),'native probe payload');
   return stats;
  }finally{for(const c of [a,b])await c.page.evaluate(()=>rawProbe.pc.close());}
 });
 const source=await open('source');
 if(mode==='blocked'){
  await step('blocked direct traffic without TURN cannot admit a peer',async()=>{
   const receiver=await pageFor('blocked-receiver');
   const failure=await receiver.page.evaluate(async args=>{try{await contracts.open(args);return null;}catch(e){return {code:e.code,message:e.message,diagnostics:e.diagnostics};}},{label:receiver.label,invite:source.invite,rtcConfiguration,timeoutMs:15000});
   assert.equal(failure?.code,'ADMISSION_TIMEOUT');
   assert.equal(events.some(e=>e.peer===receiver.label&&e.type==='rtc.selectedPair'),false);
   assert.deepEqual((await inspect(receiver)).nativeConnections,[]);
   return failure;
  });
 }else{
  const receiver=await open('selected',source.invite),excluded=await open('excluded',source.invite);
  await step('messaging crosses NAT and recovers after native data channel closure',async()=>{
   await ready();await source.page.evaluate(to=>contracts.send({to,text:'before-close'}),receiver.peerId);
   await until(async()=>(await inspect(receiver)).messages.some(m=>m.text==='before-close'&&m.from===source.peerId),'initial message');
   const prior=(await inspect(source)).connections.find(c=>c.peerId===receiver.peerId).attemptId;
   await source.page.evaluate(()=>contracts.breakMessaging());
   await until(async()=>{const p=(await inspect(source)).connections.find(c=>c.peerId===receiver.peerId);return p?.state==='ready'&&p.attemptId!==prior;},'new messaging attempt');
   await ready();await source.page.evaluate(to=>contracts.send({to,text:'after-close'}),receiver.peerId);
   await until(async()=>(await inspect(receiver)).messages.some(m=>m.text==='after-close'&&m.from===source.peerId),'recovered message');
   return {oldAttempt:prior,newAttempt:(await inspect(source)).connections.find(c=>c.peerId===receiver.peerId).attemptId};
  });
  let fixture;
  await step('receiver persists exactly one chunk before interruption',async()=>{
   await receiver.page.evaluate(()=>contracts.armPartial());
   fixture=await source.page.evaluate(recipients=>contracts.publish({recipients}),[receiver.peerId]);
   await until(async()=>(await inspect(receiver)).interrupted,'durable first chunk');
   const keys=await receiver.page.evaluate(()=>contracts.chunkKeys());assert.deepEqual(keys,[fixture.id+':0']);
   return {id:fixture.id,keys};
  });
  await step('reload resumes missing chunks across NAT and verifies SHA-256',async()=>{
   await source.page.evaluate(()=>contracts.close());
   await Promise.all([reload(source,source.invite),reload(receiver,source.invite)]);
   const restored=(await inspect(receiver)).distribution.restoreResults.find(r=>r.id===fixture.id);
   assert.equal(restored?.received,1);assert.equal(restored.complete,false);
   await ready();
   const complete=await until(async()=>{const f=await receiver.page.evaluate(id=>contracts.localFile(id),fixture.id);return !f.error&&f;},'resumed file');
   assert.equal(complete.size,fixture.size);assert.equal(complete.sha256,fixture.sha256);
   assert.deepEqual(await source.page.evaluate(id=>contracts.localFile(id),fixture.id),complete);
   const requests=(await inspect(receiver)).trace.filter(t=>t.kind==='PHANTOM_FILE_REQUEST'&&t.direction==='send');
   assert.ok(requests.length);assert.ok(requests.every(t=>t.index!==0));
   return {restored,complete,requestedIndexes:requests.map(t=>t.index)};
  });
  await step('excluded peer receives no file chunks or receiver jobs',async()=>{
   await until(async()=>(await Promise.all(clients.map(inspect))).every(s=>s.distribution.jobs.length===0&&s.distribution.connections.length===0),'distribution idle');
   assert.deepEqual(await excluded.page.evaluate(()=>contracts.chunkKeys()),[]);
   assert.ok((await excluded.page.evaluate(id=>contracts.localFile(id),fixture.id)).error);
   assert.equal(events.some(e=>e.type==='distributionJob'&&e.data.receiver===excluded.peerId),false);
   assert.equal((await inspect(excluded)).trace.some(t=>t.kind==='binary'&&t.direction==='receive'),false);
   return {chunks:0,receiverJobs:0};
  });
  await step('live transfer survives twelve seconds of bidirectional packet loss without reload',async()=>{
   // Only mutate the disposable namespace created by the guarded outer runner.
   const namespace=readlinkSync('/proc/self/ns/net');
   assert.equal(process.env.PHANTOM_NAT_ISOLATED_NET_NS,namespace);
   assert.notEqual(process.env.PHANTOM_NAT_PARENT_NET_NS,namespace);
   const firewall=(...args)=>execFileSync('ip',['netns','exec','pg2','iptables',...args],{encoding:'utf8'});
   const rule=['FORWARD','-m','comment','--comment','phantom-live-outage','-j','DROP'];
   let installed=false,triggered,release;
   const started=new Promise(resolve=>{triggered=resolve;});
   const gate=new Promise(resolve=>{release=resolve;});
   const token=await receiver.page.evaluate(()=>globalThis.liveTestDocument=crypto.randomUUID());
   const peerId=(await inspect(receiver)).state.peerId;
   await receiver.page.exposeBinding('harnessOutage',async(_,chunk)=>{
    firewall('-I',...rule);installed=true;
    log(receiver.label,'fault.networkDropStarted',chunk);triggered(chunk);
    await gate;
   });
   try{
    await receiver.page.evaluate(()=>contracts.armLiveOutage());
    const file=await source.page.evaluate(recipients=>contracts.publish({recipients,size:2097152}),[receiver.peerId]);
    const first=await Promise.race([started,new Promise((_,reject)=>{const timer=setTimeout(()=>reject(Error('No durable chunk reached outage gate')),15000);timer.unref();})]);
    assert.equal(first.id,file.id);assert.equal(first.index,0);
    // Release storage only after the firewall is active: the next chunk request must cross the broken path.
    release();
    const before=(await receiver.page.evaluate(()=>contracts.chunkKeys())).filter(k=>k.startsWith(file.id+':'));
    await new Promise(resolve=>setTimeout(resolve,12000));
    const during=(await receiver.page.evaluate(()=>contracts.chunkKeys())).filter(k=>k.startsWith(file.id+':'));
    assert.deepEqual(during,before,'transfer must stall during the outage');
    assert.ok((await receiver.page.evaluate(id=>contracts.localFile(id),file.id)).error,'file must remain incomplete while blocked');
    const counters=firewall('-L','FORWARD','-v','-x','-n');
    const dropped=counters.split('\n').find(line=>line.includes('phantom-live-outage'));
    assert.ok(Number(dropped?.trim().split(/\s+/)[0])>0,'fault must actually drop packets');
    firewall('-D',...rule);installed=false;log(receiver.label,'fault.networkRestored',{durationMs:12000,dropped});
    const complete=await until(async()=>{const result=await receiver.page.evaluate(id=>contracts.localFile(id),file.id);return !result.error&&result;},'live transfer recovery');
    assert.equal(complete.sha256,file.sha256);assert.equal(complete.size,file.size);
    assert.equal(await receiver.page.evaluate(()=>globalThis.liveTestDocument),token,'same browser document');
    assert.equal((await inspect(receiver)).state.peerId,peerId);
    await ready();
    await source.page.evaluate(to=>contracts.send({to,text:'after-live-outage'}),receiver.peerId);
    await until(async()=>(await inspect(receiver)).messages.some(m=>m.text==='after-live-outage'&&m.from===source.peerId),'message after live outage');
    assert.deepEqual(await excluded.page.evaluate(()=>contracts.chunkKeys()),[]);
    return {fileId:file.id,outageMs:12000,storedDuringOutage:during,dropped,complete,sameDocument:true,samePeerId:true};
   }finally{release();if(installed)firewall('-D',...rule);}
  });
  await step('native ICE evidence confirms the expected network path',async()=>{
   const pairs=events.filter(e=>e.type==='rtc.selectedPair').map(e=>({peer:e.peer,...e.data}));
   for(const kind of ['system','messaging','distribution'])assert.ok(pairs.some(e=>e.kind===kind),'missing selected pair for '+kind);
   for(const p of pairs){
    assert.equal(p.state,'succeeded');
    if(mode==='direct'){
     assert.notEqual(p.localType,'relay');assert.notEqual(p.remoteType,'relay');
     assert.ok(p.localType==='srflx'||p.remoteType==='srflx'||p.localType==='prflx'||p.remoteType==='prflx','NAT must produce a reflexive candidate');
    }else{
     assert.ok(p.localType==='relay'||p.remoteType==='relay','blocked direct path requires TURN');
     if(p.localType==='relay')assert.equal(p.relayProtocol,mode==='turn-tcp'?'tcp':'udp');
    }
   }
   assert.equal(events.some(e=>e.type==='pageerror'),false);
   return pairs;
  });
 }
 summary.status='passed';
}catch(e){summary.status='failed';summary.error=e.stack;console.error(e);process.exitCode=1;}
finally{
 summary.final=await Promise.all(clients.map(async c=>{try{return {peer:c.label,...await inspect(c)};}catch(e){return {peer:c.label,error:e.message};}}));save();
 await Promise.all(browsers.map(b=>b.close()));await server?.close();console.log('Report:',output);
}
