import {chromium} from 'playwright';
import {build} from 'esbuild';
import {transportConfig} from './live-transport.mjs';
import {startServer} from './server.mjs';
import {importAdminIdentity} from '../../packages/phantom-core/src/authorization.js';
import {createPublishAuthorizer} from './publish-authorization.mjs';
import {mkdirSync,writeFileSync,appendFileSync,readFileSync,readdirSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {resolve,join} from 'node:path';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
const root=fileURLToPath(new URL('../../',import.meta.url));
const args=process.argv.slice(2);
function option(key,fallback){const i=args.indexOf('--'+key);return i<0?fallback:args[i+1];}
const known=new Set(['--peers','--bytes','--timeout','--output','--headed','--scenario','--nostr','--relays','--cf','--batch-size']);
for(let i=0;i<args.length;i++){if(!known.has(args[i]))throw Error('Unknown option: '+args[i]);if(!['--headed','--nostr'].includes(args[i])){if(args[i+1]===undefined||args[i+1].startsWith('--'))throw Error('Missing option value: '+args[i]);i++;}}
const transport=transportConfig({nostr:args.includes('--nostr'),relays:option('relays',undefined),cf:option('cf',undefined)});
const scenario=option('scenario','baseline');if(!['baseline','stress','peer-recovery','peer-bridge','early-successor'].includes(scenario))throw Error('Scenario must be baseline, stress, peer-recovery, peer-bridge, or early-successor');
const batchSize=Number(option('batch-size',8));if(!Number.isInteger(batchSize)||batchSize<1||batchSize>64)throw Error('--batch-size must be 1–64');
const peers=Number(option('peers',8)),size=Number(option('bytes',262144)),timeout=Number(option('timeout',120000));
if(!Number.isInteger(peers)||peers<3||peers>64||!Number.isInteger(size)||size<1024||size>16777216||!Number.isInteger(timeout)||timeout<1000)throw Error('Use 3–64 peers, 1024–16777216 bytes, and timeout >=1000 ms.');
if(scenario==='peer-bridge'&&peers<5)throw Error('Peer bridge requires at least five initial peers');
if(scenario==='stress'&&size<32768)throw Error('Stress requires at least 32768 bytes so faults occur before transfer completion.');
const runId=new Date().toISOString().replaceAll(':','-')+'-'+crypto.randomUUID().slice(0,8);
const output=resolve(option('output',join(root,'tools/swarm/reports',runId)));mkdirSync(output,{recursive:true});
const started=Date.now(),clients=[],steps=[];let browser,server,stage='setup',fixture,transferFault,publishAuthorizer;
const handoffEvents=[];
const departed=[];let recoverySignals=0;const bridgeCounts={sent:0,forwarded:0,received:0,failed:0,rejected:0};
const metrics={jobs:0,connectionEndpointOpens:0,connectionEndpointCloses:0,jobFailures:0,requestedChunks:0,byStage:{}};
const summary={build:'swarm-bridge-identities-v19',runId,status:'running',startedAt:new Date(started).toISOString(),settings:{scenario,peers,size,timeout,batchSize,transport,network:transport.mode==='local'?'single-host loopback; real WebRTC, no NAT/loss emulation':'single-host real WebRTC; live '+transport.mode+' discovery/signaling; no NAT/loss emulation'},steps,output,metrics,bridgeCounts};
try{summary.sdkRevision=execFileSync('git',['rev-parse','HEAD'],{cwd:root,encoding:'utf8',stdio:['ignore','pipe','pipe']}).trim();summary.dirty=!!execFileSync('git',['status','--porcelain'],{cwd:root,encoding:'utf8'}).trim();}catch{summary.sdkRevision='unavailable';}
// Identify the actual source even when the checkout came from a ZIP.
function sourceHashes(directory){const result={};for(const entry of readdirSync(directory,{withFileTypes:true})){const path=join(directory,entry.name);if(entry.isDirectory()&&entry.name!=='node_modules')Object.assign(result,sourceHashes(path));else if(entry.isFile()&&/\.(js|mjs|json)$/.test(entry.name))result[path.slice(root.length)]=createHash('sha256').update(readFileSync(path)).digest('hex');}return result;}
summary.sdkSourceHashes=sourceHashes(join(root,'packages'));
summary.harnessSourceHashes=Object.fromEntries(['run.mjs','peer.js','server.mjs','launch.mjs','stress.mjs','publish-authorization.mjs','observed-rtc.mjs','native-loss-gate.mjs','live-transport.mjs','package-lock.json'].map(name=>[name,createHash('sha256').update(readFileSync(join(root,'tools/swarm',name))).digest('hex')]));
summary.dependencies=JSON.parse(readFileSync(join(root,'tools/swarm/package.json'),'utf8')).dependencies;
function log(peer,type,data){if(type==='sdk.event'&&['SYS_SUCCESSOR_OFFER','ICE_TRACE','SYS_SIGNAL_TIMEOUT'].includes(data.type))handoffEvents.push({peer,at:Date.now(),type:data.type,payload:data.payload});if(type==='sdk.event'&&data.type==='SYS_SIGNAL_BRIDGE'&&data.payload?.action in bridgeCounts)bridgeCounts[data.payload.action]++;if(type==='recovery.peer_pipe')recoverySignals++;const key=({distributionJob:'jobs',distributionConnectionOpened:'connectionEndpointOpens',distributionConnectionClosed:'connectionEndpointCloses',distributionJobFailed:'jobFailures'})[type];if(key){metrics[key]++;const counters=metrics.byStage[stage]??={jobs:0,connectionEndpointOpens:0,connectionEndpointCloses:0,jobFailures:0,requestedChunks:0};counters[key]++;if(type==='distributionJob'){metrics.requestedChunks+=data.indexes.length;counters.requestedChunks+=data.indexes.length;}}const row={at:new Date().toISOString(),elapsedMs:Date.now()-started,stage,peer,type,data};appendFileSync(join(output,'events.jsonl'),JSON.stringify(row)+'\n');if(type==='distributionProgress'&&transferFault?.client.label===peer&&data.received<data.total)triggerTransferFault(data);}
function save(){writeFileSync(join(output,'summary.json'),JSON.stringify(summary,null,2));}
async function bounded(promise,label){let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(`Deadline exceeded: ${label}`)),timeout);})]);}finally{clearTimeout(timer);}}
async function capture(label){const results=await Promise.all(clients.filter(c=>!c.closed).map(async c=>{try{const state=await bounded(c.page.evaluate(()=>swarm.inspect()),'snapshot '+c.label);return {peer:c.label,...state};}catch(e){return {peer:c.label,error:e.message};}}));writeFileSync(join(output,label+'.json'),JSON.stringify(results,null,2));return results;}
async function step(label,action){stage=label;console.log('RUN ',label);log('runner','stage.start',{});const at=Date.now();try{const evidence=await bounded(action(),label);steps.push({name:label,status:'passed',durationMs:Date.now()-at,evidence});console.log('PASS',label);}catch(e){steps.push({name:label,status:'failed',durationMs:Date.now()-at,error:e.message});throw e;}finally{save();}}
async function poll(check,label){const end=Date.now()+timeout;let last,nextProgress=Date.now()+5000;while(Date.now()<end){last=await check();if(last.ok)return last.evidence;if(Date.now()>=nextProgress){log('runner','waiting',{waitingFor:label,last});console.log('WAIT',label,JSON.stringify(Array.isArray(last.evidence)?last.evidence.map(r=>({peer:r.peer,state:r.state?.state,leader:r.state?.leader,mirror:r.state?.mirror,verified:!!r.file,received:r.progress?.received})):last));nextProgress=Date.now()+5000;}await new Promise(r=>setTimeout(r,250));}log('runner','deadline',{waitingFor:label,last});throw Error(`Timed out waiting for ${label}; last states recorded in events.jsonl`);}
async function addPeer(label,invite){const context=await browser.newContext();const page=await context.newPage();const c={context,page,label,closed:false};clients.push(c);await page.exposeBinding('harnessAuthorizePublish',(_,request)=>{if(!publishAuthorizer)throw Error('Harness publisher authority unavailable');return publishAuthorizer(request);});await page.exposeBinding('harnessLog',(_,e)=>log(label,e.type,e.data));page.on('console',m=>log(label,'console.'+m.type(),{text:m.text()}));page.on('pageerror',e=>log(label,'pageerror',{message:e.message,stack:e.stack}));page.on('crash',()=>log(label,'crash',{}));page.on('requestfailed',r=>log(label,'requestfailed',{url:r.url(),failure:r.failure()}));await page.goto(server.url);await page.waitForFunction(()=>!!globalThis.swarm);const info=await page.evaluate(config=>swarm.open(config),{label,invite,timeoutMs:timeout,transport,batchSize});Object.assign(c,info);log(label,'peer.open',{peerId:c.peerId,doorways:c.doorways});return c;}
async function admitted(excluded=departed){return poll(async()=>{const rows=await Promise.all(clients.filter(c=>!c.closed).map(async c=>({peer:c.label,state:await c.page.evaluate(()=>swarm.inspect().then(s=>s.state))})));return {ok:rows.length>0&&rows.every(r=>r.state.state==='admitted'&&r.state.leader&&!excluded.includes(r.state.leader)&&!excluded.includes(r.state.mirror))&&new Set(rows.map(r=>r.state.leader)).size===1&&new Set(rows.map(r=>r.state.mirror)).size===1,evidence:rows};},'all live peers admitted');}
async function completed(){return poll(async()=>{const rows=await Promise.all(clients.filter(c=>!c.closed).map(async c=>({peer:c.label,...await c.page.evaluate(id=>swarm.inspect().then(s=>({file:s.completed[id],state:s.state,progress:s.progress[id]})),fixture.id)})));for(const row of rows)if(row.file&&(row.file.sha256!==fixture.sha256||row.file.size!==fixture.size))throw Error('File integrity mismatch: '+row.peer);return {ok:rows.every(r=>!!r.file),evidence:rows};},'all live peers verified file');}
async function kill(c){await capture('before-'+c.label+'-exit');c.closed=true;departed.push(c.peerId);log(c.label,'fault.context_closed',{peerId:c.peerId});await c.context.close();}

function armTransferFault(client){
 if(transferFault)throw Error('Transfer fault already armed');
 let resolve,reject;const promise=new Promise((ok,no)=>{resolve=ok;reject=no;});
 transferFault={client,resolve,reject};log('runner','fault.armed',{peer:client.label,peerId:client.peerId,trigger:'first received chunk'});
 return promise;
}
function triggerTransferFault(progress){
 const fault=transferFault;if(!fault)return;transferFault=null;
 const c=fault.client;c.closed=true;departed.push(c.peerId);
 log(c.label,'fault.context_closed',{peerId:c.peerId,trigger:'distributionProgress',progress});
 c.context.close().then(()=>fault.resolve({peer:c.label,peerId:c.peerId,trigger:'distributionProgress',progress}),fault.reject);
}
async function publish(client){fixture=await client.page.evaluate(bytes=>swarm.publish(bytes),{size,variant:(summary.fixtures?.length??0)+2});summary.fixtures??=[];summary.fixtures.push(fixture);return fixture;}
async function currentRole(role){const active=clients.filter(c=>!c.closed);const state=await active[0].page.evaluate(()=>swarm.inspect().then(s=>s.state));const c=active.find(c=>c.peerId===state[role]);if(!c)throw Error('Missing '+role);return c;}
async function pauseSignaling(value){if(transport.mode==='local')server.setSignalingPaused(value);else await Promise.all(clients.filter(c=>!c.closed).map(c=>c.page.evaluate(value=>swarm.setSignalingPaused(value),value)));}
async function stressScenarios(invite){
 await step('ordinary receiver disappears during transfer',async()=>{
  await admitted();await capture('before-ordinary-transfer');
  const leader=await currentRole('leader'),mirror=await currentRole('mirror');
  const victim=clients.find(c=>!c.closed&&c!==leader&&c!==mirror);if(!victim)throw Error('Stress requires an ordinary peer');
  const fault=armTransferFault(victim);await publish(leader);const injection=await fault;
  await admitted();return {injection,verified:await completed()};
 });
 await step('mirror disappears during transfer and peers join during recovery',async()=>{
  await admitted();await capture('before-mirror-transfer');
  const leader=await currentRole('leader'),mirror=await currentRole('mirror');
  const fault=armTransferFault(mirror);await publish(leader);const injection=await fault;
  let joined=false,joinError;const joining=Promise.all([addPeer('during-recovery-a',invite),addPeer('during-recovery-b',invite)]);
  joining.then(()=>{joined=true;},error=>{joinError=error;});
  await poll(async()=>{if(joinError)throw joinError;const evidence=await Promise.all(clients.filter(c=>!c.closed&&c.label.startsWith('during-recovery-')).map(async c=>({peer:c.label,state:await c.page.evaluate(()=>globalThis.swarm?.inspect().then(s=>s.state))})));return {ok:joined,evidence};},'concurrent fresh joins after mirror loss');
  const joins=await joining;
  await admitted();return {injection,joins:joins.map(c=>({peer:c.label,peerId:c.peerId})),verified:await completed()};
 });
 await step('signaling blackout preserves existing RTC file transfer',async()=>{
  await admitted();await capture('before-signaling-blackout');const leader=await currentRole('leader');
  await pauseSignaling(true);
  try{await publish(leader);return {verified:await completed(),fault:'all SDK relay signaling dropped; sockets and RTC left open',transport:transport.mode};}
  finally{await pauseSignaling(false);}
 });
 await step('fresh peer joins after signaling restored',async()=>{await addPeer('after-signaling-restore',invite);await admitted();return completed();});
}

async function peerRecoveryScenario(){
 const state=await clients.find(c=>!c.closed).page.evaluate(()=>swarm.inspect().then(s=>s.state));
 const oldLeader=clients.find(c=>!c.closed&&c.peerId===state.leader),survivors=clients.filter(c=>!c.closed&&c!==oldLeader);
 const sparse=scenario==='peer-bridge';let pairs=[];
 if(sparse){const candidate=survivors.find(c=>c.peerId===state.mirror),requester=survivors.find(c=>c.label==='late-peer'),bridge=survivors.find(c=>c!==candidate&&c!==requester&&c.label!=='peer-3');if(!candidate||!requester||!bridge)throw Error('Sparse bridge topology could not select distinct peers');const backbone=survivors.filter(c=>c!==requester);for(let i=0;i<backbone.length;i++)for(let j=i+1;j<backbone.length;j++)pairs.push([backbone[i],backbone[j]]);pairs.push([requester,bridge]);summary.bridgeTopology={bridge:bridge.peerId,requester:requester.peerId,replacementLeader:candidate.peerId,edges:pairs.map(([a,b])=>[a.peerId,b.peerId]),directRequesterLeader:false};}
 else for(let i=0;i<survivors.length;i++)for(let j=i+1;j<survivors.length;j++)pairs.push([survivors[i],survivors[j]]);
 await step(sparse?'establish sparse survivor bridge topology':'establish accepted survivor peer connections',async()=>{
  for(const [a,b] of pairs){await a.page.evaluate(id=>swarm.invitePeer(id),b.peerId);
   await poll(async()=>{await b.page.evaluate(id=>swarm.acceptPeer(id),a.peerId);const rows=await Promise.all([a,b].map(c=>c.page.evaluate(()=>swarm.peerPipes())));return {ok:rows[0].some(p=>p.hKey===b.peerId&&p.dcState==='open'&&p.chatState==='ready')&&rows[1].some(p=>p.hKey===a.peerId&&p.dcState==='open'&&p.chatState==='ready'),evidence:rows};},'accepted survivor peer pipe');
  }
  const evidence=await Promise.all(survivors.map(async c=>({peer:c.label,pipes:await c.page.evaluate(()=>swarm.peerPipes())})));
  if(sparse){const topology=summary.bridgeTopology,requester=evidence.find(e=>survivors.find(c=>c.label===e.peer)?.peerId===topology.requester);if(requester.pipes.some(p=>p.hKey!==topology.bridge&&p.dcState==='open'))throw Error('Requester has an unintended direct peer connection');log('runner','bridge.topology',topology);}
  return evidence;
 });
 await pauseSignaling(true);const before=recoverySignals,beforeBridge=bridgeCounts.forwarded;
 try{
  await step(sparse?'leader loss recovers through ordinary bridge with signaling blacked out':'leader loss recovers through existing peer pipes with signaling blacked out',async()=>{await kill(oldLeader);const evidence=await admitted();if(sparse?bridgeCounts.forwarded===beforeBridge:recoverySignals===before)throw Error('No required peer recovery signaling observed');return {peers:evidence,recoverySignals:recoverySignals-before,bridgeCounts:{...bridgeCounts}};});
  await step('new verified transfer after recovery without relay signaling',async()=>{const state=await survivors[0].page.evaluate(()=>swarm.inspect().then(s=>s.state));await publish(survivors.find(c=>c.peerId===state.leader));return completed();});
 }finally{await pauseSignaling(false);}
}

async function earlySuccessorScenario(){
 const leader=await currentRole('leader'),mirror=await currentRole('mirror');
 const receivers=clients.filter(c=>!c.closed&&c!==leader&&c!==mirror);
 let retained,ice;
 try{
  await step('retain native successor offer and ICE while old leader loss is delayed',async()=>{
   for(const c of receivers)await c.page.evaluate(()=>swarm.holdSystemLoss());
   await kill(leader);
   const evidence=await poll(async()=>{
    retained=handoffEvents.find(e=>e.type==='SYS_SUCCESSOR_OFFER'&&e.payload.action==='held'&&receivers.some(c=>c.label===e.peer));
    ice=retained&&handoffEvents.find(e=>e.type==='ICE_TRACE'&&e.peer===retained.peer&&e.payload.stage==='successor_held'&&e.payload.attemptId===retained.payload.attemptId);
    return {ok:!!(retained&&ice),evidence:{retained,ice}};
   },'native offer and ICE retained before old pipe loss');
   const receiver=receivers.find(c=>c.label===retained.peer),snapshot=await receiver.page.evaluate(()=>swarm.inspect());
   if(snapshot.state.leader!==leader.peerId||!snapshot.snapshot.successorOffers.some(r=>r.attemptId===retained.payload.attemptId&&r.candidates>0))throw Error('Retained signaling changed authority or lost matching ICE');
   return {...evidence,oldLeader:leader.peerId,candidate:mirror.peerId,receiver:receiver.peerId,authorityUnchanged:true};
  });
  await step('release old pipe loss and resume the exact native successor attempt',async()=>{
   for(const c of receivers)await c.page.evaluate(()=>swarm.releaseSystemLoss());
   const result=await poll(async()=>{
    const resumed=handoffEvents.find(e=>e.type==='SYS_SUCCESSOR_OFFER'&&e.payload.action==='resumed'&&e.peer===retained.peer&&e.payload.peer===retained.payload.peer&&e.payload.attemptId===retained.payload.attemptId);
    const applied=handoffEvents.find(e=>e.type==='ICE_TRACE'&&e.peer===ice.peer&&e.payload.key===ice.payload.key&&['applied','applied_from_buffer'].includes(e.payload.stage));
    return {ok:!!(resumed&&applied),evidence:{retained,resumed,ice,applied}};
   },'same attempt resumed and retained ICE applied');
   if(result.resumed.at-retained.at>=15000)throw Error('Retained offer outlived its bounded lifetime');
   if(handoffEvents.some(e=>e.type==='SYS_SIGNAL_TIMEOUT'&&e.payload.attemptId===retained.payload.attemptId))throw Error('Retained attempt timed out');
   return {...result,converged:await admitted()};
  });
  await step('verified new distribution after forced native successor race',async()=>{await publish(await currentRole('leader'));return completed();});
 }finally{for(const c of receivers)if(!c.closed)await c.page.evaluate(()=>swarm.releaseSystemLoss()).catch(()=>{});}
}

process.on('SIGINT',()=>{log('runner','interrupt',{});void browser?.close();});
try{
 const bundle=await build({entryPoints:[join(root,'tools/swarm/peer.js')],bundle:true,write:false,format:'esm',platform:'browser'});server=await startServer(bundle.outputFiles[0].text,(type,data)=>log('relay',type,data));
 browser=await chromium.launch({headless:!args.includes('--headed'),...(process.env.PHANTOM_CHROMIUM_PATH?{executablePath:process.env.PHANTOM_CHROMIUM_PATH}:{})});summary.browserVersion=browser.version();save();
 let seed;
 await step('admit swarm',async()=>{seed=await addPeer('seed');summary.resolvedDoorways=seed.doorways;publishAuthorizer=createPublishAuthorizer({identity:await importAdminIdentity(await seed.page.evaluate(()=>swarm.exportAdminSeed())),networkId:seed.networkId});for(let i=1;i<peers;i++)await addPeer('peer-'+i,seed.invite);return admitted();});
 await step('distribute and verify',async()=>{fixture=await seed.page.evaluate(bytes=>swarm.publish(bytes),size);summary.fixture=fixture;return completed();});
 await capture('initial-distribution');
 if(scenario==='early-successor'){await earlySuccessorScenario();}else{
 await step('remove publisher and recover',async()=>{await kill(seed);return admitted([seed.peerId]);});
 await step('late peer downloads without publisher',async()=>{await addPeer('late-peer',seed.invite);await admitted();return completed();});
 const rows=await capture('after-late-peer');
 await step('prove survivor supplied late peer',async()=>{const row=rows.find(r=>r.peer==='late-peer');const sources=Object.keys(row.progress[fixture.id]?.bySource??{});if(!sources.length||sources.includes(seed.peerId))throw Error('Missing survivor-only transfer evidence');return {sources,progress:row.progress[fixture.id],file:row.completed[fixture.id]};});
 if(['peer-recovery','peer-bridge'].includes(scenario))await peerRecoveryScenario();else{
 await step('remove current leader and recover',async()=>{const active=clients.filter(c=>!c.closed);const state=await active[0].page.evaluate(()=>swarm.inspect().then(s=>s.state));const leader=active.find(c=>c.peerId===state.leader);if(!leader)throw Error('Current leader not found');summary.removedLeader=leader.peerId;await kill(leader);const evidence=await admitted([seed.peerId,leader.peerId]);if(evidence.some(r=>r.state.leader===leader.peerId)||new Set(evidence.map(r=>r.state.leader)).size!==1)throw Error('Live peers disagree on replacement leader');return evidence;});
 await step('new transfer after leader replacement',async()=>{await addPeer('post-election-peer',seed.invite);await admitted();return completed();});
 }
 if(scenario==='stress')await stressScenarios(seed.invite);
 }
 summary.status='passed';
}catch(e){summary.status='failed';summary.error={stage,message:e.message,stack:e.stack};log('runner','failure',summary.error);console.error('FAIL',stage+':',e.message);process.exitCode=1;
}finally{
 await capture('final');summary.finishedAt=new Date().toISOString();summary.durationMs=Date.now()-started;save();
 const escape=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 writeFileSync(join(output,'report.html'),`<!doctype html><meta charset="utf-8"><title>Phantom swarm report</title><style>body{font:16px system-ui;max-width:1000px;margin:40px auto;padding:24px;background:#111;color:#eee}pre{white-space:pre-wrap}a{color:#8cc8ff}li{margin:12px 0}</style><h1>Phantom swarm: ${escape(summary.status)}</h1><p>${escape(summary.settings.network)}</p><p>Build ${escape(summary.build)} · SDK ${escape(summary.sdkRevision==='unavailable'?'ZIP checkout (source hashes in summary)':summary.sdkRevision)} · Chromium ${escape(summary.browserVersion??'not launched')}</p><ol>${steps.map(s=>`<li><strong>${escape(s.status)} — ${escape(s.name)}</strong> (${s.durationMs} ms)${s.error?'<pre>'+escape(s.error)+'</pre>':''}</li>`).join('')}</ol><p><a href="summary.json">Summary</a> · <a href="events.jsonl">Event timeline</a> · <a href="final.json">Final diagnostics</a></p><pre>${escape(summary.error?.message??'Every asserted scenario passed.')}</pre>`);
 await browser?.close().catch(()=>{});await server?.close().catch(()=>{});console.log('Report:',join(output,'report.html'));
}
