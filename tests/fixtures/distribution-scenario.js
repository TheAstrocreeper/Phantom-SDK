import {createBrowserPhantom} from '../../packages/phantom-doorways/src/browser-entry.js';
import {createAdminIdentity,createAdminController} from '../../packages/phantom-core/src/index.js';

/** The browser and automated tests run the same SDK scenario. No mock distribution algorithm. */
export async function runDistributionTest({endpoint='',relays=[],useNostr=true,sdkFactory=createBrowserPhantom,
 WebSocket=globalThis.WebSocket,runtime=globalThis,signal,onUpdate=()=>{},deadlineMs=90000,transportRecoveryGraceMs,large=false,transferDeadlineMs=600000}={}){
 const report={format:'phantom-distribution-browser-v2',build:'large-distribution-1',startedAt:new Date().toISOString(),passed:false,steps:[],checks:[],metrics:{adminChecks:0,jobs:0,peakConnections:0},scope:'Four SDK clients in one browser. Real browser WebRTC when run in the page; does not verify separate-device NAT or internet-scale performance.'};
 const peers=[];let monitor,progressId;
 const storage=()=>{const m=new Map();return {getItem:k=>m.get(k)??null,setItem:(k,v)=>m.set(k,v)};};
 const update=()=>onUpdate({...report,peers:peers.map(p=>({name:p.name,state:p.network?.getState().state??'joining',role:p.network?.getState().role??'',admin:p.network?.getAdminState().phase??'',received:p.completed.size,pieces:holdings(p,progressId).length,total:p.network?.getDistributionState()?.assets.find(a=>a.id===progressId)?.total??0,connections:p.network?.getDistributionState()?.connections.length??0}))});
 function holdings(p,id){return p.network?.getDistributionState()?.assets.find(a=>a.id===id)?.holders.find(h=>h.peer===p.network.getState().peerId)?.indexes??[];}
 async function until(fn,label,timeout=deadlineMs){const end=Date.now()+timeout;while(!fn()){if(signal?.aborted)throw Error('Test cancelled');if(Date.now()>end)throw Error(label+' timed out');await new Promise(r=>setTimeout(r,25));}}
 async function step(label,fn){const row={label,status:'running'};report.steps.push(row);update();try{if(signal?.aborted)throw Error('Test cancelled');await fn();row.status='pass';}catch(e){row.status='fail';row.error=e.message;throw e;}finally{update();}}
 const active=()=>peers.filter(p=>p.active),leader=()=>active().find(p=>p.network?.getState().role==='syscon1');
 const topology=p=>p.network.snapshot().concierge.membershipAudit.topology;
 async function agreement(){await until(()=>{const list=active(),base=topology(list[0]);return list.every(p=>{const t=topology(p);return p.network.getState().state==='admitted'&&t.committedCount===list.length&&t.pendingId==='none'&&t.committedKeys===base.committedKeys&&t.committedOne===base.committedOne&&t.committedTwo===base.committedTwo&&t.committedEpoch===base.committedEpoch&&t.committedSeq===base.committedSeq;})&&leader()?.network.getAdminState().phase==='approved';},'Roster agreement');}
 try{
 const doorways=[];if(useNostr)doorways.push({type:'nostr',...(relays.length?{relays}:{})});
 if(endpoint)doorways.unshift({type:'websocket',url:endpoint});if(!doorways.length)throw Error('Select Nostr or enter your Cloudflare WebSocket URL');
 const identity=await createAdminIdentity(),admin=createAdminController({identity});
 let invite,manifest;
 await step('Four peers enter and agree on the roster',async()=>{
  for(let i=0;i<4;i++){
   const p={name:'Peer '+(i+1),completed:new Map(),active:true};peers.push(p);
   const sdk=sdkFactory({transportRecoveryGraceMs,adminPublicKey:identity.publicKey,doorways,doorwayMode:'parallel',storage:storage(),WebSocket,runtime,
    distribution:{maxConnections:3,batchSize:large?64:4,intervalMs:100,jobTimeoutMs:large?60000:10000},
    readAdminOrders:async({networkId})=>{report.metrics.adminChecks++;return admin.getOrders(networkId);}});
   const options={name:p.name,signal,timeoutMs:deadlineMs,...(i&&useNostr&&endpoint?{doorwayTypes:i===1?['nostr','websocket']:[i===2?'websocket':'nostr']}:{})};
   p.sdk=sdk;p.options=options;p.network=i?await sdk.join(invite,options):await sdk.create(options);invite??=p.network.invite;
   if(i===0)await until(()=>p.network.getAdminState().phase==='approved','Initial authority approval');
   p.network.on('distributionComplete',e=>p.completed.set(e.id,e.blob));
   p.network.on('distributionJob',()=>report.metrics.jobs++);
   p.network.on('distributionConnectionOpened',()=>{report.metrics.peakConnections=Math.max(report.metrics.peakConnections,p.network.getDistributionState().connections.length);});
  }
  monitor=setInterval(()=>{for(const p of active())report.metrics.peakConnections=Math.max(report.metrics.peakConnections,p.network.getDistributionState().connections.length);update();},100);
  await agreement();
 });
 const source=leader(),payload=Uint8Array.from({length:large?1024*1024:256*1024},(_,i)=>i%251),file=new Blob(large?Array(100).fill(payload):[payload]);
 const digest=async blob=>Array.from(new Uint8Array(await runtime.crypto.subtle.digest('SHA-256',await blob.arrayBuffer())),b=>b.toString(16).padStart(2,'0')).join('');
 const expectedHash=await digest(file);let sourceLeft=false;
 await step(large?'100 MiB: source leaves before completion; survivors finish':'Admin authorizes a file; peers automatically exchange pieces',async()=>{
  let before=active().map(p=>JSON.stringify(topology(p)));
  const options={name:'test-update.bin',chunkSize:large?65536:4096,distributionId:'browser-release'};
  const prepared=await source.network.publishFile(file,options),order=await admin.publish(source.network.networkId,prepared.id);
  await leader().network.submitAdminOrder(order);manifest=await source.network.distributeFile(file,{...options,authorization:order});
  progressId=manifest.id;
  if(large){
   await until(()=>{const receivers=active().filter(p=>p!==source);return new Set(receivers.flatMap(p=>holdings(p,manifest.id))).size===manifest.total&&receivers.every(p=>holdings(p,manifest.id).length<manifest.total);},'Survivors collectively hold every piece before completion',transferDeadlineMs);
   report.checks.push({sourceDeparture:{totalPieces:manifest.total,collectivePieces:new Set(active().filter(p=>p!==source).flatMap(p=>holdings(p,manifest.id))).size,receiverPieces:active().filter(p=>p!==source).map(p=>holdings(p,manifest.id).length),elapsedMs:Date.now()-Date.parse(report.startedAt)}});
   const checks=report.metrics.adminChecks;source.active=false;source.network.close();sourceLeft=true;await agreement();before=active().map(p=>JSON.stringify(topology(p)));if(report.metrics.adminChecks<=checks)throw Error('Successor skipped admin check');
  }
  await until(()=>active().filter(p=>p!==source).every(p=>p.completed.has(manifest.id)),'Complete file delivery',large?transferDeadlineMs:deadlineMs);
  for(const p of active().filter(p=>p!==source)){const received=p.completed.get(manifest.id);if(received.size!==file.size||await digest(received)!==expectedHash)throw Error(p.name+' received different bytes');}
  if(active().some((p,i)=>JSON.stringify(topology(p))!==before[i]))throw Error('Distribution changed the roster');
  if(active().some(p=>p.network.snapshot().chatPairs?.length))throw Error('Distribution opened private chat');
  await until(()=>active().every(p=>!p.network.getDistributionState().connections.length&&!p.network.getDistributionState().jobs.length),'Temporary connections close');
  report.checks.push({verifiedReceivers:3,bytesEach:file.size,sha256:expectedHash,rosterUnchangedDuringDistribution:true,sourceLeftBeforeCompletion:sourceLeft,privateChatConnections:0});
 });
 await step('Original leader leaves; successor checks admin and takes over',async()=>{
  if(sourceLeft){await agreement();return;}const before=report.metrics.adminChecks;source.active=false;source.network.close();await agreement();
  if(report.metrics.adminChecks<=before)throw Error('Successor skipped its admin check');
 });
 if(useNostr&&endpoint)await step('Original peer returns through Cloudflare with the same identity',async()=>{
  const old=source.network.getState().peerId;source.network=await source.sdk.join(invite,{...source.options,doorwayTypes:['websocket']});source.active=true;
  source.network.on('distributionComplete',e=>source.completed.set(e.id,e.blob));await agreement();
  if(source.network.getState().peerId!==old)throw Error('Returning identity changed');
 });
 await step('Admin revokes an active transfer; sharing stops',async()=>{
  const seed=leader(),revokedFile=new Blob([new Uint8Array((large?100:4)*1024*1024)]),options={name:'revoked-update.bin',chunkSize:large?65536:4096,distributionId:'browser-revocation'};
  const prepared=await seed.network.publishFile(revokedFile,options),order=await admin.publish(seed.network.networkId,prepared.id);await seed.network.submitAdminOrder(order);
  const asset=await seed.network.distributeFile(revokedFile,{...options,authorization:order});
  progressId=asset.id;
  await until(()=>active().some(p=>p!==seed&&holdings(p,asset.id).length>= (large?16:1)&&holdings(p,asset.id).length<asset.total)&&active().some(p=>p.network.getDistributionState().connections.length),'Verified pieces moving',large?transferDeadlineMs:deadlineMs);
  report.checks.push({revocationPiecesReceived:active().filter(p=>p!==seed).map(p=>holdings(p,asset.id).length)});
  const revoke=await admin.revoke(seed.network.networkId,asset.id);await leader().network.submitAdminOrder(revoke);
  await until(()=>active().every(p=>p.network.isRevoked(asset.id)&&!p.network.getDistributionState().connections.length&&!p.network.getDistributionState().jobs.length),'Revocation convergence');
  let rejected=false;try{await seed.network.distributeFile(revokedFile,{...options,authorization:order});}catch(e){if(!/revok/i.test(e.message))throw e;rejected=true;}if(!rejected)throw Error('Revoked file was accepted');
  report.checks.push({revoked:true,transferConnectionsClosed:true});
 });
 await step('Admin kills the network; all remaining peers retire',async()=>{
  const l=leader(),kill=await admin.retire(l.network.networkId);await l.network.submitAdminOrder(kill);
  await until(()=>active().every(p=>p.network.getState().state==='retired'),'Kill convergence');
 });
 if(report.metrics.peakConnections>3)throw Error('Connection limit exceeded');
 report.passed=true;
 }catch(e){report.error=e.message;report.failureStates=active().map(p=>({state:p.network?.getState(),admin:p.network?.getAdminState(),topology:p.network?topology(p):null,recovery:p.network?.snapshot().gossipRecovery}));}
 finally{clearInterval(monitor);for(const p of peers)p.network?.close();report.finishedAt=new Date().toISOString();update();}
 return report;
}
