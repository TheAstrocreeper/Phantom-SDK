import {createStripedRTC} from './striped-distribution-rtc.js';
import {defaultRTCConfiguration} from './rtc-configuration.js';
import {scoutConnection} from './connection-scout.js';
import {sendFileChannelPayload,coalesceQueuedFileChunk} from './file-channel-payload.js';
import {createCapabilityExchange} from './capability-exchange.js';
import {createCapabilityReport} from './connection-planner.js';
import {createPlanAgreement} from './plan-agreement.js';
import {createTransferScout,TRANSFER_SCOUT_PROTOCOL,FILE_WORKLOAD_PROTOCOL} from './transfer-scout.js';
import {createFileDistribution,createMemoryChunkStore} from './files.js';
import {createAdminAuthorization} from './authorization.js';

/** Separate distribution state. Never writes membership, election or private chat state. */
export function createNetworkDistribution({core, networkId, adminPublicKey, lifecycle, runtime=globalThis,
  emit=()=>{}, allowed=()=>true, canUsePeer=()=>true, readAdminOrders=async()=>[], checkAdmin, options={}}) {
  const reuseConnections=options.reuseConnections!==false;
  const maxConnections=options.maxConnections??3, batchSize=options.batchSize??8;
  const transferLanes=options.transferLanes??1;
  // Experimental: the leader enables parallel jobs only after both endpoints
  // advertise support. Keep disabled until real-browser validation is complete.
  const assignedRanges=options.assignedRanges===true;
  if(![1,3].includes(transferLanes)||maxConnections<transferLanes)throw new TypeError('Invalid distribution transfer lanes');
  const maxJobs=assignedRanges?maxConnections:Math.floor(maxConnections/transferLanes);
  const DistributionRTC=transferLanes===1?runtime.RTCPeerConnection:createStripedRTC(runtime.RTCPeerConnection,{lanes:transferLanes});
  const jobTimeoutMs=options.jobTimeoutMs??15000, intervalMs=options.intervalMs??250;
  if(!Number.isInteger(maxConnections)||maxConnections<1||maxConnections>16||!Number.isInteger(batchSize)||batchSize<1||batchSize>64||!Number.isFinite(jobTimeoutMs)||jobTimeoutMs<100||!Number.isFinite(intervalMs)||intervalMs<10)throw new TypeError('Invalid distribution limits');
  const chunkStore=options.chunkStore??createMemoryChunkStore();
  if((chunkStore.load!==undefined||chunkStore.save!==undefined)&&(typeof chunkStore.load!=='function'||typeof chunkStore.save!=='function'))throw new TypeError('Persistent chunkStore requires load and save');
  let restoration=null,restorationState='pending';const restoreResults=[];
  const crypto=runtime.crypto??globalThis.crypto;
  const policy=adminPublicKey?createAdminAuthorization({networkId,adminPublicKey,crypto}):null;
  const preparedFiles=new WeakMap(),continuationSelections=new WeakMap(),progressIndexes=new Map();
  const assets=new Map(),jobs=new Map(),links=new Map(),listeners=new Set(),finished=new Set(),blockedUntil=new Map(),setupFailures=new Map(),pendingSignals=new Map(),reported=new Map(),mirrored=new Map(),mirrorJobs=new Map(),closedJobs=new Map();
  const rangePeers=new Set();let rangeAdvertisement='',scoutCoordinator=false;
  const scoutEnabled=options.transferScout===true&&options.capabilityExchange===true&&assignedRanges&&maxConnections>=3;
  let closed=false,sequence=0,leaderToken='',checkedToken='',checking=false,dirty=false,lastReport=0,receivedSequence=-1;
  const state=()=>core.getState(),self=()=>state().peerId,leader=()=>state().leader;
  const isLeader=()=>state().role==='syscon1'&&leader()===self();
  const fire=(type,payload)=>{for(const fn of listeners)fn({type,payload});};
  function traceChunk(payload){if(options.traceChunks===true)emit('distributionChunkTrace',payload);}
  function scout(link,phase){
    if(options.scout!==true)return;
    const pcs=Array.isArray(link.pc?.pcs)?link.pc.pcs:[link.pc];
    for(const [lane,pc] of pcs.entries()){
      const dc=Array.isArray(link.pc?.channels)?link.pc.channels[lane]:link.dc;
      const context={phase,lane,jobId:link.job.id,role:link.job.source===self()?'source':'receiver',at:Date.now()};
      if(phase==='closing')context.lifecycle={batch:link.job.batch??0,asset:link.job.asset,deadlineAt:link.job.deadline,idleBudgetMs:jobTimeoutMs,lastActivityAt:link.lastActivityAt??null,lastActivityReason:link.lastActivityReason??null,idleMs:link.lastActivityAt===undefined?null:context.at-link.lastActivityAt,bufferedBytes:dc?.bufferedAmount??null,submittedBytes:link.submittedBytes??null,drainedBytes:link.drainedBytes??null};
      void scoutConnection(pc,{channel:dc}).then(report=>emit('distributionScout',{...context,report})).catch(()=>emit('distributionScout',{...context,error:'scout_failed'}));
    }
  }
  function error(error,context={}){if(!closed)emit('distributionError',{...context,error:error.message});}
  const batchOf=job=>job.batch??0;
  const sameBatch=(job,batch)=>Number.isSafeInteger(batch??0)&&(batch??0)===batchOf(job);
  const ownsBatch=(link,job)=>!closed&&links.get(job.id)===link&&link.job===job;
  function frame(to,body){if(closed||!self()||!leader()||!canUsePeer(to))return;const packet={networkId,from:self(),to,body};if(to===self()){void handle(self(),packet).catch(error);return;}core.command('PHANTOM_DIST_SEND',{to:isLeader()?to:leader(),frame:packet});}
  // The facade supplies the committed roster projection. Reading it never changes it.
  let roster=[];
  const controlObservations=new Map(),controlGenerations=new Map(),controlPending=new Map();let controlSequence=0;
  const localCapability=peer=>createCapabilityReport(controlObservations.get(peer)??{version:1,messageLimit:{status:'unknown',bytes:null}},{protocols:['phantom-file-v1','phantom-file-fragments-v1',...(scoutEnabled?[TRANSFER_SCOUT_PROTOCOL,...(assignedRanges?[FILE_WORKLOAD_PROTOCOL]:[])]:[])],maxChunkBytes:options.maxChunkBytes??65536});
  const peerAllowed=peer=>roster.includes(peer)&&canUsePeer(peer);
  function observeControl(peer){
    if(controlPending.has(peer))return controlPending.get(peer);
    const generation=++controlSequence,token=leaderToken;controlGenerations.set(peer,generation);
    const pending=(async()=>{
      let observation;try{observation=await core.scoutPeer?.(peer);}catch{}
      if(closed||token!==leaderToken||!peerAllowed(peer)||controlGenerations.get(peer)!==generation)return false;
      if(observation)controlObservations.set(peer,{version:1,messageLimit:observation.messageLimit,observationScope:'control',controlRoute:observation.route});
      else controlObservations.delete(peer);
      return true;
    })();
    controlPending.set(peer,pending);
    const release=()=>{if(controlPending.get(peer)===pending)controlPending.delete(peer);};
    void pending.then(release,release);
    return pending;
  }
  const capabilityExchange=options.capabilityExchange===true?createCapabilityExchange({send:frame,allowed:peerAllowed,localReport:localCapability,onReport:(peer,report)=>{emit('distributionCapability',{peer,report});if(report.protocols.includes(TRANSFER_SCOUT_PROTOCOL)&&!(options.scoutBeforeSend===true&&report.protocols.includes(FILE_WORKLOAD_PROTOCOL)))preflight?.start(peer);}}):null;
  const agreement=capabilityExchange?createPlanAgreement({send:frame,allowed:peerAllowed,localReport:localCapability,remoteReport:peer=>capabilityExchange.get(peer),newId:()=>crypto.randomUUID(),onDecision:result=>emit('distributionPlanDecision',result)}):null;
  const preflight=scoutEnabled?createTransferScout({self,allowed:peerAllowed,send:frame,runtime,configuration:options.rtcConfiguration,fallback:Math.min(3,maxConnections),comparison:options.scoutComparison,prepareTransport:options.prepareScoutTransport??null,laneRateTradeoff:options.scoutLaneRateTradeoff,sampleBytes:options.scoutSampleBytes,fileWorkloadLimits:{queueComparison:options.scoutQueueComparison===true,queueBudgetScope:options.scoutQueueBudgetScope??'association',batchSize,maxConnections:Math.min(3,maxConnections),requestWindow:options.requestWindow??8,requestTimeoutMs:options.requestTimeoutMs??10000,sendTimeoutMs:jobTimeoutMs,maxAttemptsPerPeer:options.maxAttemptsPerPeer??2,maxChunkBytes:options.maxChunkBytes??65536},busy:()=>links.size>0,onReport:report=>emit('distributionScout',report)}):null;
  function clearPeerSetup(peer){
    preflight?.drop(peer);
    for(const asset of assets.values()){
      for(const key of asset.sourceLimits?.keys()??[])if(key.startsWith(peer+':')||key.endsWith(':'+peer))asset.sourceLimits.delete(key);
      if(asset.outgoingSetup?.receiver===peer){delete asset.outgoingSetup;delete asset.outgoingQueuePolicy;delete asset.outgoingMessagePolicy;asset.setupAcknowledged=false;reported.delete(asset.id);}
    }
  }
  function selectedSetup(peer){
    const result=preflight?.get(peer);if(!result?.transportAvailable)return null;
    const plan=result.connectionPlan;
    return {...result,senderSelection:{status:result.status,reason:result.reason,connections:result.connections,lanes:result.lanes},
      ...(plan?{status:plan.laneDecision.status==='fallback'?'inconclusive':plan.laneDecision.status,reason:plan.laneDecision.reason,connections:plan.connections,lanes:[...plan.lanes],queuePolicy:plan.queuePolicy,messagePolicy:plan.messagePolicy}:{}),
      enforcement:scoutCoordinator===2?'coordinator_supported':'default_coordinator_unconfirmed',
      effectiveConnections:scoutCoordinator===2?(plan?.connections??result.connections):Math.min(3,maxConnections)};
  }
  function setRoster(rows){const next=rows.map(r=>r.hKey).filter(k=>/^[0-9a-f]{16}$/.test(k)&&canUsePeer(k));for(const peer of roster)if(!next.includes(peer)){controlObservations.delete(peer);controlGenerations.delete(peer);controlPending.delete(peer);clearPeerSetup(peer);capabilityExchange?.drop(peer);agreement?.drop(peer);}roster=next;for(const peer of rangePeers)if(!roster.includes(peer))rangePeers.delete(peer);}
  function report(){if(assignedRanges&&self()&&leader()&&canUsePeer(leader())&&rangeAdvertisement!==leaderToken){rangeAdvertisement=leaderToken;frame(leader(),{type:'rangeCapability',version:1});}for(const asset of assets.values())if(allowed(asset.id,asset.distributionId)&&asset.holders.has(self())){const sent=reported.get(asset.id)??new Set(),have=asset.holders.get(self());if(reported.has(asset.id)&&sent.size===have.size)continue;const indexes=[...have].filter(i=>!sent.has(i)).sort((a,b)=>a-b);for(let offset=0;offset<Math.max(1,indexes.length);offset+=128)frame(leader(),{type:'inventory',id:asset.id,total:asset.total,distributionId:asset.distributionId,proof:asset.proof,indexes:indexes.slice(offset,offset+128),...(asset.outgoingSetup&&scoutCoordinator?{setup:asset.outgoingSetup}:{})});reported.set(asset.id,sent);}lastReport=Date.now();}
  function table(){return {sequence,leader:self(),assets:[...assets.values()].map(a=>({id:a.id,total:a.total,distributionId:a.distributionId,proof:a.proof,holders:[...a.holders].map(([peer,indexes])=>({peer,indexes:[...indexes].sort((a,b)=>a-b)}))}))};}
  function mirror(){if(!isLeader()||!dirty)return;dirty=false;sequence++;const mirror=state().mirror;if(mirror&&mirror!==self()){
    // Page by holder; large piece inventories stay bounded to the control frame limit.
    const rows=table();for(const a of rows.assets)for(const h of a.holders){const key=a.id+':'+h.peer,sent=mirrored.get(key)??new Set();if(mirrored.has(key)&&sent.size===h.indexes.length)continue;const indexes=h.indexes.filter(i=>!sent.has(i));for(let offset=0;offset<Math.max(1,indexes.length);offset+=128)frame(mirror,{type:'mirror',sequence,asset:{...a,holders:undefined},peer:h.peer,indexes:indexes.slice(offset,offset+128)});for(const i of indexes)sent.add(i);mirrored.set(key,sent);}
    frame(mirror,{type:'mirrorJobs',sequence,reset:true});for(const job of jobs.values())frame(mirror,{type:'mirrorJobs',sequence,job});
  }}
  function fileLink(type,payload){
    // Opposite-direction jobs for the same asset can share a peer pair.
    // Requests belong to our receiving job; replies belong to our source job.
    const receiving=type==='SEND_DM'&&payload.sys==='PHANTOM_FILE_REQUEST';
    return [...links.values()].find(l=>canUsePeer(l.peer)&&l.peer===payload.to&&l.job.asset===payload.file?.id&&
      (receiving?l.job.receiver:l.job.source)===self()&&l.dc?.readyState==='open'&&
      (payload.file?.index===undefined||l.job.indexes.includes(payload.file.index)));
  }
  function timeline(link,stage,details={},context){
    if(options.fileTimeline!==true)return;
    const identity=context??{jobId:link.job.id,batch:batchOf(link.job),asset:link.job.asset,peer:link.peer,role:link.job.source===self()?'source':'receiver'};
    try{emit('distributionTimeline',{...identity,stage,atMs:performance.now(),originMs:performance.timeOrigin,...details});}catch{} // Observers cannot interrupt a file job.
  }
  const transport={createFileTrace(to,file,scope,receiving=false){
    if(options.fileTimeline!==true)return undefined;
    const link=fileLink(receiving?'SEND_DM':'SEND_FILE_BIN_CHUNK',{to,file,...(receiving?{sys:'PHANTOM_FILE_REQUEST'}:{})});
    if(!link||(scope&&scope!==link.dc))return undefined;
    const context={jobId:link.job.id,batch:batchOf(link.job),asset:link.job.asset,peer:link.peer,role:receiving?'receiver':'source',index:file.index};
    return (stage,details={})=>timeline(link,stage,details,context);
  },getFileSendScope(to,file){return fileLink('SEND_FILE_BIN_CHUNK',{to,file})?.dc??null;},onEvent(fn){listeners.add(fn);return()=>listeners.delete(fn);},command(type,payload){
    const link=fileLink(type,payload);
    if(type==='SEND_FILE_BIN_CHUNK'||type==='SEND_DM'&&['PHANTOM_FILE_REQUEST','PHANTOM_FILE_MISSING'].includes(payload.sys))traceChunk({direction:'send',kind:type==='SEND_FILE_BIN_CHUNK'?'binary':payload.sys,jobId:link?.job.id??null,peer:payload.to,index:payload.file?.index,available:!!link});
    if(!link||(type==='SEND_FILE_BIN_CHUNK'&&payload.sendScope&&payload.sendScope!==link.dc)){
      // A source read may finish after its job/channel was retired. Settle the
      // file service's send immediately so its per-peer queue can keep moving.
      if(type==='SEND_FILE_BIN_CHUNK')fire('FILE_BIN_CHUNK_SENT',{...payload,ok:false,error:'Distribution job retired before chunk send'});
      return;
    }
    if(type==='SEND_DM'){const data=JSON.stringify({kind:'message',sys:payload.sys,file:payload.file});if(['FILE_OFFER','PHANTOM_FILE_MANIFEST'].includes(payload.sys))payloadSend(link,data);else link.dc.send(data);return;}
    if(type==='SEND_FILE_BIN_CHUNK')void sendBinary(link,payload).catch(error);
  }};
  const files=createFileDistribution({core:transport,canUsePeer,crypto,allowed,emit:(type,payload)=>{
    if(type==='fileOffer'){const asset=assets.get(payload.manifest.id);if(!asset||asset.total!==payload.manifest.total||asset.distributionId!==payload.manifest.distributionId){error(Error('Authorized manifest metadata mismatch'));return;}if(assignedRanges&&!asset.holders.has(self())){asset.holders.set(self(),new Set());report();}for(const link of links.values())if(link.job.asset===payload.manifest.id&&link.job.receiver===self()&&link.peer===payload.from)startReceiver(link);}
    if(type==='fileTransferTiming'){emit('distributionTransferTiming',payload);return;}
    if(type==='fileProgress'){for(const link of links.values())if(link.job.asset===payload.id&&link.job.receiver===self()&&link.peer===payload.from)progress(link,'verified_chunk');const asset=assets.get(payload.id);if(asset){let indexes=progressIndexes.get(payload.id);if(!indexes){indexes=new Set(asset.holders.get(self())??[]);progressIndexes.set(payload.id,indexes);}indexes.add(payload.index);payload={...payload,received:indexes.size};}}
    emit(type==='fileProgress'?'distributionProgress':type==='fileError'?'distributionError':'distributionFileEvent',{fileEvent:type,...payload});
  },chunkStore,...Object.fromEntries(Object.entries(options).filter(([key])=>['chunkStore','requestTimeoutMs','requestWindow','maxAttemptsPerPeer','maxFileBytes','maxChunkBytes'].includes(key)))});
  async function sendBinary(link,payload){let ok=false,reason='Distribution connection unavailable',bufferWaitMs=0;try{
    const striped=Array.isArray(link.pc?.pcs),pcs=striped?link.pc.pcs:[link.pc],overhead=striped?9:0;
    link.queuedChunks??=new Map();
    if(coalesceQueuedFileChunk({dc:link.dc,striped,submittedBytes:link.submittedBytes??0,queuedChunks:link.queuedChunks,index:payload.file.index})){
      timeline(link,'retry-coalesced',{index:payload.file.index,bufferedBytes:link.dc.bufferedAmount});
      fire('FILE_BIN_CHUNK_SENT',{...payload,ok:true,bufferWaitMs:0});return;
    }

    ({bufferWaitMs}=await sendFileChannelPayload({dc:link.dc,pcs,bytes:payload.bytes,file:payload.file,send:data=>payloadSend(link,data),overhead,
      messageBytes:link.messagePolicy?.maxPayloadBytes??null,fragments:capabilityExchange?.get(link.peer)?.protocols.includes('phantom-file-fragments-v1')===true,
      queuePolicy:link.queuePolicy,timeoutMs:jobTimeoutMs,signal:link.sendController.signal,assertLive(){
        if(closed||links.get(link.job.id)!==link||!canUsePeer(link.peer)||!allowed(payload.file.id,payload.file.distributionId)||link.dc.readyState!=='open')throw Error('Distribution revoked or disconnected');
      }}));
    if(!striped&&link.dc.ordered===true&&link.dc.maxRetransmits===null&&link.dc.maxPacketLifeTime===null)link.queuedChunks.set(payload.file.index,link.submittedBytes);
    ok=true;
  }catch(e){reason=e.message;}fire('FILE_BIN_CHUNK_SENT',{...payload,ok,bufferWaitMs,error:ok?undefined:reason});}
  // Job timeout is an idle budget. Control keepalives alone cannot renew it.
  function progress(link,reason='payload_activity'){
    if(links.get(link.job.id)!==link)return;
    link.lastActivityAt=Date.now();link.lastActivityReason=reason;
    link.activity=(link.activity??0)+1;link.job.deadline=link.lastActivityAt+jobTimeoutMs;
    if(isLeader()){const job=jobs.get(link.job.id);if(job)job.deadline=link.job.deadline;}
  }
  function observeDrain(link){
    if(!link.dc)return;
    const drained=Math.min(link.payloadEnd??0,Math.max(0,(link.submittedBytes??0)-link.dc.bufferedAmount));
    if(drained>(link.drainedBytes??0)){link.drainedBytes=drained;progress(link,'sender_queue_drain');}
    timeline(link,'queue',{bufferedBytes:link.dc.bufferedAmount,submittedBytes:link.submittedBytes??0,drainedBytes:link.drainedBytes??0});
    if((link.activity??0)!==(link.announcedActivity??0)&&Date.now()-(link.lastAnnouncement??0)>=Math.min(1000,jobTimeoutMs/3)){
      link.lastAnnouncement=Date.now();link.announcedActivity=link.activity;
      frame(leader(),{type:'progress',jobId:link.job.id,batch:link.job.batch??0,sequence:link.activity});
    }
  }
  function payloadSend(link,data){link.dc.send(data);link.payloadEnd=link.submittedBytes;}
  function closeLink(id){pendingSignals.delete(id);closedJobs.set(id,Date.now());const link=links.get(id);if(!link)return;scout(link,'closing');timeline(link,'close',{bufferedBytes:link.dc?.bufferedAmount??null});link.releaseTransport?.('file_connection_closed');link.releaseTransport=null;links.delete(id);link.sendController.abort();link.receiveController?.abort();files.flushTimings(link.peer,link.job.asset);if(link.readyTimer)clearTimeout(link.readyTimer);link.readyTimer=null;link.pc.onicecandidate=null;link.pc.onconnectionstatechange=null;link.pc.ondatachannel=null;if(link.dc){link.dc.onopen=null;link.dc.onmessage=null;link.dc.onclose=null;try{link.dc.close();}catch{}}try{link.pc.close();}catch{}link.pendingIce.length=0;link.binary=null;emit('distributionConnectionClosed',{jobId:id,peer:link.peer});}
  async function receive(link){const job=link.job,startedAt=performance.now();let succeeded=false;
    emit('distributionBatchStarted',{jobId:job.id,batch:job.batch??0,total:job.indexes.length,peer:link.peer});try{
    await files.request(job.asset,{peers:[link.peer],indexes:job.indexes,signal:link.receiveController.signal,concurrentRanges:job.assigned===true});
    if(!ownsBatch(link,job))return;
    const asset=assets.get(job.asset);if(!asset)throw Error('Unknown distribution');
    const have=asset.holders.get(self())??new Set();for(const i of job.indexes)have.add(i);asset.holders.set(self(),have);dirty=true;
    link.completed=true;succeeded=true;emit('distributionBatchComplete',{jobId:job.id,batch:job.batch??0,total:job.indexes.length,peer:link.peer,elapsedMs:performance.now()-startedAt});frame(leader(),{type:'done',jobId:job.id,batch:job.batch??0,reuse:link.reuse===true});report();
    if(have.size===asset.total&&!finished.has(asset.id)){
      const blob=await files.request(asset.id,{peers:[link.peer]});finished.add(asset.id);emit('distributionComplete',{id:asset.id,blob});
    }
  }catch(e){
      // Finalization follows verified batch completion and may outlive its normal close.
      if(succeeded){error(e,{id:job.asset,jobId:job.id,batch:batchOf(job),reason:'finalize_failed'});return;}
      if(!ownsBatch(link,job))return;
      frame(leader(),{type:'failed',jobId:job.id,batch:batchOf(job),reason:'receive_failed'});
      emit('distributionConnectionFailed',{id:job.asset,jobId:job.id,batch:batchOf(job),peer:link.peer,reason:'receive_failed',error:e.message});
      error(e,{id:job.asset,jobId:job.id,batch:batchOf(job)});
    }finally{if(ownsBatch(link,job)&&(!succeeded||!link.reuse)){frame(link.peer,{type:'close',jobId:job.id,batch:batchOf(job)});closeLink(job.id);}}}

  async function startReceiver(link){
    if(links.get(link.job.id)!==link||link.transportPreparing||link.job.receiver!==self()||(link.requireReady&&!link.sourceReady)||link.started||link.dc?.readyState!=='open')return;
    let manifest;try{manifest=files.getManifest(link.job.asset);}catch{return;}
    const job=link.job;link.started=true;link.receiveController=new AbortController();
    try{
      if(chunkStore.save&&link.job.needsManifest)await chunkStore.save(networkId,{manifest,proof:assets.get(manifest.id).proof});
      if(!ownsBatch(link,job))return;
      await receive(link);
    }catch(e){if(!ownsBatch(link,job))return;frame(leader(),{type:'failed',jobId:job.id,batch:batchOf(job),reason:'storage_failed'});closeLink(job.id);error(e,{id:job.asset,jobId:job.id,batch:batchOf(job)});}
  }
  function probeSource(link){
    if(links.get(link.job.id)!==link||link.sourceReady||link.dc?.readyState!=='open'||Date.now()>link.job.deadline)return;
    traceChunk({direction:'send',kind:'ready_probe',jobId:link.job.id,peer:link.peer});
    let hasManifest=false;try{files.getManifest(link.job.asset);hasManifest=true;}catch{}
    link.dc.send(JSON.stringify({kind:'ready_probe',hasManifest,jobId:link.job.id,batch:link.job.batch??0,file:{id:link.job.asset}}));
    link.readyTimer=setTimeout(()=>{link.readyTimer=null;probeSource(link);},100);
  }
  function prepareFileTransport(link){
    if(link.releaseTransport||!options.prepareTransferTransport)return null;
    const context={peer:link.peer,id:link.job.id,lane:0,pc:link.pc,signal:link.sendController.signal,jobId:link.job.id,asset:link.job.asset,role:link.job.source===self()?'source':'receiver',scope:'file_connection'};
    const report=(status,observation)=>emit('distributionScout',{phase:'file-native-socket',peer:context.peer,id:context.id,jobId:context.jobId,asset:context.asset,role:context.role,status,observation});
    return Promise.resolve().then(()=>options.prepareTransferTransport(context)).then(resource=>{
      if(!resource)return;
      let released=false;
      const release=reason=>{if(released)return;released=true;Promise.resolve().then(()=>resource.release?.(reason)).then(observation=>{if(observation)report('released',observation);}).catch(()=>report('unavailable',{reason:'socket_release_failed'}));};
      if(links.get(link.job.id)!==link||link.sendController.signal.aborted){release('file_connection_closed');return;}
      link.releaseTransport=release;report('prepared',resource.report??{status:'unavailable'});
    }).catch(()=>{if(links.get(link.job.id)===link)report('unavailable',{reason:'socket_preparation_failed'});});
  }
  function bind(link,dc){if(dc.label!=='phantom-distribution'){dc.close();return;}link.dc=dc;dc.binaryType='arraybuffer';link.submittedBytes=0;link.payloadEnd=0;link.drainedBytes=0;link.manifestOffsets=new Set();link.requestedIndexes=new Set();
    const nativeSend=dc.send.bind(dc);dc.send=data=>{const result=nativeSend(data);link.submittedBytes+=typeof data==='string'?new TextEncoder().encode(data).byteLength:data.byteLength;return result;};
    const opened=()=>{if(links.get(link.job.id)!==link||link.opened)return;link.opened=true;scout(link,'open');timeline(link,'open',link.queuePolicy?{queuePolicy:link.queuePolicy}:{});emit('distributionConnectionOpened',{jobId:link.job.id,peer:link.peer});
      if(link.job.source===self()&&!link.requireReady&&link.job.needsManifest)void files.share(link.peer,link.job.asset).catch(error);
      if(link.job.receiver===self()){if(link.requireReady)probeSource(link);startReceiver(link);}
    };
    dc.onopen=()=>{if(links.get(link.job.id)!==link||link.opened||link.transportPreparing)return;const preparation=prepareFileTransport(link);if(!preparation){opened();return;}link.transportPreparing=preparation;preparation.then(()=>{link.transportPreparing=null;opened();});};
    function rejectFragment(){const job=link.job;link.fragment=null;link.binary=null;closeLink(job.id);frame(leader(),{type:'failed',jobId:job.id,batch:batchOf(job),reason:'invalid_fragment'});emit('distributionConnectionFailed',{id:job.asset,jobId:job.id,batch:batchOf(job),peer:link.peer,reason:'invalid_fragment'});}
    const handleMessage=event=>{if(links.get(link.job.id)!==link||!canUsePeer(link.peer)||!allowed(link.job.asset,assets.get(link.job.asset)?.distributionId))return;
      if(event.data instanceof ArrayBuffer){const item=link.binary;link.binary=null;if(item?.fragment){const f=link.fragment;if(!f||event.data.byteLength!==item.length||f.received!==item.offset){rejectFragment();return;}f.bytes.set(new Uint8Array(event.data),f.received);f.received+=item.length;if(f.received===f.bytes.byteLength){link.fragment=null;traceChunk({direction:'receive',kind:'binary',jobId:link.job.id,peer:link.peer,index:f.file.index});fire('FILE_BIN_CHUNK',{from:link.peer,file:f.file,bytes:f.bytes.buffer});}return;}const file=item;if(file?.id===link.job.asset){traceChunk({direction:'receive',kind:'binary',jobId:link.job.id,peer:link.peer,index:file.index});fire('FILE_BIN_CHUNK',{from:link.peer,file,bytes:event.data});}return;}
      let m;try{m=JSON.parse(event.data);}catch{return;}
      if(m.file?.id!==link.job.asset)return;
      if(m.kind==='ready_probe'&&m.jobId===link.job.id&&(m.batch??0)===(link.job.batch??0)&&link.job.source===self()){
        if(link.job.needsManifest&&!link.manifestShared){link.manifestShared=true;if(m.hasManifest!==true)void files.share(link.peer,link.job.asset).catch(error);}
        dc.send(JSON.stringify({kind:'ready',jobId:link.job.id,batch:link.job.batch??0,file:{id:link.job.asset}}));return;
      }
      if(m.kind==='ready'&&m.jobId===link.job.id&&(m.batch??0)===(link.job.batch??0)&&link.job.receiver===self()){
        traceChunk({direction:'receive',kind:'ready',jobId:link.job.id,peer:link.peer});
        link.sourceReady=true;if(link.readyTimer)clearTimeout(link.readyTimer);link.readyTimer=null;startReceiver(link);return;
      }
      if(m.kind==='message'&&m.sys==='PHANTOM_FILE_MANIFEST'&&Number.isInteger(m.file.offset)&&m.file.offset>=0&&Array.isArray(m.file.hashes)&&m.file.hashes.length>0&&m.file.hashes.length<=64&&m.file.offset+m.file.hashes.length<=assets.get(link.job.asset).total&&m.file.hashes.every(h=>/^[0-9a-f]{64}$/.test(h))&&!link.manifestOffsets.has(m.file.offset)){link.manifestOffsets.add(m.file.offset);progress(link);}
      if(m.kind==='message'&&m.sys==='PHANTOM_FILE_REQUEST'&&link.job.indexes.includes(m.file.index)&&!link.requestedIndexes.has(m.file.index)){link.requestedIndexes.add(m.file.index);progress(link);}
      if(m.kind==='binary-fragment'){
        const f=m.file,total=f?.bytes,offset=m.offset,length=m.length;
        if(!capabilityExchange||link.job.receiver!==self()||link.binary||!Number.isInteger(f?.index)||!link.job.indexes.includes(f.index)||!Number.isInteger(total)||total<1||total>(options.maxChunkBytes??65536)||!Number.isInteger(offset)||offset<0||!Number.isInteger(length)||length<1||offset+length>total){rejectFragment();return;}
        if(offset===0){if(link.fragment){rejectFragment();return;}link.fragment={file:f,bytes:new Uint8Array(total),received:0};}
        if(!link.fragment||link.fragment.file.id!==f.id||link.fragment.file.index!==f.index||link.fragment.bytes.byteLength!==total||link.fragment.received!==offset){rejectFragment();return;}
        link.binary={fragment:true,offset,length};return;
      }
      if(m.kind==='binary'){if(link.fragment||link.binary){rejectFragment();return;}link.binary=m.file;}
      else if(m.kind==='message'){if(['PHANTOM_FILE_REQUEST','PHANTOM_FILE_MISSING'].includes(m.sys))traceChunk({direction:'receive',kind:m.sys,jobId:link.job.id,peer:link.peer,index:m.file.index});fire('INCOMING_MSG',{from:link.peer,sys:m.sys,file:m.file});}
    };
    dc.onmessage=event=>{if(link.transportPreparing)link.transportPreparing.then(()=>handleMessage(event));else handleMessage(event);};
    if(dc.readyState==='open')dc.onopen();
    dc.onclose=()=>{if(links.get(link.job.id)===link){
      const interrupted=link.job.receiver===self()&&!link.completed;
      closeLink(link.job.id);
      // Receiver completion crosses SYS independently of this channel's close.
      // A normal close at the source must not invalidate verified receipt.
      if(interrupted){frame(leader(),{type:'failed',jobId:link.job.id,batch:batchOf(link.job),reason:'channel_closed'});emit('distributionConnectionFailed',{id:link.job.asset,jobId:link.job.id,batch:batchOf(link.job),peer:link.peer,reason:'channel_closed'});}
    }};
  }
  function configureLink(link){
    const pc=link.pc,peer=link.peer,job=link.job;
    pc.onicecandidate=e=>{if(e.candidate)frame(peer,{type:'signal',jobId:job.id,candidate:e.candidate.toJSON?.()??e.candidate});};
    pc.onconnectionstatechange=()=>{if(['failed','closed'].includes(pc.connectionState)&&links.get(job.id)===link){
      const current=link.job,interrupted=current.receiver===self()&&!link.completed;closeLink(current.id);
      if(interrupted){frame(leader(),{type:'failed',jobId:current.id,batch:batchOf(current),reason:'rtc_transport_failed'});emit('distributionConnectionFailed',{id:current.asset,jobId:current.id,batch:batchOf(current),peer:link.peer,reason:'rtc_transport_failed'});}
    }};
    pc.ondatachannel=e=>bind(link,e.channel);
  }
  async function open(job){if(links.has(job.id)||closedJobs.has(job.id))return;const used=[...links.values()].reduce((n,link)=>n+(link.job.assigned?1:transferLanes),0);if(links.size>=maxJobs||used+(job.assigned?1:transferLanes)>maxConnections){frame(leader(),{type:'failed',jobId:job.id,batch:batchOf(job)});return;}
    const asset=assets.get(job.asset);if(!asset||!allowed(asset.id,asset.distributionId))return;
    preflight?.suspend('file_job_started');
    const peer=job.source===self()?job.receiver:job.source;
    let link;
    try{
    const RTC=job.assigned===true?runtime.RTCPeerConnection:DistributionRTC;
    if(job.assigned===true&&!assignedRanges)throw Error('Assigned ranges are not supported');
    const retained=job.assigned===true&&job.source===self()?preflight?.take(peer,asset.outgoingSetup?.scoutId,undefined,job.asset,job.id,asset.outgoingSetup?.lanes??[0,1,2]):null;
    const pc=retained?.pc??new RTC(options.rtcConfiguration??defaultRTCConfiguration());link={pc,peer,job,releaseTransport:retained?.releaseTransport??null,sendController:new AbortController(),started:false,pendingIce:[]};links.set(job.id,link);
    if(retained&&asset.outgoingQueuePolicy){
      link.queuePolicy={...asset.outgoingQueuePolicy};
      emit('distributionScout',{phase:'preflight-queue-applied',id:retained.id,peer,asset:job.asset,lane:retained.lane,jobId:job.id,policy:link.queuePolicy,enforcement:'retained_source_channel'});
    }
    if(retained&&asset.outgoingMessagePolicy){
      link.messagePolicy={...asset.outgoingMessagePolicy};
      emit('distributionScout',{phase:'preflight-message-applied',id:retained.id,peer,asset:job.asset,lane:retained.lane,jobId:job.id,policy:link.messagePolicy,enforcement:'retained_source_channel'});
    }
    configureLink(link);
    if(retained){
      link.requireReady=true;link.reuse=reuseConnections;
      frame(peer,{type:'signal',jobId:job.id,scoutTransport:{id:retained.id,lane:retained.lane},readyProtocol:1,reuseProtocol:reuseConnections?2:0});
      bind(link,pc.createDataChannel('phantom-distribution'));return;
    }
    if(job.source===self()){
      bind(link,pc.createDataChannel('phantom-distribution'));await pc.setLocalDescription(await pc.createOffer());if(!ownsBatch(link,job))return;frame(peer,{type:'signal',jobId:job.id,readyProtocol:1,reuseProtocol:reuseConnections?2:0,description:{type:pc.localDescription.type,sdp:pc.localDescription.sdp}});
    }
    }catch(e){if(closed||(link&&!ownsBatch(link,job)))return;closeLink(job.id);frame(leader(),{type:'failed',jobId:job.id,batch:batchOf(job),reason:'rtc_setup_failed'});emit('distributionConnectionFailed',{jobId:job.id,peer,reason:'rtc_setup_failed',error:e.message});error(e);}
  }
  async function signal(from,body){if(closedJobs.has(body.jobId))return;const link=links.get(body.jobId);if(!link){if(pendingSignals.size<maxJobs*2){const pending=pendingSignals.get(body.jobId)??[];pending.createdAt??=Date.now();if(pending.length<64)pending.push({from,body});pendingSignals.set(body.jobId,pending);}return;}if(link.peer!==from)return;
    if(body.scoutTransport){
      const handoff=body.scoutTransport;
      if(!link.job.assigned||link.job.receiver!==self()||link.dc||link.pc.remoteDescription||typeof handoff.id!=='string'||!Number.isInteger(handoff.lane)||handoff.lane<0||handoff.lane>2)throw Error('Invalid scout handoff');
      const retained=preflight?.take(from,handoff.id,handoff.lane,link.job.asset,link.job.id);if(!retained)throw Error('Scout association unavailable');
      link.pc.onconnectionstatechange=null;link.pc.ondatachannel=null;link.pc.onicecandidate=null;link.pc.close();
      link.pc=retained.pc;link.releaseTransport=retained.releaseTransport;link.requireReady=true;link.reuse=reuseConnections&&body.reuseProtocol===2;configureLink(link);
      emit('distributionScout',{phase:'preflight-handoff',status:'bound',peer:from,id:retained.id,lane:retained.lane,jobId:link.job.id});
      if(retained.channel)bind(link,retained.channel);return;
    }
    if(body.description){link.requireReady=body.readyProtocol===1;
      // Version 2 requires batch-scoped failures and closes; older peers use fresh jobs.
      link.reuse=reuseConnections&&body.reuseProtocol===2;await link.pc.setRemoteDescription(body.description);for(const candidate of link.pendingIce.splice(0))await link.pc.addIceCandidate(candidate);
      if(body.description.type==='offer'){await link.pc.setLocalDescription(await link.pc.createAnswer());frame(from,{type:'signal',jobId:body.jobId,readyProtocol:1,reuseProtocol:reuseConnections?2:0,description:{type:link.pc.localDescription.type,sdp:link.pc.localDescription.sdp}});}
    }else if(body.candidate){if(link.pc.remoteDescription)await link.pc.addIceCandidate(body.candidate);else if(link.pendingIce.length<64)link.pendingIce.push(body.candidate);}
  }
  async function safeSignal(sender,body){const link=links.get(body.jobId),job=link?.job;try{return await signal(sender,body);}catch(e){if(!link||link.peer!==sender||!ownsBatch(link,job))return;closeLink(job.id);frame(leader(),{type:'failed',jobId:job.id,batch:batchOf(job),reason:'rtc_setup_failed'});emit('distributionConnectionFailed',{id:job.asset,jobId:job.id,batch:batchOf(job),peer:sender,reason:'rtc_setup_failed',error:e.message});error(e,{id:job.asset,jobId:job.id,batch:batchOf(job)});}}
  async function acceptAsset(body){if(!policy||typeof body.id!=='string'||!/^sha256:[0-9a-f]{64}$/.test(body.id)||!Number.isSafeInteger(body.total)||body.total<0||typeof body.distributionId!=='string'||body.distributionId.length>256)return null;
    const proof=await policy.verify(body.proof);if(!proof||proof.action!=='content.publish'||proof.target!==body.id||!allowed(body.id,body.distributionId))return null;
    let asset=assets.get(body.id);if(!asset){asset={id:body.id,total:body.total,distributionId:body.distributionId,proof,holders:new Map()};assets.set(body.id,asset);}else if(asset.total!==body.total||asset.distributionId!==body.distributionId||JSON.stringify(asset.proof.recipients)!==JSON.stringify(proof.recipients))return null;return asset;
  }
  async function handle(from,packet){if(!canUsePeer(from)||!canUsePeer(packet?.from)||closed||!self()||packet?.networkId!==networkId||typeof packet.from!=='string'||!packet.body)return;await restore();if(closed||!canUsePeer(from)||!canUsePeer(packet?.from))return;syncLeadership();
    if(packet.to!==self()){
      // Only the current leader forwards, retaining the authenticated originating peer.
      if(isLeader()&&packet.from===from&&roster.includes(from)&&roster.includes(packet.to))core.command('PHANTOM_DIST_SEND',{to:packet.to,frame:packet});return;
    }
    if(packet.from!==from&&from!==leader())return;
    const body=packet.body,sender=packet.from;
    if(body.type==='transferScout'){preflight?.receive(sender,body);return;}
    if(['capabilityRequest','capabilityReport'].includes(body.type)){if(body.type==='capabilityRequest'&&!await observeControl(sender))return;if(!closed)capabilityExchange?.receive(sender,body);return;}
    if(['planProposal','planDecision'].includes(body.type)){agreement?.receive(sender,body);return;}
    if(body.type==='rangeCapability'&&isLeader()&&roster.includes(sender)&&body.version===1){rangePeers.add(sender);frame(sender,{type:'rangeCapabilityAck',version:1,scoutScheduling:1,scoutLaneScheduling:1});return;}
    if(body.type==='rangeCapabilityAck'&&sender===leader()&&body.version===1&&body.scoutScheduling===1){if(!scoutCoordinator)reported.clear();scoutCoordinator=body.scoutLaneScheduling===1?2:1;return;}
    if(body.type==='inventory'&&isLeader()&&roster.includes(sender)){
      const asset=await acceptAsset(body);if(!asset||!Array.isArray(body.indexes)||body.indexes.length>128||body.indexes.some(i=>!Number.isInteger(i)||i<0||i>=asset.total))return;
      const setup=body.setup;
      for(const key of asset.sourceLimits?.keys()??[])if(key.startsWith(sender+':'))asset.sourceLimits.delete(key);
      if(setup&&((setup.version===1&&[1,3].includes(setup.connections))||(setup.version===2&&[1,2,3].includes(setup.connections)&&Array.isArray(setup.lanes)&&setup.lanes.length===setup.connections&&setup.lanes.every((lane,i)=>Number.isInteger(lane)&&lane>=0&&lane<3&&(i===0||lane>setup.lanes[i-1]))))&&typeof setup.receiver==='string'&&roster.includes(setup.receiver)&&setup.receiver!==sender&&(!asset.proof.recipients||asset.proof.recipients.includes(setup.receiver))&&typeof setup.scoutId==='string'&&/^[a-zA-Z0-9-]{1,64}$/.test(setup.scoutId)){asset.sourceLimits??=new Map();asset.sourceLimits.set(sender+':'+setup.receiver,setup.connections);}
      const have=asset.holders.get(sender)??new Set(),old=have.size;for(const i of body.indexes)have.add(i);asset.holders.set(sender,have);if(have.size!==old)dirty=true;frame(sender,{type:'inventoryAck',id:asset.id,indexes:body.indexes,...(setup&&asset.sourceLimits?.get(sender+':'+setup.receiver)===setup.connections?{setup}:{})});return;
    }
    if(body.type==='inventoryAck'&&sender===leader()){const localAsset=assets.get(body.id);if(body.setup&&localAsset?.outgoingSetup&&JSON.stringify(body.setup)===JSON.stringify(localAsset.outgoingSetup)&&!localAsset.setupAcknowledged){localAsset.setupAcknowledged=true;emit('distributionScout',{phase:'preflight-applied',peer:body.setup.receiver,id:body.setup.scoutId,asset:body.id,connections:body.setup.connections,...(body.setup.lanes?{lanes:body.setup.lanes}:{}),enforcement:'coordinator_acknowledged'});}const have=localAsset?.holders.get(self());if(!have||!Array.isArray(body.indexes)||body.indexes.length>128)return;const sent=reported.get(body.id)??new Set();for(const i of body.indexes)if(have.has(i))sent.add(i);reported.set(body.id,sent);return;}
    if(body.type==='mirrorJobs'&&sender===leader()&&state().role==='syscon2'){if(!Number.isInteger(body.sequence)||body.sequence<receivedSequence)return;receivedSequence=body.sequence;if(body.reset)mirrorJobs.clear();else if(body.job&&mirrorJobs.size<1024)mirrorJobs.set(body.job.id,body.job);return;}
    if(body.type==='mirror'&&sender===leader()&&state().role==='syscon2'){
      if(!Number.isInteger(body.sequence)||body.sequence<receivedSequence)return;receivedSequence=body.sequence;
      const asset=await acceptAsset(body.asset);if(!asset||!Array.isArray(body.indexes)||body.indexes.length>128||body.indexes.some(i=>!Number.isInteger(i)||i<0||i>=asset.total))return;
      const have=body.reset&&body.peer!==self()?new Set():asset.holders.get(body.peer)??new Set();for(const i of body.indexes)have.add(i);asset.holders.set(body.peer,have);return;
    }
    if(['job','continue'].includes(body.type)&&sender===leader()){
      const job=body.job;if(!job||typeof job.id!=='string'||job.id.length>128||![job.source,job.receiver].includes(self())||job.source===job.receiver||!roster.includes(job.source)||!roster.includes(job.receiver)||!Array.isArray(job.indexes)||job.indexes.length>batchSize)return;
      const asset=await acceptAsset(body.asset);if(!asset||(asset.proof.recipients&&!asset.proof.recipients.includes(job.receiver))||job.indexes.some(i=>!Number.isInteger(i)||i<0||i>=asset.total))return;
      if(body.type==='continue'){
        const link=links.get(job.id);
        if(!link?.reuse||link.dc?.readyState!=='open'||job.asset!==link.job.asset||job.source!==link.job.source||job.receiver!==link.job.receiver||!!job.assigned!==!!link.job.assigned||body.previousBatch!==(link.job.batch??0)||job.batch!==body.previousBatch+1)return;
        if(job.receiver===self()&&!link.completed)return;
        link.job=job;link.started=false;link.completed=false;link.sourceReady=false;link.binary=null;link.fragment=null;link.requestedIndexes.clear();link.activity=0;link.announcedActivity=0;link.leases={};
        if(job.receiver===self())probeSource(link);
        return;
      }
      await open(job);const pending=pendingSignals.get(job.id)??[];pendingSignals.delete(job.id);for(const message of pending)await safeSignal(message.from,message.body);return;
    }
    if(body.type==='progress'&&isLeader()){
      const job=jobs.get(body.jobId);if(!job||![job.source,job.receiver].includes(sender)||(body.batch??0)!==(job.batch??0)||!Number.isSafeInteger(body.sequence)||body.sequence<1)return;
      job.progress??={};if(body.sequence<=(job.progress[sender]??0))return;job.progress[sender]=body.sequence;job.deadline=Date.now()+jobTimeoutMs;dirty=true;
      for(const peer of [job.source,job.receiver])frame(peer,{type:'progressLease',jobId:job.id,batch:job.batch??0,sequence:body.sequence,reporter:sender});return;
    }
    if(body.type==='progressLease'&&sender===leader()){
      const link=links.get(body.jobId);if(!link||(body.batch??0)!==(link.job.batch??0)||![link.job.source,link.job.receiver].includes(body.reporter)||!Number.isSafeInteger(body.sequence)||body.sequence<1)return;
      link.leases??={};if(body.sequence<=(link.leases[body.reporter]??0))return;link.leases[body.reporter]=body.sequence;link.job.deadline=Date.now()+jobTimeoutMs;return;
    }
    if(body.type==='signal')return safeSignal(sender,body);
    if(body.type==='close'){
      const link=links.get(body.jobId);
      if(link&&(link.peer===sender||sender===leader())&&sameBatch(link.job,body.batch)){
        closeLink(body.jobId);
      }else if(!link&&sender===leader()&&typeof body.jobId==='string'&&body.jobId.length<=128&&sameBatch({batch:0},body.batch))closeLink(body.jobId);
      return;
    }
    if(['done','failed'].includes(body.type)&&isLeader()){
      const job=jobs.get(body.jobId);if(!job||![job.source,job.receiver].includes(sender)||!sameBatch(job,body.batch))return;
      // Only the receiver confirms verified receipt.
      if(body.type==='done'&&(sender!==job.receiver||(body.batch??0)!==(job.batch??0)))return;
      if(body.type==='done'){const a=assets.get(job.asset),have=a?.holders.get(sender)??new Set();for(const i of job.indexes)have.add(i);a?.holders.set(sender,have);dirty=true;setupFailures.delete(job.source+':'+job.receiver);blockedUntil.delete(job.source+':'+job.receiver);}
      else {const pair=job.source+':'+job.receiver;let delay=1000;if(body.reason==='rtc_setup_failed'){const failures=(setupFailures.get(pair)??0)+1;setupFailures.set(pair,failures);delay=Math.min(60000,jobTimeoutMs*2**Math.min(failures-1,3));}blockedUntil.set(pair,Date.now()+delay);emit('distributionJobFailed',{jobId:job.id,source:job.source,receiver:job.receiver,reportedBy:sender,reason:body.reason??'transfer_failed',retryAfterMs:delay});}
      if(body.type==='done'&&reuseConnections&&body.reuse===true&&!(assignedRanges&&!job.assigned&&rangePeers.has(job.source)&&rangePeers.has(job.receiver))){
        const asset=assets.get(job.asset),have=asset?.holders.get(job.receiver),source=asset?.holders.get(job.source);
        // A live inventory cursor visits each piece once across reused batches.
        // Newly acquired pieces remain visible without rescanning the file or reopening RTC.
        let selection=continuationSelections.get(job);const wanted=[];
        if(source&&have&&have.size<asset.total){
          const reserved=new Set([...jobs.values()].filter(j=>j!==job&&j.asset===job.asset&&j.receiver===job.receiver).flatMap(j=>j.indexes));
          if(!selection||selection.source!==source)selection={source,iterator:source.values()};
          while(wanted.length<batchSize){const step=selection.iterator.next();if(step.done)break;if(!have.has(step.value)&&!reserved.has(step.value))wanted.push(step.value);}
          wanted.sort((a,b)=>a-b);
        }
        if(wanted.length&&roster.includes(job.source)&&roster.includes(job.receiver)&&canUsePeer(job.source)&&canUsePeer(job.receiver)&&allowed(asset.id,asset.distributionId)){
          const previousBatch=job.batch??0;
          const next={...job,indexes:wanted,needsManifest:false,batch:previousBatch+1,deadline:Date.now()+jobTimeoutMs,progress:{}};
          continuationSelections.set(next,selection);jobs.set(job.id,next);dirty=true;
          const continuation={type:'continue',previousBatch,job:next,asset:{id:asset.id,total:asset.total,distributionId:asset.distributionId,proof:asset.proof}};
          frame(next.source,continuation);frame(next.receiver,continuation);emit('distributionConnectionReused',{jobId:next.id,peer:next.source,batch:next.batch});return;
        }
      }
      finishJob(job);return;
    }
  }
  function finishJob(job){if(jobs.get(job.id)!==job)return;jobs.delete(job.id);dirty=true;for(const peer of [job.source,job.receiver])frame(peer,{type:'close',jobId:job.id,batch:batchOf(job)});}
  function schedule(){if(!isLeader()||checkedToken!==leaderToken)return;
    const live=new Set(roster.filter(canUsePeer)),now=Date.now();
    for(const job of [...jobs.values()])if(now>job.deadline||!live.has(job.source)||!live.has(job.receiver)||!allowed(job.asset,assets.get(job.asset)?.distributionId)){if(now>job.deadline)blockedUntil.set(job.source+':'+job.receiver,now+1000);finishJob(job);}
    for(const asset of assets.values()){
      if(!allowed(asset.id,asset.distributionId))continue;
      for(const [peer]of asset.holders)if(!live.has(peer)){asset.holders.delete(peer);dirty=true;}
      const counts=peer=>[...jobs.values()].filter(j=>j.source===peer||j.receiver===peer).reduce((n,j)=>n+(j.assigned?1:transferLanes),0);
      const canReceive=receiver=>{
        if(!live.has(receiver)||(asset.proof.recipients&&!asset.proof.recipients.includes(receiver)))return false;
        const have=asset.holders.get(receiver)??new Set();
        const active=[...jobs.values()].filter(j=>j.receiver===receiver);
        return !(have.size===asset.total&&(asset.total>0||asset.holders.has(receiver)))&&(!active.length||(assignedRanges&&rangePeers.has(receiver)&&asset.holders.has(receiver)&&active.every(j=>j.assigned===true&&j.asset===asset.id&&rangePeers.has(j.source))))&&counts(receiver)<maxConnections;
      };
      const receivers=roster.filter(canReceive);
      if(!receivers.length)continue;
      // Count reserved pieces only when a receiver can actually accept a new job.
      const copies=new Map();for(const indexes of asset.holders.values())for(const i of indexes)copies.set(i,(copies.get(i)??0)+1);
      for(const job of jobs.values())if(job.asset===asset.id)for(const i of job.indexes)copies.set(i,(copies.get(i)??0)+1);
      const rarity=i=>copies.get(i)??0;
      for(const receiver of receivers){
        if(!canReceive(receiver))continue;
        const have=asset.holders.get(receiver)??new Set();
        const candidates=[...asset.holders].filter(([peer,indexes])=>{const useAssigned=assignedRanges&&rangePeers.has(receiver)&&rangePeers.has(peer),cost=useAssigned?1:transferLanes;const pairLimit=asset.sourceLimits?.get(peer+':'+receiver)??maxConnections,pairCount=[...jobs.values()].filter(j=>j.asset===asset.id&&j.source===peer&&j.receiver===receiver).length;return peer!==receiver&&live.has(peer)&&pairCount<pairLimit&&counts(peer)+cost<=maxConnections&&counts(receiver)+cost<=maxConnections&&(![...jobs.values()].some(j=>j.receiver===receiver)||useAssigned)&&(blockedUntil.get(peer+':'+receiver)??0)<=now&&(asset.total===0||[...indexes].some(i=>!have.has(i)));}).sort((a,b)=>counts(a[0])-counts(b[0])||a[1].size-b[1].size||a[0].localeCompare(b[0]));
        const reserved=new Set([...jobs.values()].filter(j=>j.asset===asset.id&&j.receiver===receiver).flatMap(j=>j.indexes));
        const candidate=candidates.find(([,indexes])=>asset.total===0||[...indexes].some(i=>!have.has(i)&&!reserved.has(i)));
        if(!candidate)continue;const [source,indexes]=candidate,wanted=[...indexes].filter(i=>!have.has(i)&&!reserved.has(i)).sort((a,b)=>rarity(a)-rarity(b)||a-b).slice(0,batchSize);
        const job={id:crypto.randomUUID(),asset:asset.id,source,receiver,indexes:wanted,needsManifest:!asset.holders.has(receiver),deadline:now+jobTimeoutMs,...(assignedRanges&&rangePeers.has(source)&&rangePeers.has(receiver)?{assigned:true}:{})};jobs.set(job.id,job);for(const i of wanted)copies.set(i,rarity(i)+1);dirty=true;
        const body={type:'job',job,asset:{id:asset.id,total:asset.total,distributionId:asset.distributionId,proof:asset.proof}};
        // Receiver first so an immediately delivered offer has an assigned owner.
        frame(receiver,body);frame(source,body);emit('distributionJob',{...job});
      }
    }
  }
  function syncLeadership(){const token=leader()+':'+state().mirror;
    if(token!==leaderToken){leaderToken=token;checkedToken='';receivedSequence=-1;rangePeers.clear();scoutCoordinator=false;preflight?.clear();for(const a of assets.values()){a.sourceLimits?.clear();delete a.outgoingSetup;delete a.outgoingQueuePolicy;delete a.outgoingMessagePolicy;a.setupAcknowledged=false;}controlObservations.clear();controlGenerations.clear();controlPending.clear();capabilityExchange?.clear();agreement?.clear();rangeAdvertisement='';reported.clear();mirrored.clear();mirrorJobs.clear();for(const job of [...jobs.values()])finishJob(job);for(const id of [...links.keys()]){const link=links.get(id);closeLink(id);}dirty=true;lastReport=0;}
  }
  function restore(){
    if(restoration)return restoration;
    if(closed)return Promise.reject(Error('Distribution closed'));
    if(!self())return Promise.reject(Error('Peer identity required before restoration'));
    restorationState='restoring';
    restoration=(async()=>{
      const records=chunkStore.load?await chunkStore.load(networkId):[];
      if(!Array.isArray(records))throw Error('Invalid stored distribution records');
      for(const record of records){
        if(closed)throw Error('Distribution closed');
        try{
          const proof=await policy?.verify(record?.proof);
          if(!proof||proof.action!=='content.publish'||proof.target!==record?.manifest?.id||!allowed(proof.target,record.manifest.distributionId))throw Error('Stored publication is invalid or revoked');
          const restored=await files.restore(record.manifest);
          if(closed)throw Error('Distribution closed');
          const asset=await acceptAsset({...restored.manifest,proof});
          if(!asset)throw Error('Stored publication policy conflicts with current state');
          if(closed||!allowed(asset.id,asset.distributionId))throw Error('Distribution unavailable');
          asset.holders.set(self(),new Set(restored.indexes));dirty=true;
          if(restored.indexes.length===asset.total)finished.add(asset.id);
          const result={id:asset.id,received:restored.indexes.length,total:asset.total,complete:restored.indexes.length===asset.total};
          restoreResults.push({status:'restored',...result});emit('distributionRestored',result);
        }catch(e){if(closed)throw e;const result={id:record?.manifest?.id??null,error:e.message};restoreResults.push({status:'rejected',...result});emit('distributionRestoreRejected',result);}
      }
      if(closed)throw Error('Distribution closed');
      restorationState='ready';lastReport=0;
    })().catch(e=>{restorationState='failed';throw e;});
    return restoration;
  }
  async function tick(){if(closed||!self()||restorationState==='failed')return;await restore();if(closed)return;for(const [id,at]of closedJobs)if(Date.now()-at>jobTimeoutMs*2)closedJobs.delete(id);for(const [id,pending]of pendingSignals)if(Date.now()-pending.createdAt>jobTimeoutMs)pendingSignals.delete(id);const token=leader()+':'+state().mirror;
    syncLeadership();
    if(isLeader()&&state().state==='admitted'&&checkedToken!==token&&!checking){checking=true;try{let approved=true;if(checkAdmin)approved=await checkAdmin();else{const orders=await readAdminOrders({networkId,peerId:self()});if(!Array.isArray(orders))throw Error('Admin orders must be an array');for(const record of orders){if(!await lifecycle?.apply(record))throw Error('Invalid admin order');}}if(approved&&!closed&&leaderToken===token){checkedToken=token;emit('distributionAdminChecked',{peerId:self()});}}catch(e){error(e);}finally{checking=false;}}
    for(const link of links.values())observeDrain(link);
    if(Date.now()-lastReport>1000){if(state().role==='peer'&&state().state!=='admitted')core.command('PHANTOM_DIST_RECOVER_SYS',{});report();}schedule();mirror();
    for(const [id,link]of links)if(Date.now()>link.job.deadline){closeLink(id);frame(leader(),{type:'failed',jobId:id,batch:batchOf(link.job)});}
  }
  const off=core.onEvent(event=>{if(event.type==='PRESENCE_ROSTER')setRoster(event.payload?.list??[]);if(event.type==='PEER_CONNECTION_STATE'&&event.payload?.state!=='ready'){const peer=event.payload?.peerId;if(peer){controlObservations.delete(peer);controlGenerations.delete(peer);controlPending.delete(peer);clearPeerSetup(peer);capabilityExchange?.drop(peer);agreement?.drop(peer);}}if(event.type==='PHANTOM_ADMIN_LINK_READY'){rangeAdvertisement='';reported.clear();mirrored.clear();dirty=true;lastReport=0;}if(event.type==='PHANTOM_DIST')void handle(event.payload.from,event.payload.frame).catch(error);});
  const timer=setInterval(()=>{void tick().catch(error);},intervalMs);
  return {
    dropPeer(peer){files.dropPeer(peer);controlObservations.delete(peer);controlGenerations.delete(peer);controlPending.delete(peer);clearPeerSetup(peer);capabilityExchange?.drop(peer);agreement?.drop(peer);rangePeers.delete(peer);roster=roster.filter(p=>p!==peer);for(const asset of assets.values())asset.holders.delete(peer);for(const job of [...jobs.values()])if(job.source===peer||job.receiver===peer)finishJob(job);for(const [id,link]of [...links])if(link.peer===peer){closeLink(id);}for(const [id,job]of mirrorJobs)if(job.source===peer||job.receiver===peer)mirrorJobs.delete(id);dirty=true;},
    requestCapabilities(peer){if(!capabilityExchange)throw Error('Capability exchange is disabled');if(!peerAllowed(peer))throw Error('Peer unavailable');syncLeadership();void observeControl(peer).then(valid=>{if(valid&&!closed&&peerAllowed(peer))capabilityExchange.request(peer);}).catch(error);},
    getCapabilities(peer){return capabilityExchange?.get(peer)??null;},
    proposePlan(peer,settings={}){if(!agreement)throw Error('Capability exchange is disabled');return agreement.propose(peer,{...settings,protocolPreference:['phantom-file-v1']});},
    getPlan(peer){const plan=agreement?.get(peer);return plan?{...plan,setup:selectedSetup(peer)}:null;},
    getScout(peer){return selectedSetup(peer);},
    skipScout(){preflight?.suspend('send_without_waiting');},
    restore,
    async getFile(id){await restore();const asset=assets.get(id);if(closed||!asset||asset.holders.get(self())?.size!==asset.total)throw Error('No complete local distribution copy');return files.getFile(id);},
    async prepare(file,settings={}){preflight?.suspend();if(closed)throw Error('Distribution closed');await restore();const manifest=await files.publish(file,settings);preparedFiles.set(manifest,{file,id:manifest.id});return manifest;},
    async prepareAgreed(file,peer,settings={}){
      if('chunkSize' in settings)throw TypeError('Chunk size comes from the agreed plan');
      const agreed=agreement?.get(peer);if(!agreed)throw Error('No agreed plan');
      if(options.scoutBeforeSend===true&&capabilityExchange?.get(peer)?.protocols.includes(TRANSFER_SCOUT_PROTOCOL)){
        const workload=assignedRanges&&scoutCoordinator===2&&capabilityExchange.get(peer).protocols.includes(FILE_WORKLOAD_PROTOCOL);
        await preflight?.forSend(peer,{connectionOnly:workload});
        if(workload){await preflight?.qualifyFileWorkload(peer,{chunkSize:agreed.plan.payloadBytes,fileBytes:file.size,sourceReadBlockBytes:settings.sourceReadBlockBytes??0});if(preflight?.get(peer)?.fileWorkloadSelection?.status==='blocked')throw Error('Could not verify a working file connection. Reconnect and try again.');}
      }
      const setup=selectedSetup(peer);preflight?.suspend();
      if(closed||!peerAllowed(peer))throw Error('Peer unavailable');
      if(agreement?.get(peer)?.id!==agreed.id)throw Error('Agreed plan changed during file preparation');
      await restore();const manifest=await files.publish(file,{...settings,chunkSize:agreed.plan.payloadBytes});
      if(closed||!peerAllowed(peer)||agreement.get(peer)?.id!==agreed.id)throw Error('Agreed plan no longer valid');
      preparedFiles.set(manifest,{file,id:manifest.id,peer,planId:agreed.id,setup});return manifest;
    },
    async publish(file,{authorization,recipients,prepared,...settings}={}){if(closed)throw Error('Distribution closed');if(!state().admitted)throw Error('Wait for network admission before distributing');if(!policy)throw Error('Distribution requires an admin authority');await restore();if(closed)throw Error('Distribution closed');
      if(recipients!==undefined){
        if(!Array.isArray(recipients)||recipients.length>128||recipients.some(id=>typeof id!=='string'||! /^[0-9a-f]{16}$/.test(id)))throw new TypeError('Invalid publication recipients');
        recipients=[...new Set(recipients)].sort();
      }
      let manifest,setup=recipients?.length===1?selectedSetup(recipients[0]):null;if(setup?.fileWorkloadSelection?.status==='blocked')throw Error('Could not verify a working file connection. Reconnect and try again.');preflight?.suspend();
      if(prepared!==undefined){
        const record=preparedFiles.get(prepared);
        if(!record||record.file!==file||Object.keys(settings).length)throw new TypeError('Use an unchanged preparation from this distribution session without file settings');
        if(record.peer&&(!recipients||recipients.length!==1||recipients[0]!==record.peer||!peerAllowed(record.peer)||agreement?.get(record.peer)?.id!==record.planId))throw Error('Agreed plan no longer matches the recipient');
        manifest=files.getManifest(record.id);setup=record.setup??null;
      }else manifest=await files.publish(file,settings);
      const existing=assets.get(manifest.id);
      if(!authorization&&existing&&JSON.stringify(existing.proof.recipients)!==JSON.stringify(recipients))throw Error('Publication recipients are immutable; use a different distributionId');
      const proof=authorization??await lifecycle?.issue('content.publish',manifest.id,{recipients});const verified=await policy.verify(proof);if(!verified||verified.action!=='content.publish'||verified.target!==manifest.id)throw Error('Admin publish authorization required');
      if(recipients!==undefined&&JSON.stringify(recipients)!==JSON.stringify(verified.recipients))throw Error('Recipients do not match admin publish authorization');
      if(existing&&JSON.stringify(existing.proof.recipients)!==JSON.stringify(verified.recipients))throw Error('Publication recipients are immutable; use a different distributionId');
      const asset=await acceptAsset({...manifest,proof:verified});if(!asset)throw Error('Distribution unavailable');
      delete asset.outgoingQueuePolicy;delete asset.outgoingMessagePolicy;
      if(setup&&recipients?.length===1&&scoutCoordinator===2){
        if((setup.queuePolicy?.status==='measured'&&setup.queuePolicy.highWaterMark===1024*1024&&setup.queuePolicy.lowWaterMark===512*1024)||(setup.queuePolicy?.status==='workload_verified'&&setup.queuePolicy.highWaterMark===setup.fileWorkload?.profile?.queuePolicy?.highWaterMark&&setup.queuePolicy.lowWaterMark===setup.fileWorkload?.profile?.queuePolicy?.lowWaterMark))
          asset.outgoingQueuePolicy={highWaterMark:setup.queuePolicy.highWaterMark,lowWaterMark:setup.queuePolicy.lowWaterMark};
        if(setup.messagePolicy?.status==='measured'&&Number.isSafeInteger(setup.messagePolicy.maxPayloadBytes)&&setup.messagePolicy.maxPayloadBytes>=1024&&setup.messagePolicy.maxPayloadBytes<=262144&&capabilityExchange?.get(recipients[0])?.protocols.includes('phantom-file-fragments-v1'))
          asset.outgoingMessagePolicy={maxPayloadBytes:setup.messagePolicy.maxPayloadBytes};
        asset.outgoingSetup={version:2,receiver:recipients[0],connections:setup.connections,lanes:[...setup.lanes],scoutId:setup.id};
        emit('distributionScout',{phase:'preflight-selected',id:setup.id,peer:recipients[0],asset:asset.id,selection:setup,enforcement:'awaiting_coordinator_acknowledgment'});
      }
      if(chunkStore.save)await chunkStore.save(networkId,{manifest,proof:asset.proof});
      if(closed||!allowed(asset.id,asset.distributionId))throw Error('Distribution unavailable');asset.holders.set(self(),new Set(Array.from({length:manifest.total},(_,i)=>i)));finished.add(manifest.id);dirty=true;report();return manifest;
    },
    snapshot:()=>({restoration:restorationState,restoreResults:structuredClone(restoreResults),leader:leader(),adminChecked:isLeader()&&checkedToken===leaderToken,assets:table().assets,jobs:[...(isLeader()?jobs:mirrorJobs).values()].map(j=>({...j,indexes:[...j.indexes]})),connections:[...links.values()].map(l=>({jobId:l.job.id,peer:l.peer,state:l.dc?.readyState??'connecting'}))}),
    stop(target){files.stop(target);for(const job of [...jobs.values()])if(job.asset===target||assets.get(job.asset)?.distributionId===target)finishJob(job);for(const [id,link]of [...links])if(link.job.asset===target||assets.get(link.job.asset)?.distributionId===target)closeLink(id);},
    close(){if(closed)return;preflight?.close();closed=true;controlObservations.clear();controlGenerations.clear();controlPending.clear();agreement?.close();capabilityExchange?.close();clearInterval(timer);off();files.close();for(const id of [...links.keys()])closeLink(id);jobs.clear();assets.clear();progressIndexes.clear();reported.clear();mirrored.clear();mirrorJobs.clear();pendingSignals.clear();closedJobs.clear();blockedUntil.clear();setupFailures.clear();finished.clear();listeners.clear();}
  };
}
