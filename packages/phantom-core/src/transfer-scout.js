import {defaultRTCConfiguration} from './rtc-configuration.js';
import {scoutConnection,compareScoutTransport} from './connection-scout.js';
import {makeReceiverEvidence,readReceiverEvidence} from './scout-receiver-evidence.js';
import {compareWorkloadQueues,workloadQueuePolicy} from './workload-queue-comparison.js';
import {decideFileWorkload} from './file-workload-decision.js';
import {createScoutFileWorkload} from './scout-file-workload.js';
import {requestWindowCandidates,requestWindowOrder,compareRequestWindows,readRequestWindowDelivery} from './request-window-comparison.js';
import {FILE_RECEIVE_SLOTS} from './files.js';
import {buildScoutConnectionPlan} from './scout-connection-plan.js';

export const FILE_WORKLOAD_PROTOCOL='phantom-file-workload-v7';
export const TRANSFER_SCOUT_PROTOCOL = 'phantom-transfer-scout-v8';
const LANES = 3, MAX_BYTES = 3 * 1024 * 1024, DEFAULT_BYTES = 768 * 1024;
const CONFIRM_SCALE = 4, SCREEN_ROUNDS = 14, CONFIRM_ROUNDS = 6, LANE_ROUNDS = 21, QUEUE_ROUNDS = 4, ROUNDS = 25, QUEUE_BYTES = 768 * 1024;
const TIGHT_QUEUE_BYTES=1024*1024, DEFAULT_QUEUE_BYTES=16*1024*1024, QUEUE_SAMPLE_SCALE=12;
const CANDIDATES=[[0],[1,2],[0,1,2],[1],[0,2],[2],[0,1]], FINAL_ORDER=[1,2,3,3,2,1];
const laneKey=lanes=>lanes.join(',');
const validLanes=lanes=>Array.isArray(lanes)&&lanes.length>=1&&lanes.length<=3&&lanes.every((lane,i)=>Number.isInteger(lane)&&lane>=0&&lane<3&&(i===0||lane>lanes[i-1]));
const validId = id => typeof id === 'string' && /^[a-zA-Z0-9-]{1,64}$/.test(id);

function lateRate(sample) {
  const points=sample.delivery;
  if(!Array.isArray(points)||points.length<3||points.length>6||points.some(p=>!p||typeof p!=='object')||points[0].bytes!==0||points[0].elapsedMs!==0)return null;
  for(let i=1;i<points.length;i++)if(!Number.isSafeInteger(points[i].bytes)||points[i].bytes<=points[i-1].bytes||!Number.isFinite(points[i].elapsedMs)||points[i].elapsedMs<points[i-1].elapsedMs)return null;
  const last=points.at(-1),middle=points.find(p=>p.bytes>=sample.bytes/2&&p.bytes<=sample.bytes*3/4);
  if(last.bytes!==sample.bytes||last.elapsedMs!==sample.elapsedMs||!middle||last.elapsedMs<=middle.elapsedMs)return null;
  return (last.bytes-middle.bytes)*1000/(last.elapsedMs-middle.elapsedMs);
}

function sampleRanges(rows,field='elapsedMs') {
  return CANDIDATES.filter(lanes=>rows.some(s=>laneKey(s.lanes)===laneKey(lanes))).map(lanes=>{
    const values=rows.filter(s=>laneKey(s.lanes)===laneKey(lanes)).map(s=>field==='elapsedMs'?s.elapsedMs:lateRate(s));
    return {lanes:[...lanes],connections:lanes.length,min:Math.min(...values),max:Math.max(...values)};
  });
}
function clearWinner(ranges,higher=false){
  return ranges.find(candidate=>ranges.every(other=>other===candidate||(higher?candidate.min>other.max:candidate.max<other.min)))??null;
}
/** Screening is exploration, not a confirmed selection. Rank by the slower
 * repetition, then the faster repetition, then lane identity for stable ties.
 * Confirmation still needs independent repeated evidence and a clear winner.
 */
export function screenTransferCandidates(samples) {
  if(!Array.isArray(samples)||samples.length!==SCREEN_ROUNDS||samples.some(s=>!s||s.stage!=='screen'||!validLanes(s.lanes)||s.connections!==s.lanes.length||!Number.isSafeInteger(s.bytes)||s.bytes<1||!Number.isFinite(s.elapsedMs)||s.elapsedMs<=0)||new Set(samples.map(s=>s.bytes)).size!==1)return null;
  const ranges=sampleRanges(samples);
  if(ranges.length!==7||ranges.some(r=>samples.filter(s=>laneKey(s.lanes)===laneKey(r.lanes)).length!==2))return null;
  return [1,2,3].map(count=>ranges.filter(r=>r.connections===count).sort((a,b)=>a.max-b.max||a.min-b.min||laneKey(a.lanes).localeCompare(laneKey(b.lanes)))[0].lanes);
}

/** Screen exact lane sets to nominate finalists, then qualify on independent
 * repeated larger payloads and late delivery. Screening overlap is not a veto.
 */
export function selectTransferSetup(samples, fallback = 3) {
  const fallbackLanes=Array.from({length:[1,2,3].includes(fallback)?fallback:3},(_,i)=>i);
  const inconclusive=reason=>({status:'inconclusive',connections:fallbackLanes.length,lanes:fallbackLanes,reason});
  if(![1,2,3].includes(fallback)||!Array.isArray(samples)||samples.length!==SCREEN_ROUNDS+CONFIRM_ROUNDS||samples.some(s=>!s||!validLanes(s.lanes)||s.connections!==s.lanes.length||!['screen','confirm'].includes(s.stage)||!Number.isSafeInteger(s.bytes)||s.bytes<1||!Number.isFinite(s.elapsedMs)||s.elapsedMs<=0))return inconclusive('insufficient_comparable_samples');
  const screen=samples.filter(s=>s.stage==='screen'),confirm=samples.filter(s=>s.stage==='confirm'),finalists=screenTransferCandidates(screen);
  if(!finalists||confirm.length!==CONFIRM_ROUNDS||new Set(confirm.map(s=>s.bytes)).size!==1||confirm[0].bytes!==screen[0].bytes*CONFIRM_SCALE||finalists.some(lanes=>confirm.filter(s=>laneKey(s.lanes)===laneKey(lanes)).length!==2))return inconclusive('insufficient_comparable_samples');
  const evidence=[screen,confirm].map((rows,i)=>{
    const ranges=sampleRanges(rows),winner=clearWinner(ranges);
    return {stage:i?'confirm':'screen',bytes:rows[0].bytes,winner:winner?.connections??null,winningLanes:winner?.lanes??null,ranges};
  });
  const provisional=evidence[0],confirmed=evidence[1];
  let reason=!confirmed.winningLanes?'confirmation_ranges_overlap':null;
  let lateWinner=null,lateRanges=null;
  if(confirm.every(s=>Number.isFinite(lateRate(s))&&lateRate(s)>0)){
    lateRanges=sampleRanges(confirm,'lateRate');lateWinner=clearWinner(lateRanges,true);
  }
  if(!reason&&(!lateRanges||!lateWinner||laneKey(lateWinner.lanes)!==laneKey(confirmed.winningLanes)))reason=lateRanges?'late_delivery_does_not_confirm_winner':'late_delivery_unavailable';
  return {status:reason?'inconclusive':'measured',connections:reason?fallback:confirmed.winner,
    lanes:reason?fallbackLanes:[...confirmed.winningLanes],
    reason:reason??'confirmed_finalist_on_repeated_larger_payloads_and_late_delivery',
    provisionalConnections:provisional.winner,provisionalLanes:provisional.winningLanes,
    finalists:finalists.map(lanes=>[...lanes]),evidence,
    lateDelivery:{winner:lateWinner?.connections??null,lanes:lateWinner?.lanes??null,ranges:lateRanges}};
}

/** Repeated ABBA comparison on one exact lane set. The default stays in force
 * unless the smaller queue improves both total and late delivery by >=10%,
 * with non-overlapping repetition envelopes. No result is inferred from a peak
 * bufferedAmount, a warmup, missing progress or an interrupted comparison.
 */
export function selectQueuePolicy(samples) {
  const fallback=reason=>({status:'inconclusive',reason,highWaterMark:DEFAULT_QUEUE_BYTES,lowWaterMark:DEFAULT_QUEUE_BYTES/4});
  if(!Array.isArray(samples)||samples.length!==QUEUE_ROUNDS||samples.some(s=>!s||s.stage!=='queue'||!validLanes(s.lanes)||s.connections!==s.lanes.length||!Number.isSafeInteger(s.bytes)||s.bytes<1||!Number.isFinite(s.elapsedMs)||s.elapsedMs<=0||!Array.isArray(s.messageBytes)||s.messageBytes.length!==s.connections||s.messageBytes.some(n=>!Number.isSafeInteger(n)||n<1024||n>262144))||new Set(samples.map(s=>s.bytes)).size!==1||new Set(samples.map(s=>laneKey(s.lanes))).size!==1||new Set(samples.map(s=>JSON.stringify(s.messageBytes))).size!==1)return fallback('insufficient_queue_comparison');
  if(samples[0].queueHighBytes!==samples[3].queueHighBytes||samples[1].queueHighBytes!==samples[2].queueHighBytes||samples[0].queueHighBytes===samples[1].queueHighBytes)return fallback('invalid_queue_comparison_order');
  const groups=[TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES].map(high=>samples.filter(s=>s.queueHighBytes===high));
  if(groups.some(rows=>rows.length!==2)||samples.some(s=>lateRate(s)===null))return fallback('insufficient_queue_delivery_evidence');
  const evidence=groups.map(rows=>({highWaterMark:rows[0].queueHighBytes,elapsedMs:{min:Math.min(...rows.map(s=>s.elapsedMs)),max:Math.max(...rows.map(s=>s.elapsedMs))},lateBytesPerSecond:{min:Math.min(...rows.map(lateRate)),max:Math.max(...rows.map(lateRate))}}));
  // A comparison that never constrained the tight queue did not test pacing.
  if(groups[0].some(s=>!Array.isArray(s.queues)||s.queues.length!==s.connections||s.queues.some(q=>!Number.isSafeInteger(q.waitCount)||q.waitCount<1)))return {...fallback('tight_queue_not_exercised'),evidence};
  const [tight,normal]=evidence;
  if(tight.elapsedMs.max<=normal.elapsedMs.min*.9&&tight.lateBytesPerSecond.min>=normal.lateBytesPerSecond.max*1.1)return {status:'measured',reason:'smaller_queue_repeated_total_and_late_improvement',highWaterMark:TIGHT_QUEUE_BYTES,lowWaterMark:TIGHT_QUEUE_BYTES/2,evidence};
  return {...fallback('smaller_queue_not_confirmed'),evidence};
}

/** Same associations, bytes and queue boundaries; only message size changes. */
export function selectMessagePolicy(samples){
  const fallback=reason=>({status:'inconclusive',reason,maxPayloadBytes:null});
  if(!Array.isArray(samples)||samples.length!==4||samples.some(s=>!s||s.stage!=='message'||![16384,262144].includes(s.messageCeilingBytes)||!validLanes(s.lanes)||s.connections!==s.lanes.length||!Number.isSafeInteger(s.bytes)||s.bytes<1||!Number.isFinite(s.elapsedMs)||s.elapsedMs<=0||s.queueHighBytes!==DEFAULT_QUEUE_BYTES||s.queueLowBytes!==DEFAULT_QUEUE_BYTES/4||!Array.isArray(s.messageBytes)||s.messageBytes.length!==s.connections||s.messageBytes.some(n=>!Number.isSafeInteger(n)||n<1024||n>Math.min(262144,s.messageCeilingBytes)))||new Set(samples.map(s=>s.bytes)).size!==1||new Set(samples.map(s=>laneKey(s.lanes))).size!==1)return fallback('insufficient_message_comparison');
  if(samples[0].messageCeilingBytes!==samples[3].messageCeilingBytes||samples[1].messageCeilingBytes!==samples[2].messageCeilingBytes||samples[0].messageCeilingBytes===samples[1].messageCeilingBytes)return fallback('invalid_message_comparison_order');
  const groups=[16384,262144].map(size=>samples.filter(s=>s.messageCeilingBytes===size));
  if(groups.some(rows=>rows.length!==2||JSON.stringify(rows[0].messageBytes)!==JSON.stringify(rows[1].messageBytes))||samples.some(s=>lateRate(s)===null))return fallback('insufficient_message_delivery_evidence');
  if(!groups[1][0].messageBytes.some((n,i)=>n>groups[0][0].messageBytes[i]))return fallback('negotiated_limit_prevented_message_size_contrast');
  const evidence=groups.map(rows=>({maxPayloadBytes:rows[0].messageCeilingBytes,messageBytes:[...rows[0].messageBytes],elapsedMs:{min:Math.min(...rows.map(s=>s.elapsedMs)),max:Math.max(...rows.map(s=>s.elapsedMs))},lateBytesPerSecond:{min:Math.min(...rows.map(lateRate)),max:Math.max(...rows.map(lateRate))}}));
  const [small,normal]=evidence;
  if(small.elapsedMs.max<=normal.elapsedMs.min*.9&&small.lateBytesPerSecond.min>=normal.lateBytesPerSecond.max*1.1)return {status:'measured',reason:'smaller_messages_repeated_total_and_late_improvement',maxPayloadBytes:16384,evidence};
  return {...fallback('smaller_messages_not_confirmed'),evidence};
}

/** Native drain events, not polling. The byte cap bounds each live association;
 * cancellation/close settle the wait without adding a timeout or retry timer.
 */
function queueCapacity(dc,bytes,signal,high=QUEUE_BYTES,low=high/2) {
  if(signal.aborted||dc.readyState!=='open')return Promise.reject(Error('Probe stopped'));
  if(dc.bufferedAmount+bytes<=high)return Promise.resolve();
  return new Promise((resolve,reject)=>{
    const finish=error=>{dc.removeEventListener('bufferedamountlow',check);signal.removeEventListener('abort',aborted);error?reject(error):resolve();};
    const aborted=()=>finish(Error('Probe stopped'));
    const check=()=>{if(signal.aborted)return aborted();if(dc.readyState!=='open')return finish(Error('Probe channel closed'));if(dc.bufferedAmount<=low&&dc.bufferedAmount+bytes<=high)finish();};
    dc.bufferedAmountLowThreshold=low;
    dc.addEventListener('bufferedamountlow',check);signal.addEventListener('abort',aborted,{once:true});check();
  });
}

/** Bounded pre-transfer exploration on separate, real RTC associations.
 * No retries, sleeps, polling, or timers. One active peer per local session.
 * A silent peer can occupy that bounded slot until cancellation/disconnection;
 * it never blocks file selection. Payloads are counted and discarded on receipt.
 */
export function createTransferScout({self,allowed,send,runtime=globalThis,configuration,comparison='queue',prepareTransport=null,laneRateTradeoff=0.1,fileWorkloadLimits={},
  onReport=()=>{},busy=()=>false,sampleBytes=DEFAULT_BYTES,fallback=3,newId=()=>(runtime.crypto??globalThis.crypto).randomUUID()}) {
  if (!Number.isFinite(laneRateTradeoff)||laneRateTradeoff<0||laneRateTradeoff>0.1||![self,allowed,send,onReport,busy,newId].every(f => typeof f === 'function') ||
      !Number.isInteger(sampleBytes) || sampleBytes < 3 * 1024 || sampleBytes > MAX_BYTES || sampleBytes % 6 ||
      (![1,2,3].includes(fallback)||!['queue','message'].includes(comparison))||(prepareTransport!==null&&typeof prepareTransport!=='function')) throw TypeError('Invalid transfer scout configuration');
  let active=null,parked=null,closed=false,suspended=false;
  const demanded=new Set(),waiters=new Map(),retiredIds=new Set();
  const attempted=new Set(),results=new Map(),crypto=runtime.crypto??globalThis.crypto;
  const now=()=>runtime.performance?.now() ?? performance.now();
  const trusted=peer=>!closed&&typeof peer==='string'&&/^[0-9a-f]{16}$/.test(peer)&&peer!==self()&&allowed(peer);
  const permitted=peer=>!suspended&&trusted(peer);
  const live=s=>active===s&&!closed&&!s.stopped;
  const notify=(s,report)=>onReport({peer:s.peer,id:s.id,scope:'synthetic_transfer_connections',...report});
  const receiverEvidence=s=>({status:s.remoteEvidence.size===s.sentRounds.size&&s.sentRounds.size>0?'complete':s.remoteEvidence.size?'partial':'unavailable',
    expectedRounds:s.sentRounds.size,receivedRounds:s.remoteEvidence.size,observations:structuredClone([...s.remoteEvidence.values()].sort((a,b)=>a.round-b.round))});
  function wire(s,body){if(live(s))send(s.peer,{type:'transferScout',version:8,id:s.id,...body});}
  const workloadLimits={windowComparison:false,windowCandidates:[1,2],progressiveAdmission:false,queueComparison:false,queueBudgetScope:'association',maxConnections:3,batchSize:8,requestWindow:8,requestTimeoutMs:10000,sendTimeoutMs:15000,maxAttemptsPerPeer:2,maxChunkBytes:65536,...fileWorkloadLimits};
  requestWindowCandidates(workloadLimits.windowCandidates,workloadLimits.requestWindow);
  if(typeof workloadLimits.windowComparison!=='boolean'||typeof workloadLimits.progressiveAdmission!=='boolean'||!['association','peer'].includes(workloadLimits.queueBudgetScope)||typeof workloadLimits.queueComparison!=='boolean'||!Number.isInteger(workloadLimits.maxConnections)||workloadLimits.maxConnections<1||workloadLimits.maxConnections>3||!Number.isInteger(workloadLimits.batchSize)||workloadLimits.batchSize<1||workloadLimits.batchSize>64||!Number.isInteger(workloadLimits.requestWindow)||workloadLimits.requestWindow<1||workloadLimits.requestWindow>32||!Number.isFinite(workloadLimits.requestTimeoutMs)||workloadLimits.requestTimeoutMs<=0||!Number.isFinite(workloadLimits.sendTimeoutMs)||workloadLimits.sendTimeoutMs<=0||!Number.isInteger(workloadLimits.maxAttemptsPerPeer)||workloadLimits.maxAttemptsPerPeer<1||workloadLimits.maxAttemptsPerPeer>3||!Number.isInteger(workloadLimits.maxChunkBytes)||workloadLimits.maxChunkBytes<1024||workloadLimits.maxChunkBytes>262144)throw TypeError('Invalid scout file workload limits');
  function validWindowProfile(p){
    if(p.windowTrial===undefined)return p.requestWindow===undefined&&p.windowCandidates===undefined&&p.windowStep===undefined&&p.deliveryWindowCount===undefined;
    if(p.deliveryWindowCount!==Math.min(4,p.chunksPerLane)||!['measure','selected','repair'].includes(p.windowTrial)||!validLanes(p.lanes)||!Number.isInteger(p.chunksPerLane)||p.chunksPerLane<1||!Number.isInteger(p.windowStep)||p.windowStep<0||!Number.isInteger(p.requestWindow))return false;
    try{const candidates=requestWindowCandidates(p.windowCandidates,Math.min(workloadLimits.requestWindow,Math.floor(FILE_RECEIVE_SLOTS/p.lanes.length),p.chunksPerLane));return JSON.stringify(candidates)===JSON.stringify(p.windowCandidates)&&candidates.includes(p.requestWindow)&&p.windowStep<=candidates.length*2;}catch{return false;}
  }
  function validWorkload(p){return p&&validWindowProfile(p)&&(p.laneAdmission===undefined||p.laneAdmission===true&&workloadLimits.progressiveAdmission&&p.lanes?.length<workloadLimits.maxConnections)&&(!p.queueTrial?(!p.queueBudgetScope||p.queueBudgetScope==='association'):(workloadQueuePolicy(p)&&p.queuePolicy?.highWaterMark===workloadQueuePolicy(p).highWaterMark&&p.queuePolicy?.lowWaterMark===workloadQueuePolicy(p).lowWaterMark))&&validLanes(p.lanes)&&p.lanes.length<=workloadLimits.maxConnections&&Number.isInteger(p.chunkSize)&&p.chunkSize>=1024&&p.chunkSize<=workloadLimits.maxChunkBytes&&Number.isInteger(p.batchSize)&&p.batchSize===workloadLimits.batchSize&&Number.isInteger(p.chunksPerLane)&&p.chunksPerLane>=1&&p.chunksPerLane<=p.batchSize&&p.bytes===p.chunksPerLane*p.chunkSize*p.lanes.length&&p.bytes<=48*1024*1024&&[0,4*1024*1024].includes(p.sourceReadBlockBytes)&&Number.isInteger(p.queuePolicy?.highWaterMark)&&p.queuePolicy.highWaterMark>=p.chunkSize&&p.queuePolicy.highWaterMark<=16*1024*1024&&Number.isInteger(p.queuePolicy.lowWaterMark)&&p.queuePolicy.lowWaterMark>=0&&p.queuePolicy.lowWaterMark<p.queuePolicy.highWaterMark&&(p.messageBytes===null||[16384,262144].includes(p.messageBytes));}
  function workloadTransport(pool,profile,trial,phase){
    return Promise.all(profile.lanes.map(lane=>{
      const row=pool.lanes[lane],startedAtMs=now();
      return scoutConnection(row.pc,{channel:row.dc,runtime}).then(report=>{
        const key='workloadCounters'+trial;
        const observation={lane,trial,stage:phase,startedAtMs,endedAtMs:now(),clock:'local_monotonic',current:report.transportCounters,change:compareScoutTransport(row[key],report.transportCounters)};
        if(phase==='start')row[key]=report.transportCounters;else delete row[key];
        if(!closed)notify(pool,{phase:'preflight-file-workload-transport',...observation});return observation;
      }).catch(()=>{const observation={lane,trial,stage:phase,status:'unavailable'};if(!closed)notify(pool,{phase:'preflight-file-workload-transport',...observation});return observation;});
    }));
  }
  function sameWorkload(a,b){return JSON.stringify([a.lanes,a.chunkSize,a.bytes,a.batchSize,a.chunksPerLane,a.sourceReadBlockBytes,a.messageBytes,a.laneAdmission??false])===JSON.stringify([b.lanes,b.chunkSize,b.bytes,b.batchSize,b.chunksPerLane,b.sourceReadBlockBytes,b.messageBytes,b.laneAdmission??false]);}
  function cleanWorkload(pool){return pool.fileWorkload?.report?.status==='complete'&&pool.fileWorkload.driver.snapshot().every(row=>row.bufferedBytes===0);}
  function startWorkload(pool,profile,source){
    const controller=new AbortController(),started=performance.now(),arrivals=profile.lanes.map(lane=>({lane,bytes:0,messages:0,firstAtMs:null,lastAtMs:null,maxGapMs:0,verified:false})),submissions=profile.lanes.map(lane=>({lane,bytes:0,messages:0,coalescedRetries:0,peakBufferedBytes:0,bufferWaitMs:0}));
    const retries=[],deliveryWindows=[];let deliveredMessages=0,deliveredBytes=0;
    let settled=false,resolve;const completed=new Promise(r=>resolve=r);
    const state={profile,source,trial:(pool.workloadReports?.length??0)+1,settled:false,controller,completed,driver:null,stop(reason){finish('cancelled',reason);}};pool.fileWorkload=state;pool.workloadGuard=true;pool.qualifiedLanes=null;
    function finish(status,reason,remote=null){
      if(settled)return;settled=true;state.settled=true;
      const report={version:2,trial:state.trial,status,reason,profile,sourceLimits:source?{requestTimeoutMs:workloadLimits.requestTimeoutMs,sendTimeoutMs:workloadLimits.sendTimeoutMs}:null,sourceSubmission:source?submissions.map(row=>({...row,finalQueue:state.driver?.snapshot().find(q=>q.lane===row.lane)??null})):null,...(state.sourcePressure?{sourcePressure:state.sourcePressure}:{}),receiver:source?(remote?{...remote,provenance:'peer_reported'}:null):{clock:'receiver_monotonic',elapsedMs:performance.now()-started,lanes:arrivals,retries,...(profile.windowTrial?{deliveryWindows}:{}),queues:state.driver?.snapshot()??[],requestWindow:profile.requestWindow??workloadLimits.requestWindow,laneRequestWindow:Math.max(1,Math.min(profile.requestWindow??workloadLimits.requestWindow,Math.floor(FILE_RECEIVE_SLOTS/profile.lanes.length))),requestTimeoutMs:workloadLimits.requestTimeoutMs,maxAttemptsPerPeer:workloadLimits.maxAttemptsPerPeer},storage:'verified_memory_sink',coverage:'production_file_service_and_channel_sender',limitation:'bounded_byte_windows_do_not_establish_link_capacity_or_guarantee_long_file_performance'};
      state.report=structuredClone(report);pool.workloadReports=[...(pool.workloadReports??[]),state.report].slice(-9);pool.workloadGuard=true;pool.qualifiedLanes=status==='complete'?[...profile.lanes]:null;
      state.endTransport=workloadTransport(pool,profile,state.trial,'end');controller.abort();state.driver?.close();for(const row of pool.lanes)row.dc.onmessage=null;
      if(parked===pool&&!closed&&trusted(pool.peer)){
        if(!source)send(pool.peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadResult',trial:state.trial,status,reason,receiver:report.receiver});
        const result=results.get(pool.peer);if(source&&result){result.fileWorkload=report;result.fileWorkloads=structuredClone(pool.workloadReports);if(result.connectionOnly){result.reason=status==='complete'?'file_workload_observed_without_lane_comparison':'file_workload_'+status;result.connectionPlan=buildScoutConnectionPlan(result,{fallback,maxRateReduction:laneRateTradeoff});}notify(pool,{phase:'preflight-result',...structuredClone(result)});}
        notify(pool,{phase:'preflight-file-workload',...report});
      }
      resolve(report);
    }
    state.finish=finish;
    function fail(error){
      if(settled||state.sourceFailure)return;
      if(!source){finish('failed',error.message);return;}
      // Stop submission, but keep the trial alive until authenticated receiver
      // evidence arrives. A local send error cannot describe remote receipt.
      state.sourceFailure=String(error.message).slice(0,220);
      controller.abort();state.driver?.close();
      send(pool.peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadStop',trial:state.trial,reason:state.sourceFailure});
    }
    state.remoteVerified=new Set();
    state.checkPressure=async()=>{
      if(!source||settled||state.sourceFailure||!state.remoteVerified.size)return;
      // A verified-lane event provides a bounded observation point. Never poll
      // stats or mistake old totals, changed routes or missing counters for loss.
      const observations=await Promise.all(profile.lanes.filter(lane=>!state.remoteVerified.has(lane)).map(async lane=>{
        const row=pool.lanes[lane],before=row['workloadCounters'+state.trial];
        const report=await scoutConnection(row.pc,{channel:row.dc,runtime});
        return {lane,before,current:report.transportCounters,change:compareScoutTransport(before,report.transportCounters),bufferedBytes:row.dc.bufferedAmount};
      }));
      if(settled||state.sourceFailure||parked!==pool||closed)return;
      const pressured=observations.filter(row=>!state.remoteVerified.has(row.lane)&&row.bufferedBytes>0&&row.change.counters.packetsDiscardedOnSend.status==='observed'&&row.change.counters.packetsDiscardedOnSend.value>0);
      if(!pressured.length)return;
      state.sourcePressure={reason:'local_send_discards_after_verified_lane',verifiedLanes:[...state.remoteVerified].sort((a,b)=>a-b),observations:pressured};
      fail(Error('local_send_discards_after_verified_lane'));
    };
    const driver=createScoutFileWorkload({peer:pool.peer,lanes:profile.lanes,channels:pool.lanes.map(r=>r.dc),pcs:pool.lanes.map(r=>r.pc),bytes:profile.bytes,chunkSize:profile.chunkSize,batchSize:profile.batchSize,sourceReadBlockBytes:profile.sourceReadBlockBytes,requestWindow:profile.requestWindow??workloadLimits.requestWindow,requestTimeoutMs:workloadLimits.requestTimeoutMs,sendTimeoutMs:workloadLimits.sendTimeoutMs,maxAttemptsPerPeer:workloadLimits.maxAttemptsPerPeer,queuePolicy:profile.queuePolicy,messageBytes:profile.messageBytes,scopeId:'scout:'+pool.id+':'+state.trial,crypto,signal:controller.signal,
      onChunk(lane,bytes){const row=arrivals.find(r=>r.lane===lane),at=performance.now()-started;if(row.lastAtMs!==null)row.maxGapMs=Math.max(row.maxGapMs,at-row.lastAtMs);row.firstAtMs??=at;row.lastAtMs=at;row.bytes+=bytes.byteLength;row.messages++;
        if(profile.windowTrial){deliveredMessages++;deliveredBytes+=bytes.byteLength;const count=profile.deliveryWindowCount,total=profile.chunksPerLane*profile.lanes.length;if(deliveredMessages===Math.ceil(total*(deliveryWindows.length+1)/count))deliveryWindows.push({messages:deliveredMessages,bytes:deliveredBytes,elapsedMs:at});}
      },
      onSend(lane,sample){const row=submissions.find(r=>r.lane===lane);if(sample.coalesced){row.coalescedRetries++;return;}row.bytes+=sample.bytes;row.messages++;row.peakBufferedBytes=Math.max(row.peakBufferedBytes,sample.bufferedBytes);row.bufferWaitMs+=sample.bufferWaitMs;},
      onLaneComplete(lane){arrivals.find(row=>row.lane===lane).verified=true;if(!settled&&!source)send(pool.peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadLaneVerified',trial:state.trial,lane});},
      onRetry(lane,payload){
        if(settled)return;
        const verifiedLanes=arrivals.filter(row=>row.verified).map(row=>row.lane);
        retries.push({lane,index:payload.index,attempt:payload.attempt,atMs:performance.now()-started,verifiedLanes});
        if(verifiedLanes.length)finish('failed','file_workload_retry_after_verified_lane');
      },
      onComplete(){finish('complete','bounded_file_workload_received_and_verified');},onError:fail
    });state.driver=driver;
    for(const lane of profile.lanes)pool.lanes[lane].dc.onmessage=e=>{try{driver.receive(lane,e.data);}catch(error){fail(error);}};
    state.startTransport=workloadTransport(pool,profile,state.trial,'start');notify(pool,{phase:'preflight-file-workload',status:'started',trial:state.trial,profile,source});
    return state;
  }
  async function qualifyRequestWindows(pool,result,{chunkSize,fileBytes,sourceReadBlockBytes}){
    if(!Number.isInteger(chunkSize)||chunkSize<1024||chunkSize>workloadLimits.maxChunkBytes)return null;
    let lanes=[...(result.connectionPlan?.lanes??result.lanes)],last=null,step=0,stage='measure',chosen=null;
    const chunksPerLane=Math.min(workloadLimits.batchSize,Math.floor(fileBytes/(chunkSize*lanes.length)));
    if(chunksPerLane<1)return {version:2,status:'skipped',reason:'file_smaller_than_one_agreed_chunk_per_lane'};
    let candidates=requestWindowCandidates(workloadLimits.windowCandidates,Math.min(workloadLimits.requestWindow,Math.floor(FILE_RECEIVE_SLOTS/lanes.length),chunksPerLane)),order=requestWindowOrder(candidates);
    const reports=[];
    for(let attempt=0;attempt<9;attempt++){
      const profile={lanes:[...lanes],chunkSize,sourceReadBlockBytes,batchSize:workloadLimits.batchSize,chunksPerLane,bytes:chunksPerLane*chunkSize*lanes.length,queuePolicy:{highWaterMark:1048576,lowWaterMark:524288},messageBytes:null,deliveryWindowCount:Math.min(4,chunksPerLane),requestWindow:stage==='measure'?order[step]:chosen,windowCandidates:[...candidates],windowTrial:stage,windowStep:stage==='measure'?step:stage==='selected'?order.length:0};
      if(!validWorkload(profile))return null;
      const state=startWorkload(pool,profile,true);
      send(pool.peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkload',trial:state.trial,profile});
      const report=await state.completed;report.sourceTransport=await state.endTransport;last=report;
      if(parked!==pool||closed)return report;
      pool.workloadReports[pool.workloadReports.length-1]=structuredClone(report);
      notify(pool,{phase:'preflight-request-window',...structuredClone(report)});
      const decision=decideFileWorkload(report);
      if(decision.status==='complete'&&cleanWorkload(pool)&&Array.isArray(report.receiver?.queues)&&report.receiver.queues.length===lanes.length&&report.receiver.queues.every((q,i)=>q.lane===lanes[i]&&q.bufferedBytes===0)){
        if(stage==='measure'){
          reports.push(report);
          if(++step<order.length)continue;
          result.fileWorkloadWindowComparison=compareRequestWindows(reports,candidates,{maxRateReduction:laneRateTradeoff});chosen=result.fileWorkloadWindowComparison.requestWindow;
          if(chosen!==profile.requestWindow){stage='selected';continue;}
        }
        result.fileWorkload=structuredClone(report);result.fileWorkloads=structuredClone(pool.workloadReports);
        result.fileWorkloadSelection={version:1,status:'verified',trial:state.trial,lanes:[...lanes],rejected:pool.lanes.flatMap((row,lane)=>row.rejected?[lane]:[]),reason:stage==='repair'?'verified_reduced_window_workload':'verified_request_window_workload',requestWindow:profile.requestWindow,queuePolicy:{...profile.queuePolicy}};
        result.status='workload_verified';result.reason=result.fileWorkloadSelection.reason;result.connections=lanes.length;result.lanes=[...lanes];
        result.connectionPlan=buildScoutConnectionPlan(result,{fallback,maxRateReduction:laneRateTradeoff});
        notify(pool,{phase:'preflight-result',...structuredClone(result)});return report;
      }
      result.fileWorkloadWindowComparison={status:'incomplete',reason:'window_trial_failed',failedTrial:report.trial};
      if(decision.status==='retry_subset'&&await restrictWorkload(pool,decision.lanes)){
        if(parked!==pool||closed)return report;lanes=decision.lanes;stage='repair';
        candidates=requestWindowCandidates(workloadLimits.windowCandidates,Math.min(workloadLimits.requestWindow,Math.floor(FILE_RECEIVE_SLOTS/lanes.length),chunksPerLane));chosen=candidates[0];continue;
      }
      if(report.status==='failed'||report.status==='complete'){
        result.fileWorkloadSelection={version:1,status:'blocked',reason:decision.status==='complete'?'window_queues_not_clean':decision.reason,lanes:[],rejected:[...lanes]};result.status='blocked';result.reason=result.fileWorkloadSelection.reason;result.connections=0;result.lanes=[];
        result.connectionPlan=buildScoutConnectionPlan(result,{fallback,maxRateReduction:laneRateTradeoff});notify(pool,{phase:'preflight-result',...structuredClone(result)});releasePool('failed_window_workload');
      }
      return report;
    }
    releasePool('window_workload_attempt_budget');return last;
  }
  function markRejected(pool,lanes){
    for(const [lane,row]of pool.lanes.entries())if(!lanes.includes(lane)){
      row.rejected=true;row.pc.onconnectionstatechange=null;row.pc.onicecandidate=null;row.pc.ondatachannel=null;row.dc.onclose=null;row.dc.onerror=null;row.dc.onmessage=null;
    }
  }
  function closeRejected(pool){
    for(const [lane,row]of pool.lanes.entries())if(row.rejected&&!row.rejectionClosed){
      row.rejectionClosed=true;releasePrepared(pool,row,lane,'workload_lane_rejected');
      if(row.pending){row.pending.onmessage=null;row.pending.close();}row.dc.close();row.pc.close();
      notify(pool,{phase:'preflight-workload-rejected',lane,trial:pool.fileWorkload?.trial,reason:'failed_file_workload'});
    }
  }
  function restrictWorkload(pool,lanes){
    if(pool.restriction||!validLanes(lanes))return Promise.resolve(false);
    let resolve;const completed=new Promise(r=>resolve=r),trial=pool.fileWorkload.trial;
    pool.restriction={trial,lanes:[...lanes],stage:'proposed',resolve,cancel(){resolve(false);}};
    try{send(pool.peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadRestrict',trial,lanes});}catch{pool.restriction.cancel();pool.restriction=null;}
    return completed;
  }
  function releasePrepared(s,row,lane,reason){
    const prepared=row.preparedTransport;row.preparedTransport=null;if(!prepared?.release)return;
    try{void Promise.resolve(prepared.release(reason)).then(observation=>{if(observation)notify(s,{phase:'preflight-native-socket',lane,status:'released',observation});}).catch(()=>notify(s,{phase:'preflight-native-socket',lane,status:'unavailable',reason:'native_release_failed'}));}catch{notify(s,{phase:'preflight-native-socket',lane,status:'unavailable',reason:'native_release_failed'});}
  }
  function releasePool(reason='pool_released',notifyPeer=true) {
    const pool=parked;if(!pool)return;pool.restriction?.cancel?.();pool.fileWorkload?.stop(reason);parked=null;
    rememberRetired(pool.id);demanded.delete(pool.peer);
    if(notifyPeer&&trusted(pool.peer))send(pool.peer,{type:'transferScout',version:8,id:pool.id,action:'release',reason});
    for(const [lane,row] of pool.lanes.entries())if(row&&!row.claimed){
      releasePrepared(pool,row,lane,reason);
      row.pc.ondatachannel=null;row.pc.onconnectionstatechange=null;row.pc.onicecandidate=null;
      if(row.pending){row.pending.onmessage=null;row.pending.close();}
      row.dc.onclose=null;row.dc.close();row.pc.close();
    }
    notify(pool,{phase:'preflight-pool',status:'released',reason});
  }
  function settleSend(peer){const resolve=waiters.get(peer);if(resolve){waiters.delete(peer);resolve(results.get(peer)?structuredClone(results.get(peer)):null);}}
  function rememberRetired(id){retiredIds.add(id);if(retiredIds.size>128)retiredIds.delete(retiredIds.values().next().value);}
  function retire(s,reason,notifyPeer=true) {
    if(!live(s))return;
    if(s.finishing&&['channel_closed','connection_closed','connection_failed'].includes(reason))reason='complete';
    if(notifyPeer)wire(s,{action:'cancel',reason});
    const partial=partialSample(s);
    s.stopped=true;active=null;s.controller.abort();
    s.ack?.reject(Error(reason));s.ack=null;s.planAck?.reject(Error(reason));s.planAck=null;s.endAck?.reject(Error(reason));s.endAck=null;
    if(reason==='complete'){
      releasePool();parked=s;
      for(const row of s.lanes){
        row.dc.onopen=null;row.dc.onclose=null;row.dc.onerror=null;row.dc.onmessage=null;
        row.pc.onicecandidate=null;
        row.pc.onconnectionstatechange=()=>{if(['failed','closed','disconnected'].includes(row.pc.connectionState))releasePool('connection_'+row.pc.connectionState);};
        row.pc.ondatachannel=e=>{
          if(row.claimed||row.pending||e.channel.label!=='phantom-distribution'){e.channel.close();return;}
          row.pending=e.channel;
        };
      }
      notify(s,{phase:'preflight',status:'complete',incomingSamples:s.incomingSamples,retainedAssociations:LANES});
      settleSend(s.peer);return;
    }
    rememberRetired(s.id);demanded.delete(s.peer);
    for(const [index,lane] of s.lanes.entries()){
      releasePrepared(s,lane,index,reason);
      lane.pc.onicecandidate=null;lane.pc.onconnectionstatechange=null;lane.pc.ondatachannel=null;
      if(lane.dc){lane.dc.onopen=null;lane.dc.onclose=null;lane.dc.onerror=null;lane.dc.onmessage=null;try{lane.dc.close();}catch{}}
      try{lane.pc.close();}catch{}
    }
    if(reason!=='complete') {results.delete(s.peer);notify(s,{phase:'preflight',status:'cancelled',reason,samples:s.samples,queueSamples:s.queueSamples,messageSamples:s.messageSamples,incomingSamples:s.incomingSamples,partial});}
    settleSend(s.peer);
  }
  // Receiver times describe local arrival only; they never subtract clocks
  // from the sender. Keep summaries bounded even when a round is interrupted.
  function incomingSample(receiving,complete=false) {
    return {round:receiving.round,...receiving.spec,complete,
      elapsedMs:now()-receiving.startedAt,clock:'receiver_monotonic',
      lanes:[...receiving.arrivals.values()].map(row=>({...row}))};
  }
  function partialSample(s) {
    const outgoing=s.outgoing?{...s.outgoing,elapsedMs:now()-s.outgoing.startedAt,
      delivery:s.ack?.delivery??s.outgoing.delivery,
      bufferedNow:s.lanes.map(row=>row.dc?.bufferedAmount??null)}:null;
    return outgoing||s.receiving?structuredClone({outgoing,
      incoming:s.receiving?incomingSample(s.receiving):null}):null;
  }
  function fail(s,error){retire(s,error?.message==='Invalid probe data'?'invalid_probe_data':'probe_failed');}
  function snapshot(s,phase,round) {
    // Serialize observations per association so slow getStats calls cannot
    // reverse counter history. Observation work never gates receipt or ACKs.
    for(const [lane,row] of s.lanes.entries())row.observationTask=(row.observationTask??Promise.resolve()).then(async()=>{
      if(closed||s.stopped)return;
      const startedAtMs=now(),report=await scoutConnection(row.pc,{channel:row.dc,runtime});
      const transport={lane,phase,round,startedAtMs,endedAtMs:now(),clock:'local_monotonic',
        current:report.transportCounters,change:compareScoutTransport(row.previousCounters,report.transportCounters)};
      row.previousCounters=report.transportCounters;s.transportObservations[lane]=transport;
      if(!closed&&!s.stopped)notify(s,{phase,lane,round,report,transport});
    }).catch(()=>{});
  }
  function create(peer,id,initiator,bytes,reverse,oneWay=false,comparisonKind=comparison,connectionOnly=false) {
    const s={peer,id,initiator,bytes,reverse,oneWay,connectionOnly,comparison:comparisonKind,lanes:[],ready:new Set(),started:false,stopped:false,
      receiving:null,nextReceive:initiator?ROUNDS:0,controller:new AbortController(),samples:[],queueSamples:[],messageSamples:[],incomingSamples:[],transportObservations:[],sentRounds:new Map(),remoteEvidence:new Map(),outgoing:null,ack:null,running:false,receivedRounds:0,payload:new Uint8Array(connectionOnly?0:262144)};
    active=s;attempted.add(peer);results.delete(peer);
    for(let offset=0;offset<s.payload.length;offset+=65536)crypto.getRandomValues(s.payload.subarray(offset,offset+65536));
    for(let lane=0;lane<LANES;lane++) {
      const pc=new runtime.RTCPeerConnection(configuration??defaultRTCConfiguration()),row={pc,dc:null,ice:[],signals:Promise.resolve(),signalCount:0};s.lanes.push(row);
      pc.onicecandidate=e=>{if(e.candidate)wire(s,{action:'signal',lane,candidate:e.candidate.toJSON?.()??e.candidate});};
      pc.onconnectionstatechange=()=>{if(['failed','closed','disconnected'].includes(pc.connectionState))retire(s,'connection_'+pc.connectionState);};
      pc.ondatachannel=e=>bind(s,lane,e.channel);
    }
    notify(s,{phase:'preflight',status:'started',sampleBytes:bytes,candidates:[1,2,3],laneCandidates:CANDIDATES.map(lanes=>[...lanes]),roundsPerDirection:connectionOnly?0:24,laneRoundsPerDirection:connectionOnly?0:20,confirmationBytes:connectionOnly?0:CONFIRM_SCALE*bytes,connectionOnly,
      comparison:s.comparison,intent:oneWay?'send':'explore',direction:initiator?'outgoing':'incoming',payloadBudgetBytes:connectionOnly?0:(oneWay?1:2)*(1+SCREEN_ROUNDS+CONFIRM_ROUNDS*CONFIRM_SCALE+QUEUE_ROUNDS*QUEUE_SAMPLE_SCALE)*bytes,comparisonPlan:{kind:s.comparison,rounds:QUEUE_ROUNDS,bytesPerRound:QUEUE_SAMPLE_SCALE*bytes,...(s.comparison==='queue'?{highWaterMarks:[TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES]}:{messageCeilings:[16384,262144],highWaterMark:DEFAULT_QUEUE_BYTES})},order:order(s),messageCeilingBytes:65536,queueHighBytes:QUEUE_BYTES});
    return s;
  }
  const order=s=>{
    const first=s.reverse?[...CANDIDATES].reverse():CANDIDATES;
    return [...first,...[...first].reverse()].map(lanes=>[...lanes]);
  };
  function specification(s,round,finalists=s.outgoingFinalists,queueLanes=s.outgoingQueueLanes) {
    const phase=round%ROUNDS;
    if(phase>=LANE_ROUNDS){
      if(!validLanes(queueLanes))throw Error('Invalid probe data');
      if(s.comparison==='message'){
        const sequence=s.reverse?[16384,262144,262144,16384]:[262144,16384,16384,262144],bytes=s.bytes*QUEUE_SAMPLE_SCALE;
        return {stage:'message',connections:queueLanes.length,lanes:[...queueLanes],bytes,perLane:bytes/queueLanes.length,queueHighBytes:DEFAULT_QUEUE_BYTES,queueLowBytes:DEFAULT_QUEUE_BYTES/4,messageCeilingBytes:sequence[phase-LANE_ROUNDS]};
      }
      const sequence=s.reverse?[TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES,TIGHT_QUEUE_BYTES]:[DEFAULT_QUEUE_BYTES,TIGHT_QUEUE_BYTES,TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES];
      const high=sequence[phase-LANE_ROUNDS],bytes=s.bytes*QUEUE_SAMPLE_SCALE;
      return {stage:'queue',connections:queueLanes.length,lanes:[...queueLanes],bytes,perLane:bytes/queueLanes.length,queueHighBytes:high,queueLowBytes:high===DEFAULT_QUEUE_BYTES?high/4:high/2};
    }
    const stage=phase===0?'warmup':phase<=SCREEN_ROUNDS?'screen':'confirm';
    const lanes=phase===0?[0,1,2]:stage==='screen'?order(s)[phase-1]:finalists?.find(lanes=>lanes.length===FINAL_ORDER[phase-SCREEN_ROUNDS-1]);
    if(!validLanes(lanes))throw Error('Invalid probe data');
    const bytes=s.bytes*(stage==='confirm'?CONFIRM_SCALE:1);
    return {stage,connections:lanes.length,lanes:[...lanes],bytes,perLane:bytes/lanes.length};
  }
  function maybeStart(s) {
    if(!live(s)||!s.initiator||s.started||s.ready.size!==LANES||s.lanes.some(r=>r.dc?.readyState!=='open'))return;
    s.started=true;snapshot(s,'preflight-connected',null);
    if(s.connectionOnly){
      const result={version:8,id:s.id,scope:'synthetic_transfer_connections',direction:'outgoing',connectionOnly:true,status:'inconclusive',reason:'file_workload_pending',connections:fallback,lanes:Array.from({length:fallback},(_,i)=>i),samples:[],queueSamples:[],messageSamples:[],completedSamples:0,unrunSamples:0,directionPayloadBytes:0,payloadBudgetBytes:0,profileComparison:{status:'replaced_by_file_workload',avoidedBytes:0},receiverEvidence:receiverEvidence(s),localTransportPreparation:s.lanes.map((row,lane)=>({lane,report:row.preparedTransport?.report??null}))};
      result.connectionPlan=buildScoutConnectionPlan(result,{fallback,maxRateReduction:laneRateTradeoff});s.running=false;s.sentDirection=true;results.set(s.peer,result);notify(s,{phase:'preflight-result',...structuredClone(result)});rowSend(s,0,{action:'finished'});return;
    }
    void run(s,0).catch(e=>fail(s,e));
  }
  function bind(s,lane,dc) {
    if(!live(s)||s.lanes[lane].dc||dc.label!=='phantom-distribution-scout'||dc.ordered===false||dc.maxRetransmits!=null||dc.maxPacketLifeTime!=null){try{dc.close();}catch{}return;}
    const row=s.lanes[lane];row.dc=dc;dc.binaryType='arraybuffer';
    const opened=()=>{
      if(!live(s)||row.opening)return;row.opening=true;
      const announce=()=>{if(live(s)){wire(s,{action:'ready',lane});maybeStart(s);}};
      if(!prepareTransport){announce();return;}
      void (async()=>{
        let prepared;
        try{prepared=await prepareTransport({peer:s.peer,id:s.id,lane,pc:row.pc,signal:s.controller.signal});}
        catch{if(live(s))notify(s,{phase:'preflight-native-socket',lane,status:'unavailable',reason:'native_preparation_failed'});}
        if(prepared){row.preparedTransport=prepared;if(!live(s)){releasePrepared(s,row,lane,'scout_cancelled');return;}if(prepared.report)notify(s,{phase:'preflight-native-socket',lane,status:'prepared',observation:prepared.report});}
        announce();
      })().catch(e=>fail(s,e));
    };
    dc.onopen=opened;dc.onclose=()=>retire(s,'channel_closed');dc.onerror=()=>retire(s,'channel_error');
    dc.onmessage=e=>{try{receiveData(s,lane,e.data);}catch(error){fail(s,error);}};
    if(dc.readyState==='open')opened();
  }
  function receiveData(s,lane,data) {
    if(!live(s))return;
    if(typeof data==='string') {
      if(data.length>512)throw Error('Invalid probe data');
      const body=JSON.parse(data);
      if(body.action==='sample') {
        if(s.initiator?(s.oneWay||!s.sentDirection):s.started)throw Error('Invalid probe data');
        if(body.round!==s.nextReceive||!Number.isInteger(body.round)||body.round<0||body.round>=2*ROUNDS)throw Error('Invalid probe data');
        const spec=specification(s,body.round,s.incomingFinalists,s.incomingQueueLanes);
        if(!spec.lanes.includes(lane)||body.bytes!==spec.perLane)throw Error('Invalid probe data');
        s.receiving??={round:body.round,counts:new Map(),arrivals:new Map(),startedAt:now(),spec,nextCheckpoint:spec.bytes/4};
        if(s.receiving.counts.has(lane))throw Error('Invalid probe data');
        s.receiving.counts.set(lane,0);s.receiving.arrivals.set(lane,{lane,bytes:0,messages:0,firstAtMs:null,lastAtMs:null,maxGapMs:0,largestGaps:[],completeAtMs:null});return;
      }
      if(lane!==0&&!(['progress','ack'].includes(body.action)&&s.ack?.controlLane===lane))throw Error('Invalid probe data');
      if(body.action==='endDirection'){
        const base=s.initiator?ROUNDS:0,end=base+ROUNDS;
        if((s.initiator&&(s.oneWay||!s.sentDirection))||(!s.initiator&&s.started)||s.receiving||s.receiveEnded||s.nextReceive!==base+LANE_ROUNDS||body.nextRound!==end)throw Error('Invalid probe data');
        s.nextReceive=end;s.receiveEnded=true;rowSend(s,0,{action:'endDirectionAck'});return;
      }
      if(body.action==='endDirectionAck'&&s.endAck){const ack=s.endAck;s.endAck=null;ack.resolve();return;}
      if(body.action==='queuePlan'){
        const expected=(s.initiator?ROUNDS:0)+LANE_ROUNDS;
        if((s.initiator&&(s.oneWay||!s.sentDirection))||(!s.initiator&&s.started)||s.receiving||s.nextReceive!==expected||s.incomingQueueLanes||!validLanes(body.lanes))throw Error('Invalid probe data');
        s.incomingQueueLanes=[...body.lanes];rowSend(s,0,{action:'queuePlanAck'});return;
      }
      if(body.action==='queuePlanAck'&&s.planAck){const ack=s.planAck;s.planAck=null;ack.resolve();return;}
      if(body.action==='finalists'){
        const expected=s.initiator?ROUNDS+SCREEN_ROUNDS+1:SCREEN_ROUNDS+1;
        if((s.initiator&&(s.oneWay||!s.sentDirection))||(!s.initiator&&s.started)||s.nextReceive!==expected||s.incomingFinalists||!Array.isArray(body.lanes)||body.lanes.length!==3||body.lanes.some((lanes,i)=>!validLanes(lanes)||lanes.length!==i+1))throw Error('Invalid probe data');
        s.incomingFinalists=body.lanes.map(lanes=>[...lanes]);rowSend(s,0,{action:'finalistsAck'});return;
      }
      if(body.action==='finalistsAck'&&s.planAck){const ack=s.planAck;s.planAck=null;ack.resolve();return;}
      if(body.action==='progress'&&s.ack?.round===body.round&&s.ack.controlLane===lane){const ack=s.ack;if(!Number.isSafeInteger(body.bytes)||body.bytes<=ack.delivery.at(-1).bytes||body.bytes>=ack.bytes||ack.delivery.length>=4)throw Error('Invalid probe data');ack.delivery.push({bytes:body.bytes,elapsedMs:now()-ack.startedAt});return;}
      if(body.action==='ack'&&s.ack?.round===body.round&&s.ack.controlLane===lane){const ack=s.ack;s.ack=null;ack.resolve();return;}
      if(body.action==='turn'&&!s.oneWay&&!s.initiator&&s.nextReceive===ROUNDS&&!s.started){s.started=true;void run(s,ROUNDS).catch(e=>fail(s,e));return;}
      if(body.action==='finished'&&s.connectionOnly&&!s.initiator&&s.nextReceive===0)s.nextReceive=ROUNDS;
      if(body.action==='finished'&&(s.oneWay?!s.initiator&&s.nextReceive===ROUNDS:s.initiator&&s.nextReceive===2*ROUNDS)){s.finishing=true;rowSend(s,0,{action:'finishAck'});return;}
      if(body.action==='finishAck'&&(s.oneWay?s.initiator:!s.initiator)&&s.running===false&&s.sentDirection){wire(s,{action:'complete'});retire(s,'complete',false);return;}
      throw Error('Invalid probe data');
    }
    const receiving=s.receiving;
    if(!(data instanceof ArrayBuffer)||data.byteLength<1||data.byteLength>(receiving?.spec.messageCeilingBytes??(receiving?.spec.stage==='queue'?262144:65536))||!receiving?.counts.has(lane))throw Error('Invalid probe data');
    const count=receiving.counts.get(lane)+data.byteLength;
    if(count>receiving.spec.perLane)throw Error('Invalid probe data');
    receiving.counts.set(lane,count);
    const arrival=receiving.arrivals.get(lane),atMs=now()-receiving.startedAt;
    if(arrival.lastAtMs!==null){
      const gap={fromMs:arrival.lastAtMs,toMs:atMs,gapMs:atMs-arrival.lastAtMs,afterBytes:arrival.bytes};
      arrival.maxGapMs=Math.max(arrival.maxGapMs,gap.gapMs);
      arrival.largestGaps.push(gap);arrival.largestGaps.sort((a,b)=>b.gapMs-a.gapMs);arrival.largestGaps.length=Math.min(4,arrival.largestGaps.length);
    }
    arrival.firstAtMs??=atMs;arrival.lastAtMs=atMs;arrival.messages++;arrival.bytes=count;
    if(count===receiving.spec.perLane)arrival.completeAtMs=atMs;
    const total=[...receiving.counts.values()].reduce((sum,n)=>sum+n,0);
    if(total<receiving.spec.bytes&&total>=receiving.nextCheckpoint){
      rowSend(s,receiving.spec.lanes[0],{action:'progress',round:receiving.round,bytes:total});
      receiving.nextCheckpoint=(Math.floor(total/(receiving.spec.bytes/4))+1)*receiving.spec.bytes/4;
    }
    if(receiving.counts.size===receiving.spec.connections&&[...receiving.counts.values()].every(n=>n===receiving.spec.perLane)) {
      const observed=incomingSample(receiving,true);s.incomingSamples.push(observed);notify(s,{phase:'preflight-receive-sample',direction:'incoming',...observed});
      try{wire(s,{action:'receiverEvidence',observation:makeReceiverEvidence(observed,s.transportObservations)});}
      catch{notify(s,{phase:'preflight-receiver-evidence-unavailable',round:receiving.round,reason:'control_send_failed'});}
      s.nextReceive++;s.receivedRounds++;s.receiving=null;rowSend(s,receiving.spec.lanes[0],{action:'ack',round:receiving.round});
      snapshot(s,'preflight-receive-stats',receiving.round);
    }
  }
  function rowSend(s,lane,body){if(!live(s)||s.lanes[lane].dc?.readyState!=='open')throw Error('Probe channel unavailable');s.lanes[lane].dc.send(JSON.stringify(body));}
  async function run(s,base) {
    s.running=true;
    for(let round=base;round<base+ROUNDS;round++) {
      if(!live(s)||busy())throw Error('Probe superseded');
      if(round%ROUNDS===SCREEN_ROUNDS+1){
        s.outgoingFinalists=screenTransferCandidates(s.samples);if(!s.outgoingFinalists)throw Error('Invalid screening data');
        const agreed=new Promise((resolve,reject)=>{s.planAck={resolve,reject};});void agreed.catch(()=>{});
        rowSend(s,0,{action:'finalists',lanes:s.outgoingFinalists});await agreed;if(!live(s))return;
      }
      if(round%ROUNDS===LANE_ROUNDS){
        const selected=selectTransferSetup(s.samples,fallback);
        const plan=buildScoutConnectionPlan({...selected,samples:s.samples,receiverEvidence:receiverEvidence(s)},{fallback,maxRateReduction:laneRateTradeoff});
        if(!['measured','bounded_tradeoff'].includes(plan.laneDecision.status)){
          s.profileComparison={status:'skipped',reason:'lane_choice_unresolved',decisionStatus:plan.laneDecision.status,decisionReason:plan.laneDecision.reason,avoidedBytes:QUEUE_ROUNDS*QUEUE_SAMPLE_SCALE*s.bytes};
          const ended=new Promise((resolve,reject)=>{s.endAck={resolve,reject};});void ended.catch(()=>{});rowSend(s,0,{action:'endDirection',nextRound:base+ROUNDS});await ended;if(!live(s))return;break;
        }
        s.outgoingQueueLanes=[...plan.lanes];
        notify(s,{phase:s.comparison==='queue'?'preflight-queue-comparison':'preflight-message-comparison',lanes:s.outgoingQueueLanes,rounds:QUEUE_ROUNDS,bytesPerRound:s.bytes*QUEUE_SAMPLE_SCALE,
          ...(s.comparison==='queue'?{highWaterMarks:[TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES],queueOrder:s.reverse?[TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES,TIGHT_QUEUE_BYTES]:[DEFAULT_QUEUE_BYTES,TIGHT_QUEUE_BYTES,TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES]}:{highWaterMark:DEFAULT_QUEUE_BYTES,lowWaterMark:DEFAULT_QUEUE_BYTES/4,messageOrder:s.reverse?[16384,262144,262144,16384]:[262144,16384,16384,262144]})});
        const agreed=new Promise((resolve,reject)=>{s.planAck={resolve,reject};});void agreed.catch(()=>{});
        rowSend(s,0,{action:'queuePlan',lanes:s.outgoingQueueLanes});await agreed;if(!live(s))return;
      }
      const spec=specification(s,round),{stage,connections,lanes,perLane,bytes}=spec;
      s.sentRounds.set(round,spec);
      const queueHigh=spec.queueHighBytes??QUEUE_BYTES,queueLow=spec.queueLowBytes??queueHigh/2;
      const delivery=[{bytes:0,elapsedMs:0}];
      const ack=new Promise((resolve,reject)=>{s.ack={round,resolve,reject,bytes,delivery,controlLane:lanes[0],startedAt:null};});
      void ack.catch(()=>{}); // Cancellation may happen while send() itself throws.
      // Larger confirmations are streamed through bounded queues, not dumped
      // into the native SCTP send buffer as one oversized burst.
      const payloads=lanes.map(lane=>{
        const row=s.lanes[lane];
        const limit=row.pc.sctp?.maxMessageSize;
        const ceiling=spec.messageCeilingBytes??(stage==='queue'?262144:65536);
        const size=Number.isFinite(limit)&&limit>0?Math.min(ceiling,Math.floor(limit)):(limit===0||limit===Infinity?ceiling:Math.min(65536,ceiling));
        if(size<1024)throw Error('Probe message limit too small');
        return s.payload.buffer.slice(0,size);
      });
      const messageBytes=payloads.map(p=>p.byteLength);
      const before=s.lanes.map(r=>r.dc.bufferedAmount),startedAt=now();s.ack.startedAt=startedAt;
      const peak=s.lanes.map(r=>r.dc.bufferedAmount);
      const queues=lanes.map(lane=>({lane,payloadBytes:0,waitCount:0,waitMs:0,maxWaitMs:0,waitingSinceMs:null,enqueuedAtMs:null}));
      s.outgoing={round,stage,connections,lanes,bytes,startedAt,delivery,queues,queueHighBytes:queueHigh,queueLowBytes:queueLow,peakBufferedBytes:peak};
      await Promise.all(payloads.map(async(payload,position)=>{
        const lane=lanes[position];
        const size=payload.byteLength;rowSend(s,lane,{action:'sample',round,bytes:perLane});
        for(let offset=0;offset<perLane;offset+=size){
          const part=offset+size<=perLane?payload:payload.slice(0,perLane-offset);
          const queue=queues[position];
          if(s.lanes[lane].dc.bufferedAmount+part.byteLength>queueHigh){
            const waitingAt=now();queue.waitCount++;queue.waitingSinceMs=waitingAt-startedAt;
            try{await queueCapacity(s.lanes[lane].dc,part.byteLength,s.controller.signal,queueHigh,queueLow);}
            finally{const waited=now()-waitingAt;queue.waitMs+=waited;queue.maxWaitMs=Math.max(queue.maxWaitMs,waited);queue.waitingSinceMs=null;}
          }
          if(!live(s))throw Error('Probe stopped');
          s.lanes[lane].dc.send(part);queue.payloadBytes+=part.byteLength;peak[lane]=Math.max(peak[lane],s.lanes[lane].dc.bufferedAmount);
        }
        queues[position].enqueuedAtMs=now()-startedAt;
      }));
      const queued=s.lanes.map(r=>r.dc.bufferedAmount);
      await ack;
      if(!live(s))return;
      const elapsedMs=now()-startedAt;
      delivery.push({bytes,elapsedMs});
      const sample={round,stage,...(spec.messageCeilingBytes?{messageCeilingBytes:spec.messageCeilingBytes}:{}),connections,lanes,bytes,elapsedMs,delivery,queues,clock:'sender_monotonic',queueHighBytes:queueHigh,queueLowBytes:queueLow,peakBufferedBytes:peak,messageBytes,bufferedBefore:before,bufferedAfterEnqueue:queued,
        bufferedAfterAck:s.lanes.map(r=>r.dc.bufferedAmount),bytesPerSecond:elapsedMs>0?bytes*1000/elapsedMs:null,warmup:stage==='warmup'};
      s.outgoing=null;
      if(stage==='queue')s.queueSamples.push(sample);else if(stage==='message')s.messageSamples.push(sample);else if(!sample.warmup)s.samples.push(sample);
      notify(s,{phase:'preflight-sample',direction:'outgoing',...sample});snapshot(s,'preflight-sample-stats',round);

    }
    s.running=false;
    const selection=selectTransferSetup(s.samples,fallback);
    const result={version:8,id:s.id,scope:'synthetic_transfer_connections',direction:'outgoing',...selection,queuePolicy:selectQueuePolicy(s.queueSamples),queueSamples:structuredClone(s.queueSamples),queueComparisonPayloadBytes:s.queueSamples.reduce((total,sample)=>total+sample.bytes,0),
      comparison:s.comparison,messagePolicy:selectMessagePolicy(s.messageSamples),messageSamples:structuredClone(s.messageSamples),messageComparisonPayloadBytes:s.messageSamples.reduce((total,sample)=>total+sample.bytes,0),profileComparison:s.profileComparison??{status:'completed',reason:'qualified_lane_profile',avoidedBytes:0},stoppedEarly:false,completedSamples:s.samples.length,unrunSamples:20-s.samples.length,
      directionPayloadBytes:s.bytes+[...s.samples,...s.queueSamples,...s.messageSamples].reduce((total,sample)=>total+sample.bytes,0),
      directionPayloadAvoidedBytes:(1+SCREEN_ROUNDS+CONFIRM_ROUNDS*CONFIRM_SCALE+QUEUE_ROUNDS*QUEUE_SAMPLE_SCALE)*s.bytes-(s.bytes+[...s.samples,...s.queueSamples,...s.messageSamples].reduce((total,sample)=>total+sample.bytes,0)),
      sampleBytes:s.bytes,confirmationBytes:s.bytes*CONFIRM_SCALE,payloadBudgetBytes:(s.oneWay?1:2)*(1+SCREEN_ROUNDS+CONFIRM_ROUNDS*CONFIRM_SCALE+QUEUE_ROUNDS*QUEUE_SAMPLE_SCALE)*s.bytes,queueHighBytes:QUEUE_BYTES,samples:structuredClone(s.samples),validFor:'current_peer_connection_generation',
      transportObservations:structuredClone(s.transportObservations),
      receiverEvidence:receiverEvidence(s),localTransportPreparation:s.lanes.map((row,lane)=>({lane,report:row.preparedTransport?.report??null})),
      limitations:['bounded_samples_include_ack_latency','per_association_probe_queue_cap','file_handoff_requires_retained_associations','does_not_measure_disk_or_hashing','does_not_establish_link_capacity','browser_stats_do_not_expose_kernel_receive_drops_or_socket_buffer_limits']};
    result.connectionPlan=buildScoutConnectionPlan(result,{fallback,maxRateReduction:laneRateTradeoff});
    s.sentDirection=true;results.set(s.peer,result);notify(s,{phase:'preflight-result',...result});
    if(base===0&&!s.oneWay)rowSend(s,0,{action:'turn'});else rowSend(s,0,{action:'finished'});
  }
  async function signal(s,lane,body) {
    if(!live(s))return;
    const row=s.lanes[lane];
    if(body.description) {
      const d=body.description;
      if(!['offer','answer'].includes(d.type)||typeof d.sdp!=='string'||d.sdp.length>24000||d.type!==(s.initiator?'answer':'offer')||row.pc.remoteDescription)throw Error('Invalid probe signal');
      await row.pc.setRemoteDescription(d);if(!live(s))return;
      for(const candidate of row.ice.splice(0))await row.pc.addIceCandidate(candidate);
      if(!s.initiator){await row.pc.setLocalDescription(await row.pc.createAnswer());wire(s,{action:'signal',lane,description:{type:'answer',sdp:row.pc.localDescription.sdp}});}
    } else if(body.candidate) {
      const c=body.candidate;
      if(typeof c.candidate!=='string'||c.candidate.length>2048||row.ice.length>=32)throw Error('Invalid probe candidate');
      if(row.pc.remoteDescription)await row.pc.addIceCandidate(c);else row.ice.push(c);
    }
  }
  return {
    start(peer,oneWay=false,connectionOnly=false) {
      if(!permitted(peer)||(!oneWay&&self()>peer)||busy()||active||parked||(!oneWay&&attempted.has(peer))||attempted.size>=128)return false;
      const id=newId();if(!validId(id))throw Error('Invalid probe id');
      let s;
      try {
        s=create(peer,id,true,sampleBytes,parseInt(id.replaceAll('-','').slice(-1),16)%2===1,oneWay,comparison,connectionOnly);
        wire(s,{action:'init',comparison:s.comparison,sampleBytes:s.bytes,reverse:s.reverse,oneWay,connectionOnly});
        for(const [lane,row]of s.lanes.entries()) {
          bind(s,lane,row.pc.createDataChannel('phantom-distribution-scout',{ordered:true}));
          void (async()=>{await row.pc.setLocalDescription(await row.pc.createOffer());wire(s,{action:'signal',lane,description:{type:'offer',sdp:row.pc.localDescription.sdp}});})().catch(e=>fail(s,e));
        }
        return true;
      }catch(e){if(active)retire(active,'setup_failed');return false;}
    },
    receive(peer,body) {
      if(!trusted(peer)||body?.type!=='transferScout'||body.version!==8||!validId(body.id))return false;
      if(['fileWorkloadRestrict','fileWorkloadRestrictReady','fileWorkloadRestrictCommit','fileWorkloadRestricted'].includes(body.action)){
        const pool=parked,w=pool?.fileWorkload;if(!pool||pool.peer!==peer||pool.id!==body.id||pool.asset||!w?.settled||w.report?.status!=='failed'||body.trial!==w.trial)return false;
        if(body.action==='fileWorkloadRestrict'){
          if(w.source||pool.restriction||!validLanes(body.lanes)||body.lanes.length>=w.profile.lanes.length||body.lanes.some(lane=>!w.profile.lanes.includes(lane)||!w.report.receiver?.lanes.find(r=>r.lane===lane&&r.verified&&r.bytes===w.profile.bytes/w.profile.lanes.length)||w.report.receiver.queues?.find(q=>q.lane===lane)?.bufferedBytes!==0))return false;
          pool.restriction={trial:w.trial,lanes:[...body.lanes],stage:'prepared'};markRejected(pool,body.lanes);
          send(peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadRestrictReady',trial:w.trial});return true;
        }
        const pending=pool.restriction;if(!pending||pending.trial!==body.trial)return false;
        if(body.action==='fileWorkloadRestrictReady'&&w.source&&pending.stage==='proposed'){
          pending.stage='committed';markRejected(pool,pending.lanes);closeRejected(pool);
          send(peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadRestrictCommit',trial:w.trial});return true;
        }
        if(body.action==='fileWorkloadRestrictCommit'&&!w.source&&pending.stage==='prepared'){
          closeRejected(pool);pool.activeWorkloadLanes=[...pending.lanes];pool.restriction=null;
          send(peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadRestricted',trial:w.trial});return true;
        }
        if(body.action==='fileWorkloadRestricted'&&w.source&&pending.stage==='committed'){
          pool.activeWorkloadLanes=[...pending.lanes];pool.restriction=null;pending.resolve(true);return true;
        }
        return false;
      }
      if(['fileWorkload','fileWorkloadReady','fileWorkloadResult','fileWorkloadStop','fileWorkloadLaneVerified'].includes(body.action)){
        const pool=parked;if(!pool||pool.peer!==peer||pool.id!==body.id||pool.asset)return false;
        if(body.action==='fileWorkload'){
          const previous=pool.fileWorkload,order=['tight-1','baseline-1','baseline-2','tight-2','baseline-selected'];
          const nextComparison=!previous?.profile?.windowTrial&&previous?.settled&&cleanWorkload(pool)&&sameWorkload(previous.profile,body.profile??{})&&order.indexOf(body.profile?.queueTrial)===order.indexOf(previous.profile.queueTrial)+1&&order.indexOf(previous.profile.queueTrial)>=0;
          // Expansion follows verified receipt and empty receiver queues.
          // Keep the per-lane workload and queue-policy rule fixed.
          const next=body.profile??{},prior=previous?.profile;
          const nextAdmission=workloadLimits.progressiveAdmission&&validLanes(next.lanes)&&previous?.settled&&prior.laneAdmission===true&&cleanWorkload(pool)&&next.lanes?.length===prior.lanes.length+1&&prior.lanes.every((lane,i)=>next.lanes[i]===lane)&&!pool.lanes[next.lanes.at(-1)]?.rejected&&(prior.queueTrial||next.queuePolicy?.highWaterMark===prior.queuePolicy.highWaterMark&&next.queuePolicy?.lowWaterMark===prior.queuePolicy.lowWaterMark)&&JSON.stringify([next.chunkSize,next.chunksPerLane,next.batchSize,next.sourceReadBlockBytes,next.messageBytes,next.queueTrial,next.queueBudgetScope])===JSON.stringify([prior.chunkSize,prior.chunksPerLane,prior.batchSize,prior.sourceReadBlockBytes,prior.messageBytes,prior.queueTrial,prior.queueBudgetScope]);
          const windowOrder=next.windowTrial&&validWindowProfile(next)?requestWindowOrder(next.windowCandidates):[];
          const nextWindow=previous?.settled&&prior.windowTrial==='measure'&&cleanWorkload(pool)&&sameWorkload(prior,next)&&JSON.stringify(prior.windowCandidates)===JSON.stringify(next.windowCandidates)&&((next.windowTrial==='measure'&&next.windowStep===prior.windowStep+1&&next.requestWindow===windowOrder[next.windowStep])||(next.windowTrial==='selected'&&prior.windowStep===windowOrder.length-1&&next.windowStep===windowOrder.length));
          const reduced=previous?.settled&&previous.report?.status==='failed'&&pool.activeWorkloadLanes&&JSON.stringify(pool.activeWorkloadLanes)===JSON.stringify(body.profile?.lanes)&&body.profile.lanes.length<previous.profile.lanes.length;
          if(!previous&&body.profile?.windowTrial&&(body.profile.windowTrial!=='measure'||body.profile.windowStep!==0||body.profile.requestWindow!==windowOrder[0]))return false;
          if(body.trial!==(pool.workloadReports?.length??0)+1||body.trial>9||(!previous&&body.profile?.laneAdmission&&body.profile.lanes?.length!==1)||(previous&&!nextComparison&&!nextAdmission&&!nextWindow&&!reduced))return false;if(!validWorkload(body.profile)){send(peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadResult',trial:body.trial,status:'failed',reason:'file_workload_profile_not_supported',receiver:null});return true;}
          try{startWorkload(pool,body.profile,false);send(peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadReady',trial:body.trial});}catch{pool.fileWorkload?.stop('workload_setup_failed');}return true;
        }
        const w=pool.fileWorkload;
        if(body.action==='fileWorkloadStop'){
          if(!w||w.source||body.trial!==w.trial||typeof body.reason!=='string'||body.reason.length>256)return false;
          if(w.settled){const r=w.report;send(peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkloadResult',trial:w.trial,status:r.status,reason:r.reason,receiver:r.receiver});}
          else w.finish('failed','sender_failed:'+body.reason);
          return true;
        }
        if(!w?.source||w.settled||body.trial!==w.trial)return false;
        if(body.action==='fileWorkloadLaneVerified'){
          if(!w.profile.lanes.includes(body.lane)||w.remoteVerified.has(body.lane))return false;
          w.remoteVerified.add(body.lane);void w.checkPressure().catch(()=>{});return true;
        }
        if(body.action==='fileWorkloadReady'){
          if(w.started)return false;w.started=true;void Promise.resolve(w.profile.windowTrial?w.startTransport:null).then(()=>w.driver.prepare()).then(()=>w.driver.start()).catch(e=>w.finish('failed',e.message));return true;
        }
        if(!['complete','failed','cancelled'].includes(body.status)||typeof body.reason!=='string'||body.reason.length>256)return false;
        const r=body.receiver;
        if(body.status==='complete'&&(w.profile.windowTrial&&!readRequestWindowDelivery(w.profile,r)||!r||r.clock!=='receiver_monotonic'||!Number.isInteger(r.requestWindow)||r.requestWindow<1||r.requestWindow>32||(w.profile.windowTrial&&(r.requestWindow!==w.profile.requestWindow||r.laneRequestWindow!==w.profile.requestWindow))||!Number.isFinite(r.elapsedMs)||r.elapsedMs<=0||!Array.isArray(r.lanes)||r.lanes.length!==w.profile.lanes.length||r.lanes.some((l,i)=>l.lane!==w.profile.lanes[i]||l.bytes!==w.profile.bytes/w.profile.lanes.length||l.verified!==true||!Number.isInteger(l.messages)||l.messages<1||!Number.isFinite(l.maxGapMs)||l.maxGapMs<0||l.maxGapMs>r.elapsedMs)))return false;
        w.finish(w.sourceFailure?'failed':body.status,w.sourceFailure??body.reason,r);return true;
      }
      if(body.action==='release'){
        const reason=typeof body.reason==='string'&&body.reason.length<=128?'peer_released:'+body.reason:'peer_released';
        if(parked?.peer===peer&&parked.id===body.id){releasePool(reason,false);return true;}
        // Native close may beat the authenticated control message. Keep both observations.
        if(retiredIds.has(body.id)){notify({peer,id:body.id},{phase:'preflight-pool',status:'peer_release_observed',reason});return true;}
        return false;
      }
      if(body.action==='receiverEvidence'){
        const s=active?.peer===peer&&active.id===body.id?active:parked?.peer===peer&&parked.id===body.id?parked:null;
        const round=body.observation?.round;
        if(!s||!Number.isInteger(round)||!s.sentRounds.has(round)||s.remoteEvidence.has(round))return false;
        const observation=readReceiverEvidence(body.observation,s.sentRounds.get(round));if(!observation)return false;
        s.remoteEvidence.set(round,observation);
        const status=s.remoteEvidence.size===s.sentRounds.size?'complete':'partial',result=results.get(peer);if(result?.id===s.id){result.receiverEvidence=receiverEvidence(s);result.connectionPlan=buildScoutConnectionPlan(result,{fallback,maxRateReduction:laneRateTradeoff});}
        notify(s,{phase:'preflight-receiver-evidence',direction:'incoming_report',observation,status,expectedRounds:s.sentRounds.size,receivedRounds:s.remoteEvidence.size});return true;
      }
      if(body.action==='init') {
        if(active?.peer===peer&&active.id===body.id)return true;
        const oneWay=body.oneWay===true;
        const invalid=(body.connectionOnly!==undefined&&typeof body.connectionOnly!=='boolean')||(body.connectionOnly===true&&!oneWay)||!['queue','message'].includes(body.comparison)||typeof body.oneWay!=='boolean'||(!oneWay&&self()<peer)||!Number.isInteger(body.sampleBytes)||body.sampleBytes<3072||body.sampleBytes>sampleBytes||body.sampleBytes%6||typeof body.reverse!=='boolean';
        if(retiredIds.has(body.id))return false;
        if(!invalid&&oneWay&&!demanded.has(peer)&&active?.peer===peer&&!active.oneWay)retire(active,'sender_intent',false);
        if(!invalid&&oneWay&&!active&&!busy()&&parked?.peer===peer&&parked.asset)releasePool('next_file');
        if(invalid||(suspended&&!oneWay)||active||parked||busy()||(oneWay?demanded.has(peer):attempted.has(peer))||attempted.size>=128){send(peer,{type:'transferScout',version:8,id:body.id,action:'cancel',reason:invalid?'profile_not_supported':'busy'});return false;}
        try{if(oneWay)demanded.add(peer);create(peer,body.id,false,body.sampleBytes,body.reverse,oneWay,body.comparison,body.connectionOnly===true);}catch{if(active)retire(active,'setup_failed');}return true;
      }
      const s=active;if(!s||s.peer!==peer||s.id!==body.id)return false;
      if(body.action==='cancel'){retire(s,['busy','profile_not_supported'].includes(body.reason)?'peer_'+body.reason:'peer_cancelled',false);return true;}
      if(body.action==='complete'&&(s.oneWay?!s.initiator&&s.nextReceive===ROUNDS:s.initiator&&s.nextReceive===2*ROUNDS)){retire(s,'complete',false);return true;}
      if(!Number.isInteger(body.lane)||body.lane<0||body.lane>=LANES)return false;
      if(body.action==='ready'){s.ready.add(body.lane);maybeStart(s);return true;}
      if(body.action==='signal'){const row=s.lanes[body.lane];if(++row.signalCount>64){retire(s,'signal_budget_exceeded');return false;}row.signals=row.signals.then(()=>signal(s,body.lane,body)).catch(e=>fail(s,e));return true;}
      return false;
    },
    async forSend(peer,{connectionOnly=false}={}){
      if(!trusted(peer)||busy())return null;
      // An explicit file selection may scout again; background starts stay suspended.
      suspended=false;
      if(parked?.peer===peer&&!parked.asset&&results.has(peer))return structuredClone(results.get(peer));
      if(waiters.has(peer))return null;
      // A file declares its direction. Replace incomplete background exploration
      // with one bounded outgoing pass; never probe the opposite direction first.
      if(active)retire(active,'sender_intent');
      releasePool('next_file');results.delete(peer);demanded.add(peer);
      const completed=new Promise(resolve=>waiters.set(peer,resolve));
      if(!this.start(peer,true,connectionOnly))settleSend(peer);
      return completed;
    },
    async qualifyFileWorkload(peer,{chunkSize,fileBytes,sourceReadBlockBytes=0}={}){
      const pool=parked,result=results.get(peer);if(!pool||pool.peer!==peer||pool.asset||!result||pool.fileWorkload||!Number.isSafeInteger(fileBytes)||fileBytes<1)return null;
      if(workloadLimits.windowComparison)return qualifyRequestWindows(pool,result,{chunkSize,fileBytes,sourceReadBlockBytes});
      const plan=result.connectionPlan,initialLaneBytes=Math.min(workloadLimits.batchSize,Math.floor(fileBytes/(chunkSize*result.lanes.length)))*chunkSize,study=workloadLimits.queueComparison&&initialLaneBytes>1048576,order=['tight-1','baseline-1','baseline-2','tight-2','baseline-selected'];let lanes=[...(plan?.lanes??result.lanes)],last=null,repaired=false,queueStep=0;const comparisonReports=[];
      const admissionLanes=[...lanes],progressive=workloadLimits.progressiveAdmission&&lanes.length>1&&initialLaneBytes>=chunkSize;
      if(progressive)lanes=admissionLanes.slice(0,1);
      if(workloadLimits.queueComparison&&!study)result.fileWorkloadQueueComparison={status:'skipped',reason:'payload_cannot_exercise_queue_boundary',bytesPerLane:initialLaneBytes};
      for(let attempt=0;attempt<(study?9:5);attempt++){
        const chunksPerLane=progressive&&!repaired?initialLaneBytes/chunkSize:Math.min(workloadLimits.batchSize,Math.floor(fileBytes/(chunkSize*lanes.length)));
        if(chunksPerLane<1){const report={version:2,status:'skipped',reason:'file_smaller_than_one_agreed_chunk_per_lane',chunkSize,fileBytes};result.fileWorkload=report;result.reason=report.reason;result.connectionPlan=buildScoutConnectionPlan(result,{fallback,maxRateReduction:laneRateTradeoff});notify(pool,{phase:'preflight-result',...structuredClone(result)});notify(pool,{phase:'preflight-file-workload',...report});return report;}
        const profile={lanes,chunkSize,sourceReadBlockBytes,batchSize:workloadLimits.batchSize,chunksPerLane,bytes:chunksPerLane*chunkSize*lanes.length,queuePolicy:{highWaterMark:repaired?16*1024*1024:plan?.queuePolicy?.highWaterMark??16*1024*1024,lowWaterMark:repaired?4*1024*1024:plan?.queuePolicy?.lowWaterMark??4*1024*1024},messageBytes:repaired?null:plan?.messagePolicy?.maxPayloadBytes??null};
        if(study){
          profile.messageBytes=null;profile.queueTrial=repaired?'reverify':order[queueStep];profile.queueBudgetScope=workloadLimits.queueBudgetScope;profile.queuePolicy=workloadQueuePolicy(profile);
        }
        if(progressive&&!repaired&&lanes.length<admissionLanes.length)profile.laneAdmission=true;
        if(!validWorkload(profile))return null;
        const state=startWorkload(pool,profile,true);
        send(peer,{type:'transferScout',version:8,id:pool.id,action:'fileWorkload',trial:state.trial,profile});
        const report=await state.completed;last=report;
        if(parked!==pool||closed)return report;
        const decision=decideFileWorkload(report);
        if(decision.status==='complete'){
          if(profile.laneAdmission&&cleanWorkload(pool)){
            notify(pool,{phase:'preflight-lane-admission',trial:state.trial,status:'verified',lanes:[...lanes],nextLane:admissionLanes[lanes.length],reason:'verified_workload_and_empty_queues'});
            lanes=admissionLanes.slice(0,lanes.length+1);continue;
          }
          if(study&&!repaired&&!profile.laneAdmission&&queueStep<4){
            comparisonReports.push(report);
            if(queueStep<3&&cleanWorkload(pool)){queueStep++;continue;}
            result.fileWorkloadQueueComparison=compareWorkloadQueues(comparisonReports,{maxRateReduction:laneRateTradeoff});
            if(queueStep===3&&result.fileWorkloadQueueComparison.selected==='baseline'&&cleanWorkload(pool)){queueStep++;continue;}
          }
          result.fileWorkloadSelection={version:1,status:'verified',trial:state.trial,lanes:[...lanes],rejected:pool.lanes.flatMap((row,lane)=>row.rejected?[lane]:[]),reason:repaired?'verified_reduced_file_workload':'verified_file_workload',...(study?{queuePolicy:{...profile.queuePolicy}}:{})};
          result.status='workload_verified';result.reason=result.fileWorkloadSelection.reason;result.connections=lanes.length;result.lanes=[...lanes];if(repaired){result.queuePolicy={status:'fallback',highWaterMark:16*1024*1024,lowWaterMark:4*1024*1024,reason:'baseline_policy_verified_on_reduced_workload'};result.messagePolicy={status:'fallback',maxPayloadBytes:null,reason:'baseline_policy_verified_on_reduced_workload'};}
          if(study)result.queuePolicy={status:'workload_verified',...profile.queuePolicy,reason:'queue_policy_verified_on_selected_file_associations'};
          result.connectionPlan=buildScoutConnectionPlan(result,{fallback,maxRateReduction:laneRateTradeoff});notify(pool,{phase:'preflight-result',...structuredClone(result)});return report;
        }
        if(study)result.fileWorkloadQueueComparison={status:'incomplete',reason:'queue_trial_failed',failedTrial:report.trial};
        if(decision.status==='retry_subset'){
          if(await restrictWorkload(pool,decision.lanes)){if(parked!==pool||closed)return report;lanes=decision.lanes;repaired=true;continue;}
          releasePool('workload_restriction_cancelled');return {...report,status:'cancelled',reason:'workload_restriction_cancelled'};
        }
        if(report.status==='failed'){
          result.fileWorkloadSelection={version:1,status:decision.status==='blocked'?'blocked':'unavailable',reason:decision.reason,lanes:[],rejected:[...lanes]};if(decision.status==='blocked'){result.status='blocked';result.reason=decision.reason;result.connections=0;result.lanes=[];result.connectionPlan=buildScoutConnectionPlan(result,{fallback,maxRateReduction:laneRateTradeoff});}notify(pool,{phase:'preflight-result',...structuredClone(result)});releasePool('failed_file_workload');
        }
        return report;
      }
      releasePool('file_workload_attempt_budget');return last;
    },
    take(peer,id,lane,asset,jobId,preferredLanes=[0,1,2],expectedWindow=undefined){
      const pool=parked;if(!pool||pool.peer!==peer||(id&&pool.id!==id)||!trusted(peer)||(pool.asset&&pool.asset!==asset))return null;
      if(!validLanes(preferredLanes))return null;
      const index=lane===undefined?preferredLanes.find(index=>!pool.lanes[index].claimed&&!pool.lanes[index].rejected&&(!pool.workloadGuard||pool.qualifiedLanes?.includes(index))):lane;
      if(!Number.isInteger(index)||index<0||index>=LANES)return null;
      const measuredWindow=pool.fileWorkload?.profile?.requestWindow??null;if(expectedWindow!==undefined&&expectedWindow!==measuredWindow)return null;
      const row=pool.lanes[index];if(row.rejected||(pool.workloadGuard&&!pool.qualifiedLanes?.includes(index))||row.claimed||row.pc.connectionState!=='connected')return null;
      pool.asset=asset;row.claimed=true;row.pc.onicecandidate=null;row.pc.onconnectionstatechange=null;row.pc.ondatachannel=null;
      row.dc.onclose=null;row.dc.onmessage=null;row.dc.close();
      notify(pool,{phase:'preflight-handoff',lane:index,status:'claimed',jobId});
      return {pc:row.pc,channel:row.pending??null,id:pool.id,lane:index,requestWindow:measuredWindow,releaseTransport:reason=>releasePrepared(pool,row,index,reason)};
    },
    get(peer){const result=results.get(peer);return result?{...structuredClone(result),transportAvailable:parked?.peer===peer&&!parked.asset}:null;},
    suspend(reason='file_selected'){if(parked?.restriction||parked?.fileWorkload&&!parked.fileWorkload.settled)releasePool('workload_cancelled:'+reason);parked?.fileWorkload?.stop(reason);suspended=true;if(active)retire(active,reason);},
    drop(peer){if(parked?.peer===peer)releasePool('peer_disconnected',false);demanded.delete(peer);if(active?.peer===peer)retire(active,'peer_disconnected',false);results.delete(peer);attempted.delete(peer);},
    clear(){releasePool('generation_changed');demanded.clear();if(active)retire(active,'generation_changed',false);results.clear();attempted.clear();suspended=false;},
    close(){releasePool('session_closed');demanded.clear();if(active)retire(active,'session_closed');closed=true;results.clear();attempted.clear();},
  };
}
