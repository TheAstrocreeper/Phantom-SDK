import {defaultRTCConfiguration} from './rtc-configuration.js';
import {scoutConnection,compareScoutTransport} from './connection-scout.js';
import {makeReceiverEvidence,readReceiverEvidence} from './scout-receiver-evidence.js';

export const TRANSFER_SCOUT_PROTOCOL = 'phantom-transfer-scout-v6';
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

/** Compare exact lane sets. Ambiguous screening, reversed larger results or
 * missing late-delivery confirmation retain the explicit fallback profile.
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
  let reason=!provisional.winningLanes?'screen_ranges_overlap':!confirmed.winningLanes?'confirmation_ranges_overlap':laneKey(provisional.winningLanes)!==laneKey(confirmed.winningLanes)?'winner_changed_with_sample_size':null;
  let lateWinner=null,lateRanges=null;
  if(confirm.every(s=>Number.isFinite(lateRate(s))&&lateRate(s)>0)){
    lateRanges=sampleRanges(confirm,'lateRate');lateWinner=clearWinner(lateRanges,true);
  }
  if(!reason&&(!lateRanges||!lateWinner||laneKey(lateWinner.lanes)!==laneKey(confirmed.winningLanes)))reason=lateRanges?'late_delivery_does_not_confirm_winner':'late_delivery_unavailable';
  return {status:reason?'inconclusive':'measured',connections:reason?fallback:confirmed.winner,
    lanes:reason?fallbackLanes:[...confirmed.winningLanes],
    reason:reason??'confirmed_lane_set_across_payload_sizes_and_late_delivery',
    provisionalConnections:provisional.winner,provisionalLanes:provisional.winningLanes,
    finalists:finalists.map(lanes=>[...lanes]),evidence,
    lateDelivery:{winner:lateWinner?.connections??null,lanes:lateWinner?.lanes??null,ranges:lateRanges}};
}

/** Observed timing envelopes can only widen as repetitions arrive. If even
 * the current envelopes cannot satisfy the existing selection rule, another
 * repetition cannot repair them. Unseen candidates remain possible.
 */
export function scoutStopDecision(samples,fallback=3) {
  if(!Array.isArray(samples)||!samples.length||samples.some(s=>!s||!validLanes(s.lanes)||s.connections!==s.lanes.length||!['screen','confirm'].includes(s.stage)||!Number.isFinite(s.elapsedMs)||s.elapsedMs<=0))return null;
  const screen=samples.filter(s=>s.stage==='screen'),confirm=samples.filter(s=>s.stage==='confirm');
  const ranges=sampleRanges(screen);
  const possible=CANDIDATES.filter(lanes=>{
    const own=ranges.find(r=>laneKey(r.lanes)===laneKey(lanes));
    return !own||ranges.every(other=>other===own||own.max<other.min);
  });
  const result=reason=>({reason,possibleLanes:possible.map(lanes=>[...lanes]),
    evidence:[{stage:'screen',ranges},...(confirm.length?[{stage:'confirm',ranges:sampleRanges(confirm)}]:[])]});
  if(!possible.length)return result('screen_ranges_overlap_irrecoverable');
  const fallbackLanes=Array.from({length:fallback},(_,i)=>i);
  if(possible.length===1&&laneKey(possible[0])===laneKey(fallbackLanes))return result('only_fallback_can_still_qualify');
  if(!confirm.length)return null;
  const winner=clearWinner(ranges);if(!winner)return result('screen_ranges_overlap_irrecoverable');
  const confirmation=sampleRanges(confirm),own=confirmation.find(r=>laneKey(r.lanes)===laneKey(winner.lanes));
  if(own&&confirmation.some(other=>other!==own&&own.max>=other.min))return result('confirmation_cannot_confirm_screen_winner');
  if(confirm.some(s=>lateRate(s)===null))return result('late_delivery_unavailable');
  const tails=sampleRanges(confirm,'lateRate'),tail=tails.find(r=>laneKey(r.lanes)===laneKey(winner.lanes));
  if(tail&&tails.some(other=>other!==tail&&tail.min<=other.max))return result('late_delivery_cannot_confirm_screen_winner');
  return null;
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
export function createTransferScout({self,allowed,send,runtime=globalThis,configuration,
  onReport=()=>{},busy=()=>false,sampleBytes=DEFAULT_BYTES,fallback=3,newId=()=>(runtime.crypto??globalThis.crypto).randomUUID()}) {
  if (![self,allowed,send,onReport,busy,newId].every(f => typeof f === 'function') ||
      !Number.isInteger(sampleBytes) || sampleBytes < 3 * 1024 || sampleBytes > MAX_BYTES || sampleBytes % 6 ||
      ![1,2,3].includes(fallback)) throw TypeError('Invalid transfer scout configuration');
  let active=null,parked=null,closed=false,suspended=false;
  const demanded=new Set(),waiters=new Map();
  const attempted=new Set(),results=new Map(),crypto=runtime.crypto??globalThis.crypto;
  const now=()=>runtime.performance?.now() ?? performance.now();
  const trusted=peer=>!closed&&typeof peer==='string'&&/^[0-9a-f]{16}$/.test(peer)&&peer!==self()&&allowed(peer);
  const permitted=peer=>!suspended&&trusted(peer);
  const live=s=>active===s&&!closed&&!s.stopped;
  const notify=(s,report)=>onReport({peer:s.peer,id:s.id,scope:'synthetic_transfer_connections',...report});
  const receiverEvidence=s=>({status:s.remoteEvidence.size===s.sentRounds.size&&s.sentRounds.size>0?'complete':s.remoteEvidence.size?'partial':'unavailable',
    expectedRounds:s.sentRounds.size,receivedRounds:s.remoteEvidence.size,observations:structuredClone([...s.remoteEvidence.values()].sort((a,b)=>a.round-b.round))});
  function wire(s,body){if(live(s))send(s.peer,{type:'transferScout',version:6,id:s.id,...body});}
  function releasePool(reason='pool_released',notifyPeer=true) {
    const pool=parked;if(!pool)return;parked=null;
    if(notifyPeer&&trusted(pool.peer))send(pool.peer,{type:'transferScout',version:6,id:pool.id,action:'release'});
    for(const row of pool.lanes)if(row&&!row.claimed){
      row.pc.ondatachannel=null;row.pc.onconnectionstatechange=null;row.pc.onicecandidate=null;
      if(row.pending){row.pending.onmessage=null;row.pending.close();}
      row.dc.onclose=null;row.dc.close();row.pc.close();
    }
    notify(pool,{phase:'preflight-pool',status:'released',reason});
  }
  function settleSend(peer){const resolve=waiters.get(peer);if(resolve){waiters.delete(peer);resolve(results.get(peer)?structuredClone(results.get(peer)):null);}}
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
    for(const lane of s.lanes){
      lane.pc.onicecandidate=null;lane.pc.onconnectionstatechange=null;lane.pc.ondatachannel=null;
      if(lane.dc){lane.dc.onopen=null;lane.dc.onclose=null;lane.dc.onerror=null;lane.dc.onmessage=null;try{lane.dc.close();}catch{}}
      try{lane.pc.close();}catch{}
    }
    if(reason!=='complete') {results.delete(s.peer);notify(s,{phase:'preflight',status:'cancelled',reason,samples:s.samples,queueSamples:s.queueSamples,incomingSamples:s.incomingSamples,partial});}
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
  function create(peer,id,initiator,bytes,reverse,oneWay=false) {
    const s={peer,id,initiator,bytes,reverse,oneWay,lanes:[],ready:new Set(),started:false,stopped:false,
      receiving:null,nextReceive:initiator?ROUNDS:0,controller:new AbortController(),samples:[],queueSamples:[],incomingSamples:[],transportObservations:[],sentRounds:new Map(),remoteEvidence:new Map(),outgoing:null,ack:null,running:false,receivedRounds:0,payload:new Uint8Array(262144)};
    active=s;attempted.add(peer);results.delete(peer);
    for(let offset=0;offset<s.payload.length;offset+=65536)crypto.getRandomValues(s.payload.subarray(offset,offset+65536));
    for(let lane=0;lane<LANES;lane++) {
      const pc=new runtime.RTCPeerConnection(configuration??defaultRTCConfiguration()),row={pc,dc:null,ice:[],signals:Promise.resolve(),signalCount:0};s.lanes.push(row);
      pc.onicecandidate=e=>{if(e.candidate)wire(s,{action:'signal',lane,candidate:e.candidate.toJSON?.()??e.candidate});};
      pc.onconnectionstatechange=()=>{if(['failed','closed','disconnected'].includes(pc.connectionState))retire(s,'connection_'+pc.connectionState);};
      pc.ondatachannel=e=>bind(s,lane,e.channel);
    }
    notify(s,{phase:'preflight',status:'started',sampleBytes:bytes,candidates:[1,2,3],laneCandidates:CANDIDATES.map(lanes=>[...lanes]),roundsPerDirection:24,laneRoundsPerDirection:20,confirmationBytes:CONFIRM_SCALE*bytes,
      intent:oneWay?'send':'explore',direction:initiator?'outgoing':'incoming',payloadBudgetBytes:(oneWay?1:2)*(1+SCREEN_ROUNDS+CONFIRM_ROUNDS*CONFIRM_SCALE+QUEUE_ROUNDS*QUEUE_SAMPLE_SCALE)*bytes,queueComparison:{rounds:QUEUE_ROUNDS,bytesPerRound:QUEUE_SAMPLE_SCALE*bytes,highWaterMarks:[TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES]},order:order(s),messageCeilingBytes:65536,queueHighBytes:QUEUE_BYTES});
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
    s.started=true;snapshot(s,'preflight-connected',null);void run(s,0).catch(e=>fail(s,e));
  }
  function bind(s,lane,dc) {
    if(!live(s)||s.lanes[lane].dc||dc.label!=='phantom-distribution-scout'||dc.ordered===false||dc.maxRetransmits!=null||dc.maxPacketLifeTime!=null){try{dc.close();}catch{}return;}
    const row=s.lanes[lane];row.dc=dc;dc.binaryType='arraybuffer';
    const opened=()=>{if(!live(s))return;wire(s,{action:'ready',lane});maybeStart(s);};
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
        const base=s.initiator?ROUNDS:0,end=base+LANE_ROUNDS;
        if((s.initiator&&(s.oneWay||!s.sentDirection))||(!s.initiator&&s.started)||s.receiving||s.receiveEnded||s.nextReceive<base+1||s.nextReceive>=end||body.nextRound!==end)throw Error('Invalid probe data');
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
      if(body.action==='finished'&&(s.oneWay?!s.initiator&&s.nextReceive===ROUNDS:s.initiator&&s.nextReceive===2*ROUNDS)){s.finishing=true;rowSend(s,0,{action:'finishAck'});return;}
      if(body.action==='finishAck'&&(s.oneWay?s.initiator:!s.initiator)&&s.running===false&&s.sentDirection){wire(s,{action:'complete'});retire(s,'complete',false);return;}
      throw Error('Invalid probe data');
    }
    const receiving=s.receiving;
    if(!(data instanceof ArrayBuffer)||data.byteLength<1||data.byteLength>(receiving?.spec.stage==='queue'?262144:65536)||!receiving?.counts.has(lane))throw Error('Invalid probe data');
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
        const selected=s.earlyStop?{lanes:Array.from({length:fallback},(_,i)=>i)}:selectTransferSetup(s.samples,fallback);
        s.outgoingQueueLanes=[...selected.lanes];
        notify(s,{phase:'preflight-queue-comparison',lanes:s.outgoingQueueLanes,rounds:QUEUE_ROUNDS,bytesPerRound:s.bytes*QUEUE_SAMPLE_SCALE,highWaterMarks:[TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES],queueOrder:s.reverse?[TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES,TIGHT_QUEUE_BYTES]:[DEFAULT_QUEUE_BYTES,TIGHT_QUEUE_BYTES,TIGHT_QUEUE_BYTES,DEFAULT_QUEUE_BYTES]});
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
        const ceiling=stage==='queue'?262144:65536;
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
      const sample={round,stage,connections,lanes,bytes,elapsedMs,delivery,queues,clock:'sender_monotonic',queueHighBytes:queueHigh,queueLowBytes:queueLow,peakBufferedBytes:peak,messageBytes,bufferedBefore:before,bufferedAfterEnqueue:queued,
        bufferedAfterAck:s.lanes.map(r=>r.dc.bufferedAmount),bytesPerSecond:elapsedMs>0?bytes*1000/elapsedMs:null,warmup:stage==='warmup'};
      s.outgoing=null;
      if(stage==='queue')s.queueSamples.push(sample);else if(!sample.warmup)s.samples.push(sample);
      notify(s,{phase:'preflight-sample',direction:'outgoing',...sample});snapshot(s,'preflight-sample-stats',round);
      if(!sample.warmup&&stage!=='queue'&&round<base+LANE_ROUNDS-1){
        const stop=scoutStopDecision(s.samples,fallback);
        if(stop){
          s.earlyStop=stop;
          const ended=new Promise((resolve,reject)=>{s.endAck={resolve,reject};});void ended.catch(()=>{});
          rowSend(s,0,{action:'endDirection',nextRound:base+LANE_ROUNDS});await ended;if(!live(s))return;round=base+LANE_ROUNDS-1;
        }
      }
    }
    s.running=false;
    const selection=s.earlyStop?{status:'inconclusive',connections:fallback,lanes:Array.from({length:fallback},(_,i)=>i),...s.earlyStop}:selectTransferSetup(s.samples,fallback);
    const result={version:6,id:s.id,scope:'synthetic_transfer_connections',direction:'outgoing',...selection,queuePolicy:selectQueuePolicy(s.queueSamples),queueSamples:structuredClone(s.queueSamples),queueComparisonPayloadBytes:s.queueSamples.reduce((total,sample)=>total+sample.bytes,0),
      stoppedEarly:!!s.earlyStop,completedSamples:s.samples.length,unrunSamples:20-s.samples.length,
      directionPayloadBytes:s.bytes+[...s.samples,...s.queueSamples].reduce((total,sample)=>total+sample.bytes,0),
      directionPayloadAvoidedBytes:(1+SCREEN_ROUNDS+CONFIRM_ROUNDS*CONFIRM_SCALE)*s.bytes-(s.bytes+s.samples.reduce((total,sample)=>total+sample.bytes,0)),
      sampleBytes:s.bytes,confirmationBytes:s.bytes*CONFIRM_SCALE,payloadBudgetBytes:(s.oneWay?1:2)*(1+SCREEN_ROUNDS+CONFIRM_ROUNDS*CONFIRM_SCALE+QUEUE_ROUNDS*QUEUE_SAMPLE_SCALE)*s.bytes,queueHighBytes:QUEUE_BYTES,samples:structuredClone(s.samples),validFor:'current_peer_connection_generation',
      transportObservations:structuredClone(s.transportObservations),
      receiverEvidence:receiverEvidence(s),
      limitations:['bounded_samples_include_ack_latency','per_association_probe_queue_cap','file_handoff_requires_retained_associations','does_not_measure_disk_or_hashing','does_not_establish_link_capacity','browser_stats_do_not_expose_kernel_receive_drops_or_socket_buffer_limits']};
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
    start(peer,oneWay=false) {
      if(!permitted(peer)||(!oneWay&&self()>peer)||busy()||active||parked||(!oneWay&&attempted.has(peer))||attempted.size>=128)return false;
      const id=newId();if(!validId(id))throw Error('Invalid probe id');
      let s;
      try {
        s=create(peer,id,true,sampleBytes,parseInt(id.replaceAll('-','').slice(-1),16)%2===1,oneWay);
        wire(s,{action:'init',sampleBytes:s.bytes,reverse:s.reverse,oneWay});
        for(const [lane,row]of s.lanes.entries()) {
          bind(s,lane,row.pc.createDataChannel('phantom-distribution-scout',{ordered:true}));
          void (async()=>{await row.pc.setLocalDescription(await row.pc.createOffer());wire(s,{action:'signal',lane,description:{type:'offer',sdp:row.pc.localDescription.sdp}});})().catch(e=>fail(s,e));
        }
        return true;
      }catch(e){if(active)retire(active,'setup_failed');return false;}
    },
    receive(peer,body) {
      if(!trusted(peer)||body?.type!=='transferScout'||body.version!==6||!validId(body.id))return false;
      if(body.action==='release'&&parked?.peer===peer&&parked.id===body.id){releasePool('peer_released',false);return true;}
      if(body.action==='receiverEvidence'){
        const s=active?.peer===peer&&active.id===body.id?active:parked?.peer===peer&&parked.id===body.id?parked:null;
        const round=body.observation?.round;
        if(!s||!Number.isInteger(round)||!s.sentRounds.has(round)||s.remoteEvidence.has(round))return false;
        const observation=readReceiverEvidence(body.observation,s.sentRounds.get(round));if(!observation)return false;
        s.remoteEvidence.set(round,observation);
        const status=s.remoteEvidence.size===s.sentRounds.size?'complete':'partial',result=results.get(peer);if(result?.id===s.id)result.receiverEvidence=receiverEvidence(s);
        notify(s,{phase:'preflight-receiver-evidence',direction:'incoming_report',observation,status,expectedRounds:s.sentRounds.size,receivedRounds:s.remoteEvidence.size});return true;
      }
      if(body.action==='init') {
        if(active?.peer===peer&&active.id===body.id)return true;
        const oneWay=body.oneWay===true;
        const invalid=typeof body.oneWay!=='boolean'||(!oneWay&&self()<peer)||!Number.isInteger(body.sampleBytes)||body.sampleBytes<3072||body.sampleBytes>sampleBytes||body.sampleBytes%6||typeof body.reverse!=='boolean';
        if(!invalid&&oneWay&&!demanded.has(peer)&&active?.peer===peer&&!active.oneWay)retire(active,'sender_intent',false);
        if(invalid||suspended||active||parked||busy()||(oneWay?demanded.has(peer):attempted.has(peer))||attempted.size>=128){send(peer,{type:'transferScout',version:6,id:body.id,action:'cancel',reason:invalid?'profile_not_supported':'busy'});return false;}
        try{if(oneWay)demanded.add(peer);create(peer,body.id,false,body.sampleBytes,body.reverse,oneWay);}catch{if(active)retire(active,'setup_failed');}return true;
      }
      const s=active;if(!s||s.peer!==peer||s.id!==body.id)return false;
      if(body.action==='cancel'){retire(s,['busy','profile_not_supported'].includes(body.reason)?'peer_'+body.reason:'peer_cancelled',false);return true;}
      if(body.action==='complete'&&(s.oneWay?!s.initiator&&s.nextReceive===ROUNDS:s.initiator&&s.nextReceive===2*ROUNDS)){retire(s,'complete',false);return true;}
      if(!Number.isInteger(body.lane)||body.lane<0||body.lane>=LANES)return false;
      if(body.action==='ready'){s.ready.add(body.lane);maybeStart(s);return true;}
      if(body.action==='signal'){const row=s.lanes[body.lane];if(++row.signalCount>64){retire(s,'signal_budget_exceeded');return false;}row.signals=row.signals.then(()=>signal(s,body.lane,body)).catch(e=>fail(s,e));return true;}
      return false;
    },
    async forSend(peer){
      if(!permitted(peer)||busy())return null;
      if(parked?.peer===peer&&!parked.asset&&results.has(peer))return structuredClone(results.get(peer));
      if(waiters.has(peer))return null;
      if(demanded.has(peer))return null;
      // A file declares its direction. Replace incomplete background exploration
      // with one bounded outgoing pass; never probe the opposite direction first.
      if(active)retire(active,'sender_intent');
      releasePool();demanded.add(peer);
      const completed=new Promise(resolve=>waiters.set(peer,resolve));
      if(!this.start(peer,true))settleSend(peer);
      return completed;
    },
    take(peer,id,lane,asset,jobId,preferredLanes=[0,1,2]){
      const pool=parked;if(!pool||pool.peer!==peer||(id&&pool.id!==id)||!trusted(peer)||(pool.asset&&pool.asset!==asset))return null;
      if(!validLanes(preferredLanes))return null;
      const index=lane===undefined?preferredLanes.find(index=>!pool.lanes[index].claimed):lane;
      if(!Number.isInteger(index)||index<0||index>=LANES)return null;
      const row=pool.lanes[index];if(row.claimed||row.pc.connectionState!=='connected')return null;
      pool.asset=asset;row.claimed=true;row.pc.onicecandidate=null;row.pc.onconnectionstatechange=null;row.pc.ondatachannel=null;
      row.dc.onclose=null;row.dc.onmessage=null;row.dc.close();
      notify(pool,{phase:'preflight-handoff',lane:index,status:'claimed',jobId});
      return {pc:row.pc,channel:row.pending??null,id:pool.id,lane:index};
    },
    get(peer){const result=results.get(peer);return result?{...structuredClone(result),transportAvailable:parked?.peer===peer&&!parked.asset}:null;},
    suspend(reason='file_selected'){suspended=true;if(active)retire(active,reason);},
    drop(peer){if(parked?.peer===peer)releasePool('peer_disconnected',false);demanded.delete(peer);if(active?.peer===peer)retire(active,'peer_disconnected',false);results.delete(peer);attempted.delete(peer);},
    clear(){releasePool('generation_changed');demanded.clear();if(active)retire(active,'generation_changed',false);results.clear();attempted.clear();suspended=false;},
    close(){releasePool('session_closed');demanded.clear();if(active)retire(active,'session_closed');closed=true;results.clear();attempted.clear();},
  };
}
