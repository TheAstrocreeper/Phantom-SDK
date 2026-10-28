// Peer-returned evidence is advisory, bounded, and detached from raw RTC stats.
// It never contains candidate addresses, raw stats rows, UA, or environment data.
const counters=['packetsSent','packetsReceived','bytesSent','bytesReceived','packetsDiscardedOnSend','bytesDiscardedOnSend'];
const nonnegative=n=>typeof n==='number'&&Number.isFinite(n)&&n>=0;
const integer=n=>Number.isSafeInteger(n)&&n>=0;
const identity=s=>s===null||(typeof s==='string'&&s.length>0&&s.length<=256);
const phases=['preflight-connected','preflight-sample-stats','preflight-receive-stats'];

export function makeReceiverEvidence(sample,observations) {
 return {version:1,round:sample.round,stage:sample.stage,bytes:sample.bytes,elapsedMs:sample.elapsedMs,
  lanes:sample.lanes.map(lane=>({...lane,transport:observations[lane.lane]?{
   round:observations[lane.lane].round,phase:observations[lane.lane].phase,
   startedAtMs:observations[lane.lane].startedAtMs,endedAtMs:observations[lane.lane].endedAtMs,
   current:observations[lane.lane].current}:null}))};
}

/** Validate against an actually transmitted round; copy only the allowed fields.
 * All arrival times and stats-read bounds belong to the receiver's clock.
 * Transport reads are latest available, not assumed to cover this round.
 */
export function readReceiverEvidence(value,spec) {
 if(!value||value.version!==1||!integer(value.round)||value.round>=50||!spec||value.stage!==spec.stage||value.bytes!==spec.bytes||!nonnegative(value.elapsedMs)||!Array.isArray(value.lanes)||value.lanes.length!==spec.lanes.length)return null;
 const lanes=[];
 for(const lane of value.lanes){
  if(!lane||!spec.lanes.includes(lane.lane)||lanes.some(row=>row.lane===lane.lane)||lane.bytes!==spec.perLane||!integer(lane.messages)||lane.messages<1||lane.messages>lane.bytes)return null;
  if(!['firstAtMs','lastAtMs','maxGapMs','completeAtMs'].every(k=>nonnegative(lane[k]))||lane.firstAtMs>lane.lastAtMs||lane.lastAtMs!==lane.completeAtMs||lane.completeAtMs>value.elapsedMs||lane.maxGapMs>value.elapsedMs)return null;
  if(!Array.isArray(lane.largestGaps)||lane.largestGaps.length>4)return null;
  const gaps=[];
  for(const gap of lane.largestGaps){
   if(!gap||!['fromMs','toMs','gapMs'].every(k=>nonnegative(gap[k]))||gap.fromMs>gap.toMs||gap.toMs>lane.completeAtMs||Math.abs(gap.gapMs-(gap.toMs-gap.fromMs))>0.001||!integer(gap.afterBytes)||gap.afterBytes<1||gap.afterBytes>=lane.bytes)return null;
   gaps.push({fromMs:gap.fromMs,toMs:gap.toMs,gapMs:gap.gapMs,afterBytes:gap.afterBytes});
  }
  let transport=null;
  if(lane.transport!==null){
   const t=lane.transport;
   if(!t||!(t.round===null||(integer(t.round)&&t.round<50))||!phases.includes(t.phase)||!nonnegative(t.startedAtMs)||!nonnegative(t.endedAtMs)||t.endedAtMs<t.startedAtMs)return null;
   let current=null;
   if(t.current!==null){
    const c=t.current;
    if(!c||!identity(c.transportId)||!identity(c.pairId)||!(c.timestampMs===null||nonnegative(c.timestampMs))||counters.some(k=>c[k]!==null&&!integer(c[k])))return null;
    current={transportId:c.transportId,pairId:c.pairId,timestampMs:c.timestampMs,...Object.fromEntries(counters.map(k=>[k,c[k]]))};
   }
   transport={round:t.round,phase:t.phase,startedAtMs:t.startedAtMs,endedAtMs:t.endedAtMs,current};
  }
  lanes.push({lane:lane.lane,bytes:lane.bytes,messages:lane.messages,firstAtMs:lane.firstAtMs,lastAtMs:lane.lastAtMs,maxGapMs:lane.maxGapMs,completeAtMs:lane.completeAtMs,largestGaps:gaps,transport});
 }
 lanes.sort((a,b)=>a.lane-b.lane);
 return {version:1,round:value.round,stage:value.stage,bytes:value.bytes,elapsedMs:value.elapsedMs,clock:'receiver_monotonic',provenance:'peer_reported',transportCoverage:'latest_available_not_round_aligned',lanes};
}
