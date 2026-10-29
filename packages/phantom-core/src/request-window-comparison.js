/** Candidate values are per association; callers may supply a different bounded
 * set. Slot/file limits can collapse distinct requested candidates to one. */
export function requestWindowCandidates(values,limit){
 if(!Array.isArray(values)||values.length<1||values.length>3||values.some(n=>!Number.isInteger(n)||n<1||n>32)||!Number.isInteger(limit)||limit<1||limit>32)throw TypeError('Invalid request window candidates');
 return [...new Set(values.map(n=>Math.min(n,limit)))].sort((a,b)=>a-b);
}
export function requestWindowOrder(candidates){return [...candidates,...candidates.toReversed()];}
const signature=p=>JSON.stringify([p.lanes,p.chunkSize,p.bytes,p.batchSize,p.chunksPerLane,p.sourceReadBlockBytes,p.messageBytes,p.queuePolicy]);
/** Compare receiver-local delivery on identical, verified workloads. Sender
 * selected-route deltas are local rejection evidence, never wire-loss estimates. */
export function compareRequestWindows(reports,candidates,{maxRateReduction=0.1,maxGapIncrease=0.1}={}){
 if(JSON.stringify(requestWindowCandidates(candidates,32))!==JSON.stringify(candidates))throw TypeError('Window candidates must be unique and ascending');
 if(!Number.isFinite(maxGapIncrease)||maxGapIncrease<0||maxGapIncrease>0.1)throw TypeError('Invalid window gap tradeoff');
 if(!Number.isFinite(maxRateReduction)||maxRateReduction<0||maxRateReduction>0.1)throw TypeError('Invalid request window tradeoff');
 const fallback=reason=>({status:'inconclusive',reason,requestWindow:candidates[0],candidates:[...candidates],maxRateReduction,maxGapIncrease});
 const order=requestWindowOrder(candidates);
 if(!Array.isArray(reports)||reports.length!==order.length)return fallback('repeated_window_trials_required');
 const first=reports[0]?.profile;if(!first)return fallback('window_profile_missing');
 const rows=[];
 for(const [i,r]of reports.entries()){
  const p=r?.profile,rx=r?.receiver;
  if(r?.status!=='complete'||!p||p.windowTrial!=='measure'||p.windowStep!==i||p.requestWindow!==order[i]||JSON.stringify(p.windowCandidates)!==JSON.stringify(candidates)||signature(p)!==signature(first)||rx?.clock!=='receiver_monotonic'||rx.requestWindow!==p.requestWindow||rx.laneRequestWindow!==p.requestWindow||!Number.isFinite(rx.elapsedMs)||rx.elapsedMs<=0||!Array.isArray(rx.lanes)||rx.lanes.length!==p.lanes.length||rx.lanes.some((l,n)=>l.lane!==p.lanes[n]||l.verified!==true||l.bytes!==p.bytes/p.lanes.length||l.messages!==p.chunksPerLane||!Number.isFinite(l.maxGapMs)||l.maxGapMs<0||l.maxGapMs>rx.elapsedMs))return fallback('window_trials_incomparable_or_unverified');
  const cleanQueues=Array.isArray(r.sourceSubmission)&&r.sourceSubmission.length===p.lanes.length&&r.sourceSubmission.every((l,n)=>l.lane===p.lanes[n]&&l.finalQueue?.bufferedBytes===0)&&Array.isArray(rx.queues)&&rx.queues.length===p.lanes.length&&rx.queues.every((l,n)=>l.lane===p.lanes[n]&&l.bufferedBytes===0);
  const cleanTransport=Array.isArray(r.sourceTransport)&&r.sourceTransport.length===p.lanes.length&&r.sourceTransport.every((l,n)=>l.lane===p.lanes[n]&&l.change?.counters?.packetsDiscardedOnSend?.status==='observed'&&l.change.counters.packetsDiscardedOnSend.value===0);
  rows.push({requestWindow:p.requestWindow,bytesPerSecond:p.bytes*1000/rx.elapsedMs,maxGapMs:Math.max(...rx.lanes.map(l=>l.maxGapMs)),eligible:cleanQueues&&cleanTransport&&Array.isArray(rx.retries)&&rx.retries.length===0});
 }
 const summaries=candidates.map(requestWindow=>{const own=rows.filter(r=>r.requestWindow===requestWindow);return {requestWindow,eligible:own.length===2&&own.every(r=>r.eligible),rateMin:Math.min(...own.map(r=>r.bytesPerSecond)),rateMax:Math.max(...own.map(r=>r.bytesPerSecond)),gapMax:Math.max(...own.map(r=>r.maxGapMs))};});
 const clean=summaries.filter(s=>s.eligible);
 if(!clean.length)return {...fallback('clean_window_transport_evidence_unavailable'),evidence:summaries};
 const winner=clean.find(s=>clean.every(other=>other===s||s.rateMin>other.rateMax)&&clean.every(other=>s.gapMax<=other.gapMax*(1+maxGapIncrease)));
 if(winner)return {status:'measured',reason:clean.length===1&&summaries.length>1?'only_repeated_clean_window':'repeated_verified_rate_and_stall_winner',requestWindow:winner.requestWindow,candidates:[...candidates],evidence:summaries,maxRateReduction,maxGapIncrease};
 const fastestRate=Math.max(...clean.map(s=>s.rateMax)),bestGap=Math.min(...clean.map(s=>s.gapMax));
 const bounded=clean.find(s=>s.rateMin>=fastestRate*(1-maxRateReduction)&&s.gapMax<=bestGap*(1+maxGapIncrease));
 return bounded?{status:'bounded_tradeoff',reason:'smaller_window_within_observed_rate_budget',requestWindow:bounded.requestWindow,candidates:[...candidates],evidence:summaries,maxRateReduction,maxGapIncrease}:{...fallback('window_rate_or_stall_ranges_inconclusive'),requestWindow:clean[0].requestWindow,evidence:summaries};
}
