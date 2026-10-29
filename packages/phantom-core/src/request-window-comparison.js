/** Candidate values are per association; callers may supply a different bounded
 * set. Slot/file limits can collapse distinct requested candidates to one. */
export function requestWindowCandidates(values,limit){
 if(!Array.isArray(values)||values.length<1||values.length>3||values.some(n=>!Number.isInteger(n)||n<1||n>32)||!Number.isInteger(limit)||limit<1||limit>32)throw TypeError('Invalid request window candidates');
 return [...new Set(values.map(n=>Math.min(n,limit)))].sort((a,b)=>a-b);
}
export function requestWindowOrder(candidates){return [...candidates,...candidates.toReversed()];}
const signature=p=>JSON.stringify([p.lanes,p.chunkSize,p.bytes,p.batchSize,p.chunksPerLane,p.sourceReadBlockBytes,p.messageBytes,p.queuePolicy,p.deliveryWindowCount]);
/** Four byte-bounded windows on one receiver clock. The first includes startup;
 * subsequent windows expose slow delivery that a whole-trial average can hide.
 * Zero-duration windows are valid observations but cannot establish a rate. */
export function readRequestWindowDelivery(profile,receiver){
 const count=profile?.deliveryWindowCount,windows=receiver?.deliveryWindows,total=profile?.chunksPerLane*profile?.lanes?.length;
 if(!Number.isInteger(count)||count!==Math.min(4,profile?.chunksPerLane)||count<1||!Array.isArray(windows)||windows.length!==count||!Number.isFinite(receiver?.elapsedMs)||receiver.elapsedMs<=0)return null;
 let previous={bytes:0,messages:0,elapsedMs:0};const rates=[];
 for(const [i,w]of windows.entries()){
  const expected=Math.ceil(total*(i+1)/count);
  if(!w||w.messages!==expected||w.bytes!==expected*profile.chunkSize||!Number.isFinite(w.elapsedMs)||w.elapsedMs<previous.elapsedMs||w.elapsedMs>receiver.elapsedMs)return null;
  const elapsed=w.elapsedMs-previous.elapsedMs;rates.push(elapsed>0?(w.bytes-previous.bytes)*1000/elapsed:null);previous=w;
 }
 if(previous.bytes!==profile.bytes)return null;
 return {windows:structuredClone(windows),rates,lateRateMin:count===4&&rates.slice(1).every(n=>Number.isFinite(n)&&n>0)?Math.min(...rates.slice(1)):null};
}
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
  const delivery=readRequestWindowDelivery(p,rx);if(!delivery)return fallback('window_delivery_evidence_invalid');
  const cleanQueues=Array.isArray(r.sourceSubmission)&&r.sourceSubmission.length===p.lanes.length&&r.sourceSubmission.every((l,n)=>l.lane===p.lanes[n]&&l.finalQueue?.bufferedBytes===0)&&Array.isArray(rx.queues)&&rx.queues.length===p.lanes.length&&rx.queues.every((l,n)=>l.lane===p.lanes[n]&&l.bufferedBytes===0);
  const cleanTransport=Array.isArray(r.sourceTransport)&&r.sourceTransport.length===p.lanes.length&&r.sourceTransport.every((l,n)=>l.lane===p.lanes[n]&&l.change?.counters?.packetsDiscardedOnSend?.status==='observed'&&l.change.counters.packetsDiscardedOnSend.value===0);
  rows.push({requestWindow:p.requestWindow,bytesPerSecond:delivery.lateRateMin,totalBytesPerSecond:p.bytes*1000/rx.elapsedMs,deliveryWindows:delivery.windows,windowRates:delivery.rates,maxGapMs:Math.max(...rx.lanes.map(l=>l.maxGapMs)),eligible:delivery.lateRateMin!==null&&cleanQueues&&cleanTransport&&Array.isArray(rx.retries)&&rx.retries.length===0});
 }
 const summaries=candidates.map(requestWindow=>{const own=rows.filter(r=>r.requestWindow===requestWindow);return {requestWindow,eligible:own.length===2&&own.every(r=>r.eligible),rateMin:own.every(r=>r.bytesPerSecond!==null)?Math.min(...own.map(r=>r.bytesPerSecond)):null,rateMax:own.every(r=>r.bytesPerSecond!==null)?Math.max(...own.map(r=>r.bytesPerSecond)):null,gapMax:Math.max(...own.map(r=>r.maxGapMs)),totalRateMin:Math.min(...own.map(r=>r.totalBytesPerSecond)),totalRateMax:Math.max(...own.map(r=>r.totalBytesPerSecond)),trials:own.map(r=>({deliveryWindows:r.deliveryWindows,windowRates:r.windowRates}))};});
 const clean=summaries.filter(s=>s.eligible);
 if(!clean.length)return {...fallback('clean_window_transport_evidence_unavailable'),evidence:summaries};
 const winner=clean.find(s=>clean.every(other=>other===s||s.rateMin>other.rateMax)&&clean.every(other=>s.gapMax<=other.gapMax*(1+maxGapIncrease)));
 if(winner)return {status:'measured',reason:summaries.length===1?'single_candidate_verified_without_contrast':clean.length===1&&summaries.length>1?'only_repeated_clean_window':'repeated_verified_late_window_and_stall_winner',requestWindow:winner.requestWindow,candidates:[...candidates],evidence:summaries,maxRateReduction,maxGapIncrease};
 const fastestRate=Math.max(...clean.map(s=>s.rateMax)),bestGap=Math.min(...clean.map(s=>s.gapMax));
 const bounded=clean.find(s=>s.rateMin>=fastestRate*(1-maxRateReduction)&&s.gapMax<=bestGap*(1+maxGapIncrease));
 return bounded?{status:'bounded_tradeoff',reason:'smaller_window_within_observed_rate_budget',requestWindow:bounded.requestWindow,candidates:[...candidates],evidence:summaries,maxRateReduction,maxGapIncrease}:{...fallback('window_rate_or_stall_ranges_inconclusive'),requestWindow:clean[0].requestWindow,evidence:summaries};
}

/** First-contact decisions use only this connection generation's completed trials.
 * Survey each configured candidate, then confirm an observed leader. Overlap or
 * missing transport evidence spends the remaining bounded repetitions instead
 * of pretending that one noisy sample established an optimum. */
export function nextRequestWindowProbe(reports,candidates,{maxRateReduction=0.1,maxGapIncrease=0.1}={}){
 if(JSON.stringify(requestWindowCandidates(candidates,32))!==JSON.stringify(candidates)||!Number.isFinite(maxRateReduction)||maxRateReduction<0||maxRateReduction>0.1||!Number.isFinite(maxGapIncrease)||maxGapIncrease<0||maxGapIncrease>0.1)throw TypeError('Invalid adaptive window comparison');
 const base={adaptive:true,candidates:[...candidates],maxRateReduction,maxGapIncrease};
 const fallback=reason=>({...base,action:'select',status:'inconclusive',reason,requestWindow:candidates[0]});
 if(!Array.isArray(reports)||reports.length>candidates.length*2)return fallback('adaptive_window_budget_invalid');
 const groups=new Map(candidates.map(n=>[n,[]]));const first=reports[0]?.profile;
 for(const [i,r]of reports.entries()){
  const p=r?.profile,rx=r?.receiver,own=groups.get(p?.requestWindow);
  if(!own||own.length>=2||r.status!=='complete'||p.windowAdaptive!==true||p.windowTrial!=='measure'||p.windowStep!==i||JSON.stringify(p.windowCandidates)!==JSON.stringify(candidates)||signature(p)!==signature(first)||rx?.clock!=='receiver_monotonic'||rx.requestWindow!==p.requestWindow||rx.laneRequestWindow!==p.requestWindow||!Array.isArray(rx.lanes)||rx.lanes.length!==p.lanes.length||rx.lanes.some((l,n)=>l.lane!==p.lanes[n]||l.verified!==true||l.bytes!==p.bytes/p.lanes.length||l.messages!==p.chunksPerLane||!Number.isFinite(l.maxGapMs)||l.maxGapMs<0||l.maxGapMs>rx.elapsedMs))return fallback('adaptive_window_evidence_invalid');
  const delivery=readRequestWindowDelivery(p,rx);if(!delivery)return fallback('adaptive_window_delivery_invalid');
  const queues=Array.isArray(r.sourceSubmission)&&r.sourceSubmission.length===p.lanes.length&&r.sourceSubmission.every((l,n)=>l.lane===p.lanes[n]&&l.finalQueue?.bufferedBytes===0)&&Array.isArray(rx.queues)&&rx.queues.length===p.lanes.length&&rx.queues.every((l,n)=>l.lane===p.lanes[n]&&l.bufferedBytes===0);
  const transport=Array.isArray(r.sourceTransport)&&r.sourceTransport.length===p.lanes.length&&r.sourceTransport.every((l,n)=>l.lane===p.lanes[n]&&l.change?.counters?.packetsDiscardedOnSend?.status==='observed'&&l.change.counters.packetsDiscardedOnSend.value===0);
  const rates=delivery.rates.slice(1),measured=delivery.lateRateMin!==null;
  own.push({trial:r.trial??i+1,rateMin:measured?Math.min(...rates):null,rateMax:measured?Math.max(...rates):null,totalRate:p.bytes*1000/rx.elapsedMs,gap:Math.max(...rx.lanes.map(l=>l.maxGapMs)),clean:measured&&queues&&transport&&Array.isArray(rx.retries)&&rx.retries.length===0,deliveryWindows:delivery.windows});
 }
 const evidence=candidates.map(requestWindow=>{const rows=groups.get(requestWindow),measured=rows.length>0&&rows.every(r=>r.rateMin!==null);return {requestWindow,count:rows.length,eligible:rows.length>0&&rows.every(r=>r.clean),rateMin:measured?Math.min(...rows.map(r=>r.rateMin)):null,rateMax:measured?Math.max(...rows.map(r=>r.rateMax)):null,gapMax:rows.length?Math.max(...rows.map(r=>r.gap)):null,trials:rows};});
 const probe=(requestWindow,reason)=>({...base,action:'probe',status:'collecting',reason,requestWindow,evidence});
 const untested=evidence.find(s=>s.count===0);if(untested)return probe(untested.requestWindow,'survey_untested_candidate');
 const clean=evidence.filter(s=>s.eligible),allClean=clean.length===evidence.length;
 const leader=allClean&&clean.find(s=>clean.every(o=>o===s||s.rateMin>o.rateMax)&&clean.every(o=>s.gapMax<=o.gapMax*(1+maxGapIncrease)));
 if(leader){
  if(leader.count<2)return probe(leader.requestWindow,'confirm_observed_leader');
  return {...base,action:'select',status:'measured',reason:evidence.length===1?'single_candidate_verified_without_contrast':'confirmed_leader_over_observed_alternatives',requestWindow:leader.requestWindow,evidence};
 }
 const uncertain=evidence.filter(s=>s.count<2).sort((a,b)=>Number(b.eligible)-Number(a.eligible)||(b.rateMin??-1)-(a.rateMin??-1)||a.requestWindow-b.requestWindow);
 if(uncertain.length)return probe(uncertain[0].requestWindow,'resolve_overlapping_or_unknown_evidence');
 const fastest=Math.max(...clean.map(s=>s.rateMax)),gap=Math.min(...clean.map(s=>s.gapMax));
 const bounded=clean.find(s=>s.rateMin>=fastest*(1-maxRateReduction)&&s.gapMax<=gap*(1+maxGapIncrease));
 if(allClean&&bounded)return {...base,action:'select',status:'bounded_tradeoff',reason:'repeated_windows_within_observed_tradeoff',requestWindow:bounded.requestWindow,evidence};
 return {...fallback(clean.length?'adaptive_window_ranges_inconclusive':'clean_window_transport_evidence_unavailable'),requestWindow:clean[0]?.requestWindow??candidates[0],evidence};
}
