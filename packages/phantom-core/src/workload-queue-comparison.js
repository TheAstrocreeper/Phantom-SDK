/** Repeated receiver observations on identical, fully verified workloads.
 * This is a bounded observed tradeoff, not a link-capacity/fastest-policy claim.
 */
export function compareWorkloadQueues(reports,{maxRateReduction=0.1}={}){
 if(!Number.isFinite(maxRateReduction)||maxRateReduction<0||maxRateReduction>0.1)throw TypeError('Invalid queue rate tradeoff');
 const unknown=reason=>({status:'incomplete',reason,selected:'baseline',maxRateReduction});
 if(!Array.isArray(reports)||reports.length!==4)return unknown('four_verified_trials_required');
 const first=reports[0]?.profile;
 if(!first)return unknown('profile_missing');
 const signature=p=>JSON.stringify([p.lanes,p.bytes,p.chunkSize,p.batchSize,p.chunksPerLane,p.sourceReadBlockBytes,p.messageBytes]);
 const order=['tight-1','baseline-1','baseline-2','tight-2'];
 const rows=[];
 for(const [i,r]of reports.entries()){
  const p=r?.profile,rx=r?.receiver;
  if(r?.status!=='complete'||!p||signature(p)!==signature(first)||p.queueTrial!==order[i]||rx?.clock!=='receiver_monotonic'||!Number.isFinite(rx.elapsedMs)||rx.elapsedMs<=0||!Array.isArray(rx.lanes)||rx.lanes.length!==p.lanes.length||rx.lanes.some((l,n)=>l.lane!==p.lanes[n]||l.verified!==true||l.bytes!==p.bytes/p.lanes.length||!Number.isFinite(l.maxGapMs)||l.maxGapMs<0||l.maxGapMs>rx.elapsedMs))return unknown('incomparable_or_unverified_trials');
  const tight=order[i].startsWith('tight');
  if(p.queuePolicy.highWaterMark!==(tight?1048576:16777216)||p.queuePolicy.lowWaterMark!==(tight?524288:4194304))return unknown('queue_profile_mismatch');
  if(!Array.isArray(r.sourceSubmission)||r.sourceSubmission.length!==p.lanes.length||r.sourceSubmission.some((l,n)=>l.lane!==p.lanes[n]||l.finalQueue?.bufferedBytes!==0)||!Array.isArray(rx.queues)||rx.queues.length!==p.lanes.length||rx.queues.some((l,n)=>l.lane!==p.lanes[n]||l.bufferedBytes!==0))return unknown('trial_queues_not_clean');
  rows.push({trial:r.trial,policy:tight?'tight':'baseline',bytesPerSecond:p.bytes*1000/rx.elapsedMs,maxGapMs:Math.max(...rx.lanes.map(l=>l.maxGapMs))});
 }
 if(!reports.filter(r=>r.profile.queueTrial.startsWith('baseline')).some(r=>r.sourceSubmission.some(l=>Number.isFinite(l.peakBufferedBytes)&&l.peakBufferedBytes>1048576)))return unknown('queue_depth_contrast_not_observed');
 const envelope=policy=>{const own=rows.filter(r=>r.policy===policy);return {trials:own.map(r=>r.trial),rateMin:Math.min(...own.map(r=>r.bytesPerSecond)),rateMax:Math.max(...own.map(r=>r.bytesPerSecond)),gapMin:Math.min(...own.map(r=>r.maxGapMs)),gapMax:Math.max(...own.map(r=>r.maxGapMs))};};
 const tight=envelope('tight'),baseline=envelope('baseline');
 const accept=tight.rateMin>=baseline.rateMax*(1-maxRateReduction)&&tight.gapMax<=baseline.gapMin;
 return {status:accept?'bounded_tradeoff':'baseline_retained',reason:accept?'smaller_queue_within_observed_rate_budget_and_no_stall_regression':'smaller_queue_not_confirmed',selected:accept?'tight':'baseline',maxRateReduction,tight,baseline};
}
