const validLanes=lanes=>Array.isArray(lanes)&&lanes.length>0&&lanes.length<=3&&lanes.every((lane,i)=>Number.isInteger(lane)&&lane>=0&&lane<3&&(!i||lane>lanes[i-1]));
/** A subset is eligible only after verified receipt and empty queues at both ends.
 * No throughput threshold, cross-clock subtraction, or fastest-lane claim.
 */
export function decideFileWorkload(report){
 const profile=report?.profile,lanes=profile?.lanes,receiver=report?.receiver;
 const unknown=reason=>({status:'unavailable',reason,lanes:[],rejected:[]});
 if(report?.version!==2||!validLanes(lanes)||!Number.isSafeInteger(profile.bytes)||profile.bytes<1||profile.bytes%lanes.length)return unknown('workload_evidence_unavailable');
 const perLane=profile.bytes/lanes.length,rows=receiver?.lanes;
 if(!receiver||receiver.clock!=='receiver_monotonic'||!Array.isArray(rows)||rows.length!==lanes.length||rows.some((row,i)=>row.lane!==lanes[i]||!Number.isSafeInteger(row.bytes)||row.bytes<0||row.bytes>perLane||typeof row.verified!=='boolean'))return unknown('receiver_verification_unavailable');
 if(report.status==='complete'&&rows.every(row=>row.bytes===perLane&&row.verified))return {status:'complete',reason:'bounded_workload_verified',lanes:[...lanes],rejected:[]};
 if(report.status!=='failed')return unknown('workload_did_not_complete');
 if(!Array.isArray(report.sourceSubmission)||!Array.isArray(receiver.queues))return unknown('queue_ownership_unavailable');
 const survivors=rows.filter(row=>row.bytes===perLane&&row.verified&&report.sourceSubmission?.find(s=>s.lane===row.lane)?.finalQueue?.bufferedBytes===0&&receiver.queues?.find(s=>s.lane===row.lane)?.bufferedBytes===0).map(row=>row.lane);
 const rejected=lanes.filter(lane=>!survivors.includes(lane));
 if(!survivors.length||!rejected.length)return {status:'blocked',reason:'no_verified_clean_subset',lanes:[],rejected:[...lanes]};
 return {status:'retry_subset',reason:'verify_completed_subset_without_failed_associations',lanes:survivors,rejected};
}
