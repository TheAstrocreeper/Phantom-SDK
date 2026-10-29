const DEFAULT_HIGH=16*1024*1024,DEFAULT_LOW=4*1024*1024;
const key=lanes=>lanes.join(',');
const range=values=>({min:Math.min(...values),max:Math.max(...values)});
const lateRate=sample=>{
 const points=sample.delivery;if(!Array.isArray(points)||points.length<3||points.length>6||points.some(p=>!p||!Number.isSafeInteger(p.bytes)||!Number.isFinite(p.elapsedMs)||p.elapsedMs<0)||points[0].bytes!==0||points[0].elapsedMs!==0)return null;
 for(let i=1;i<points.length;i++)if(points[i].bytes<=points[i-1].bytes||points[i].elapsedMs<points[i-1].elapsedMs)return null;
 const last=points.at(-1),middle=points.find(p=>p.bytes>=sample.bytes/2&&p.bytes<=sample.bytes*3/4);return last.bytes===sample.bytes&&last.elapsedMs===sample.elapsedMs&&middle&&last.elapsedMs>middle.elapsedMs?(last.bytes-middle.bytes)*1000/(last.elapsedMs-middle.elapsedMs):null;
};
const validLanes=lanes=>Array.isArray(lanes)&&lanes.length>0&&lanes.length<=3&&lanes.every((n,i)=>Number.isInteger(n)&&n>=0&&n<3&&(!i||n>lanes[i-1]));

function receiverRows(samples,evidence){
 if(!Array.isArray(samples)||!Array.isArray(evidence?.observations))return null;
 const rows=[];
 for(const sample of samples){
  if(!sample||!validLanes(sample.lanes)||!Number.isSafeInteger(sample.bytes)||sample.bytes<1)return null;
  const matches=evidence.observations.filter(r=>r.round===sample.round);
  if(matches.length!==1)return null;
  const r=matches[0];
  if(r.provenance!=='peer_reported'||r.clock!=='receiver_monotonic'||r.stage!==sample.stage||r.bytes!==sample.bytes||!Number.isFinite(r.elapsedMs)||r.elapsedMs<=0||!Array.isArray(r.lanes)||r.lanes.length!==sample.lanes.length)return null;
  if(r.lanes.some((lane,i)=>lane.lane!==sample.lanes[i]||lane.bytes!==sample.bytes/sample.lanes.length||!Number.isFinite(lane.maxGapMs)||lane.maxGapMs<0||lane.maxGapMs>r.elapsedMs))return null;
  rows.push({round:sample.round,lanes:[...sample.lanes],bytes:sample.bytes,highWaterMark:sample.queueHighBytes,messageCeilingBytes:sample.messageCeilingBytes,
   elapsedMs:r.elapsedMs,bytesPerSecond:r.bytes*1000/r.elapsedMs,maxGapMs:Math.max(...r.lanes.map(l=>l.maxGapMs))});
 }
 return rows;
}
function envelope(rows){return {repetitions:rows.length,bytes:rows[0].bytes,elapsedMs:range(rows.map(r=>r.elapsedMs)),bytesPerSecond:range(rows.map(r=>r.bytesPerSecond)),maxGapMs:range(rows.map(r=>r.maxGapMs))};}
function testedProfiles(result){
 const groups=new Map();
 for(const sample of [...(Array.isArray(result?.samples)?result.samples:[]),...(Array.isArray(result?.queueSamples)?result.queueSamples:[]),...(Array.isArray(result?.messageSamples)?result.messageSamples:[])]){
  const row=receiverRows([sample],result?.receiverEvidence)?.[0];if(!row)continue;
  const id=[sample.stage,key(row.lanes),row.bytes,row.highWaterMark??'',row.messageCeilingBytes??''].join(':');
  if(!groups.has(id))groups.set(id,{stage:sample.stage,lanes:row.lanes,highWaterMark:row.highWaterMark??null,messageCeilingBytes:row.messageCeilingBytes??null,rows:[]});
  groups.get(id).rows.push(row);
 }
 return [...groups.values()].map(({rows,...profile})=>({...profile,rounds:rows.map(r=>r.round),...envelope(rows)}));
}

/** A connection plan uses the existing repeated sender qualification, then
 * checks receiver delivery and stalls on the same profiles. Unknown native
 * socket state stays unknown; peer counter differences are never wire loss.
 */
export function buildScoutConnectionPlan(result,{fallback=3,maxRateReduction=0.1}={}){
 if(!Number.isFinite(maxRateReduction)||maxRateReduction<0||maxRateReduction>0.1)throw TypeError('Invalid lane rate tradeoff');
 if(![1,2,3].includes(fallback))throw TypeError('Invalid connection plan fallback');
 const fallbackLanes=Array.from({length:fallback},(_,i)=>i);
 let lanes=fallbackLanes,laneReason=result?.reason??'sender_evidence_unavailable',laneEvidence=[];
 const confirmations=(Array.isArray(result?.samples)?result.samples:[]).filter(s=>s?.stage==='confirm'),receiverConfirmations=receiverRows(confirmations,result?.receiverEvidence);
 if(receiverConfirmations?.length===6)laneEvidence=[...new Set(receiverConfirmations.map(r=>key(r.lanes)))].map(k=>({lanes:receiverConfirmations.find(r=>key(r.lanes)===k).lanes,...envelope(receiverConfirmations.filter(r=>key(r.lanes)===k))}));
 if(result?.status==='measured'&&validLanes(result.lanes)){
  const samples=(Array.isArray(result.samples)?result.samples:[]).filter(s=>s?.stage==='confirm'),rows=receiverRows(samples,result.receiverEvidence);
  if(!rows||rows.length!==6)laneReason='receiver_confirmation_unavailable';
  else{
   const profiles=[...new Set(rows.map(r=>key(r.lanes)))].map(k=>({lanes:rows.find(r=>key(r.lanes)===k).lanes,...envelope(rows.filter(r=>key(r.lanes)===k))}));
   laneEvidence=profiles;
   const own=profiles.find(p=>key(p.lanes)===key(result.lanes)),others=profiles.filter(p=>p!==own);
   if(!own||profiles.length!==3||profiles.some(p=>p.repetitions!==2)||new Set(profiles.map(p=>p.bytes)).size!==1)laneReason='receiver_confirmation_incomparable';
   else if(!others.every(p=>own.bytesPerSecond.min>p.bytesPerSecond.max))laneReason='receiver_rate_does_not_confirm_lane_winner';
   else if(!others.every(p=>own.maxGapMs.max<=p.maxGapMs.min))laneReason='receiver_stalls_do_not_confirm_lane_winner';
   else{lanes=[...result.lanes];laneReason='sender_and_receiver_confirm_lane_winner';}
  }
 }
 let laneStatus=laneReason==='sender_and_receiver_confirm_lane_winner'?'measured':'fallback';
 const tradeoff={maxRateReduction,referenceLanes:fallbackLanes,claim:'bounded_observed_tradeoff_not_statistical_equivalence',candidates:[]};
 // A fallback may deliberately use fewer connections, but only with complete
 // larger-payload repetitions and explicit bounds at both endpoints and tails.
 const screening=(Array.isArray(result?.samples)?result.samples:[]).filter(s=>s?.stage==='screen');
 const comparable=screening.length===14&&screening.every(s=>validLanes(s.lanes))&&new Set(screening.map(s=>key(s.lanes))).size===7&&screening.every(s=>validLanes(s.lanes)&&Number.isFinite(s.elapsedMs)&&s.elapsedMs>0&&screening.filter(other=>key(other.lanes)===key(s.lanes)).length===2)&&new Set(screening.map(s=>s.bytes)).size===1&&confirmations.length===6&&confirmations.every(s=>validLanes(s.lanes)&&Number.isFinite(s.elapsedMs)&&s.elapsedMs>0&&s.bytes===screening[0].bytes*4&&Number.isFinite(lateRate(s))&&lateRate(s)>0)&&new Set([...screening,...confirmations].map(s=>s.round)).size===20;
 if(laneStatus==='fallback'&&(result?.status==='measured'||['confirmation_ranges_overlap','late_delivery_does_not_confirm_winner'].includes(result?.reason))&&comparable&&receiverConfirmations?.length===6&&laneEvidence.length===3&&laneEvidence.every(p=>p.repetitions===2)&&new Set(laneEvidence.map(p=>p.lanes.length)).size===3){
  const summaries=laneEvidence.map(profile=>{const rows=confirmations.filter(s=>key(s.lanes)===key(profile.lanes)),tails=rows.map(lateRate);return {...profile,senderRate:range(rows.map(s=>s.bytes*1000/s.elapsedMs)),senderLateRate:tails.every(n=>Number.isFinite(n)&&n>0)?range(tails):null};});
  const reference=summaries.find(p=>key(p.lanes)===key(fallbackLanes));
  if(reference?.senderLateRate){
   for(const p of summaries.filter(p=>p.lanes.length<fallbackLanes.length)){
    const reasons=[];if(p.senderRate.min<reference.senderRate.max*(1-maxRateReduction))reasons.push('sender_rate_outside_tradeoff');if(!p.senderLateRate||p.senderLateRate.min<reference.senderLateRate.max*(1-maxRateReduction))reasons.push('sender_late_rate_outside_tradeoff');if(p.bytesPerSecond.min<reference.bytesPerSecond.max*(1-maxRateReduction))reasons.push('receiver_rate_outside_tradeoff');if(p.maxGapMs.max>reference.maxGapMs.max)reasons.push('receiver_stalls_worse_than_reference');
    tradeoff.candidates.push({lanes:p.lanes,eligible:!reasons.length,reasons,...p});
   }
   const chosen=tradeoff.candidates.filter(p=>p.eligible).sort((a,b)=>a.lanes.length-b.lanes.length||key(a.lanes).localeCompare(key(b.lanes)))[0];
   if(chosen){lanes=[...chosen.lanes];laneStatus='bounded_tradeoff';laneReason='fewer_connections_within_explicit_rate_and_stall_bounds';}
  }
 }

 let queueReason=result?.comparison==='message'?'queue_held_fixed_for_message_comparison':result?.queuePolicy?.reason??'queue_evidence_unavailable',queueEvidence=[];
 let queuePolicy={status:'fallback',highWaterMark:DEFAULT_HIGH,lowWaterMark:DEFAULT_LOW};
 if(result?.queuePolicy?.status==='measured'){
  const samples=result.queueSamples,rows=receiverRows(samples,result.receiverEvidence);
  if(!Array.isArray(samples)||samples.length!==4||samples.some(s=>!validLanes(s.lanes)||key(s.lanes)!==key(lanes)))queueReason='queue_profile_does_not_match_selected_lanes';
  else if(!rows||rows.length!==4)queueReason='receiver_queue_evidence_unavailable';
  else{
   queueEvidence=[1024*1024,DEFAULT_HIGH].map(high=>{const group=rows.filter(r=>r.highWaterMark===high);return group.length===2?{highWaterMark:high,...envelope(group)}:null;});
   const [tight,normal]=queueEvidence;
   if(!tight||!normal||tight.bytes!==normal.bytes)queueReason='receiver_queue_comparison_incomparable';
   else if(tight.bytesPerSecond.min<normal.bytesPerSecond.max/0.9)queueReason='receiver_rate_does_not_confirm_smaller_queue';
   else if(tight.maxGapMs.max>normal.maxGapMs.min)queueReason='receiver_stalls_do_not_confirm_smaller_queue';
   else{queuePolicy={status:'measured',highWaterMark:1024*1024,lowWaterMark:512*1024};queueReason='sender_and_receiver_confirm_smaller_queue';}
  }
 }
 let messageReason=result?.messagePolicy?.reason??'message_size_not_tested',messageEvidence=[];
 let messagePolicy={status:'fallback',maxPayloadBytes:null};
 if(result?.messagePolicy?.status==='measured'){
  const samples=result.messageSamples,rows=receiverRows(samples,result.receiverEvidence);
  if(!Array.isArray(samples)||samples.length!==4||samples.some(s=>!validLanes(s.lanes)||key(s.lanes)!==key(lanes)))messageReason='message_profile_does_not_match_selected_lanes';
  else if(!rows||rows.length!==4)messageReason='receiver_message_evidence_unavailable';
  else{
   messageEvidence=[16384,262144].map(size=>{const group=rows.filter(r=>r.messageCeilingBytes===size);return group.length===2?{maxPayloadBytes:size,...envelope(group)}:null;});
   const [small,normal]=messageEvidence;
   if(!small||!normal||small.bytes!==normal.bytes)messageReason='receiver_message_comparison_incomparable';
   else if(small.bytesPerSecond.min<normal.bytesPerSecond.max/0.9)messageReason='receiver_rate_does_not_confirm_smaller_messages';
   else if(small.maxGapMs.max>normal.maxGapMs.min)messageReason='receiver_stalls_do_not_confirm_smaller_messages';
   else{messagePolicy={status:'measured',maxPayloadBytes:16384};messageReason='sender_and_receiver_confirm_smaller_messages';}
  }
 }
 const selection=result?.fileWorkloadSelection,workload=result?.fileWorkload,workloadLanes=workload?.profile?.lanes;
 if(selection?.version===1&&selection.status==='verified'&&workload?.version===2&&workload.status==='complete'&&selection.trial===workload.trial&&validLanes(selection.lanes)&&validLanes(workloadLanes)&&key(selection.lanes)===key(workloadLanes)&&workload.receiver?.lanes?.length===selection.lanes.length&&workload.receiver.lanes.every((row,i)=>row.lane===selection.lanes[i]&&row.verified===true&&row.bytes===workload.profile.bytes/selection.lanes.length)){
  lanes=[...selection.lanes];laneStatus='workload_verified';laneReason=selection.reason;
  // A repaired set is exercised with the baseline policy; never inherit an
  // optimization measured on a different set of associations.
  if(selection.rejected?.length){queuePolicy={status:'fallback',highWaterMark:DEFAULT_HIGH,lowWaterMark:DEFAULT_LOW};queueReason='baseline_policy_verified_on_reduced_workload';messagePolicy={status:'fallback',maxPayloadBytes:null};messageReason='baseline_policy_verified_on_reduced_workload';}
  const policy=selection.queuePolicy,actual=workload.profile.queuePolicy;
  if(policy&&policy.highWaterMark===actual?.highWaterMark&&policy.lowWaterMark===actual?.lowWaterMark&&((policy.highWaterMark===1048576&&policy.lowWaterMark===524288)||(policy.highWaterMark===DEFAULT_HIGH&&policy.lowWaterMark===DEFAULT_LOW))){queuePolicy={status:'workload_verified',...policy};queueReason='queue_policy_verified_on_selected_file_associations';}

 }
 if(selection?.version===1&&selection.status==='blocked'){lanes=[];laneStatus='blocked';laneReason=selection.reason;}
 const nativeLanes=lanes.map(lane=>{
  const report=result?.localTransportPreparation?.find(row=>row.lane===lane)?.report,before=report?.before,after=report?.applied?.after;
  const known=report?.scope==='owned_webview_udp_socket'&&before?.status==='observed'&&after?.status==='observed'&&typeof before.inode==='string'&&before.inode.length>0&&Number.isInteger(before.port)&&before.port>0&&before.port<=65535&&before.inode===after.inode&&before.cookie===after.cookie&&before.port===after.port&&Number.isSafeInteger(after.receiveBufferBytes)&&after.receiveBufferBytes>0;
  return {lane,status:known?'observed':'unknown',receiveBufferBytes:known?after.receiveBufferBytes:null,receiveDropsAtPreparation:known&&Number.isSafeInteger(after.receiveDrops)&&after.receiveDrops>=0?after.receiveDrops:null};
 });
 const nativeReceiveState={status:nativeLanes.length>0&&nativeLanes.every(row=>row.status==='observed')?'observed':'unknown',scope:'local_sockets_at_preflight_preparation',remoteState:'unknown',lanes:nativeLanes,reason:nativeLanes.length>0&&nativeLanes.every(row=>row.status==='observed')?'native_hook_readback':'native_readback_unavailable'};
 return {version:1,validFor:'current_peer_connection_generation' ,connections:lanes.length,lanes,
  laneDecision:{status:laneStatus,reason:laneReason,evidence:laneEvidence,tradeoff},
  messagePolicy:{...messagePolicy,reason:messageReason},messageDecision:{status:messagePolicy.status,reason:messageReason,evidence:messageEvidence},
  queuePolicy:{...queuePolicy,reason:queueReason},queueDecision:{status:queuePolicy.status,reason:queueReason,evidence:queueEvidence},
  receiverEvidence:{receivedRounds:result?.receiverEvidence?.receivedRounds??0,expectedRounds:result?.receiverEvidence?.expectedRounds??0},
  testedProfiles:testedProfiles(result),
  nativeReceiveState,fileWorkloadSelection:selection??null,
  limitations:['peer_reported_arrival_evidence','bounded_preflight_does_not_establish_link_capacity','packet_counters_are_not_aligned_wire_loss_measurements']};
}
