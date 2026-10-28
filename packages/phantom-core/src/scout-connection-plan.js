const DEFAULT_HIGH=16*1024*1024,DEFAULT_LOW=4*1024*1024;
const key=lanes=>lanes.join(',');
const range=values=>({min:Math.min(...values),max:Math.max(...values)});
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
export function buildScoutConnectionPlan(result,{fallback=3}={}){
 if(![1,2,3].includes(fallback))throw TypeError('Invalid connection plan fallback');
 const fallbackLanes=Array.from({length:fallback},(_,i)=>i);
 let lanes=fallbackLanes,laneReason=result?.reason??'sender_evidence_unavailable',laneEvidence=[];
 if(result?.status==='measured'&&validLanes(result.lanes)){
  const samples=(Array.isArray(result.samples)?result.samples:[]).filter(s=>s.stage==='confirm'),rows=receiverRows(samples,result.receiverEvidence);
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
 const laneMeasured=laneReason==='sender_and_receiver_confirm_lane_winner';
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
 return {version:1,validFor:'current_peer_connection_generation',connections:lanes.length,lanes,
  laneDecision:{status:laneMeasured?'measured':'fallback',reason:laneReason,evidence:laneEvidence},
  messagePolicy:{...messagePolicy,reason:messageReason},messageDecision:{status:messagePolicy.status,reason:messageReason,evidence:messageEvidence},
  queuePolicy:{...queuePolicy,reason:queueReason},queueDecision:{status:queuePolicy.status,reason:queueReason,evidence:queueEvidence},
  receiverEvidence:{receivedRounds:result?.receiverEvidence?.receivedRounds??0,expectedRounds:result?.receiverEvidence?.expectedRounds??0},
  testedProfiles:testedProfiles(result),
  nativeReceiveState:{status:'unknown',reason:'browser_api_does_not_expose_kernel_receive_drops_or_socket_buffer_limits'},
  limitations:['peer_reported_arrival_evidence','bounded_preflight_does_not_establish_link_capacity','packet_counters_are_not_aligned_wire_loss_measurements']};
}
