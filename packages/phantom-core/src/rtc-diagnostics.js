const fields=['peer','pipe','attempt','incomingAttempt','boot','incomingBoot','incomingUfrag','owner','pc','ice','signaling','gathering','channel','candidate','candidateType','protocol','candidateUfrag','localUfrag','remoteUfrag','generationMatches','errorName','errorMessage','errorCode','result','reason','state','pairId','localAddress','localPort','remoteAddress','remotePort','selected','nominated','writable','bytesSent','bytesReceived','requestsSent','requestsReceived','responsesSent','responsesReceived','localCandidateType','remoteCandidateType','currentRoundTripTime','pendingCandidates','appliedCandidates','failedCandidates'];
export function createRtcDiagnostics({limit=256,now=Date.now}={}) {
 const rows=[];let sequence=0,dropped=0;const flights=new WeakSet();
 function record(event,values={}){
  const row={sequence:++sequence,at:now(),event:String(event).slice(0,256)};
  let remaining=4096;row.truncatedFields=[];
  for(const field of fields){const value=values[field];if(typeof value==='string'){
    const cap=Math.min(field==='errorMessage'||field==='reason'||field==='candidate'?2048:256,remaining);
    row[field]=value.slice(0,cap);remaining-=row[field].length;
    if(value.length>cap)row.truncatedFields.push(field);
   }else if(typeof value==='boolean'||typeof value==='number'&&Number.isFinite(value))row[field]=value;}

  rows.push(row);if(rows.length>limit){rows.shift();dropped++;}
 }
 async function stats(pc,context={}){
  if(typeof pc?.getStats!=='function'||flights.has(pc))return;
  flights.add(pc);
  try{
   const report=await pc.getStats(),all=[];report.forEach(row=>all.push(row));
   const byId=new Map(all.map(row=>[row.id,row]));
   const selected=new Set(all.filter(row=>row.type==='transport').map(row=>row.selectedCandidatePairId));
   const pairs=all.filter(row=>row.type==='candidate-pair').sort((a,b)=>Number(selected.has(b.id))-Number(selected.has(a.id))||Number(!!b.nominated)-Number(!!a.nominated)||(b.requestsSent||0)-(a.requestsSent||0)).slice(0,16);
   if(!pairs.length)record('candidate_pairs_empty',context);
   for(const pair of pairs){const local=byId.get(pair.localCandidateId),remote=byId.get(pair.remoteCandidateId);
    record('candidate_pair',{...context,...pair,pairId:String(pair.id||''),localAddress:local?.address||local?.ip,localPort:local?.port,remoteAddress:remote?.address||remote?.ip,remotePort:remote?.port,selected:selected.has(pair.id)||pair.selected===true,localCandidateType:local?.candidateType,remoteCandidateType:remote?.candidateType,protocol:local?.protocol});}
  }catch(error){record('get_stats_failed',{...context,errorName:error?.name,errorMessage:error?.message});}
  finally{flights.delete(pc);}
 }
 return {record,stats,snapshot:()=>({limit,dropped,events:structuredClone(rows)})};
}
export function iceDetails(candidate,pc){
 const line=String(candidate?.candidate||'');
 const candidateUfrag=String(candidate?.usernameFragment||line.match(/\bufrag\s+(\S+)/)?.[1]||'');
 const ufrag=description=>String(description?.sdp||'').match(/(?:^|\r?\n)a=ice-ufrag:([^\r\n]+)/)?.[1]||'';
 const remoteUfrag=ufrag(pc?.remoteDescription);
 return {candidate:line,candidateType:line.match(/\btyp\s+(\S+)/)?.[1]||'',protocol:line.split(/\s+/)[2]||'',candidateUfrag,localUfrag:ufrag(pc?.localDescription),remoteUfrag,
  ...(candidateUfrag&&remoteUfrag?{generationMatches:candidateUfrag===remoteUfrag}:{})};
}
