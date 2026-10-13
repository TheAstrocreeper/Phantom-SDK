/** Bounded candidate-level evidence. This does not select or alter ICE routes. */
export function describeIceCandidate(candidate){
 const c=typeof candidate?.toJSON==='function'?candidate.toJSON():candidate??{};
 const raw=String(c.candidate??'').slice(0,2048),parts=raw.trim().split(/\s+/),typeAt=parts.indexOf('typ'),ufragAt=parts.indexOf('ufrag');
 return {candidate:raw,sdpMid:c.sdpMid??null,sdpMLineIndex:c.sdpMLineIndex??null,usernameFragment:c.usernameFragment??(ufragAt>=0?parts[ufragAt+1]:null),address:parts[4]??null,port:parts[5]??null,protocol:parts[2]??null,candidateType:typeAt>=0?parts[typeAt+1]:null};
}
export function createIceTrace({now=Date.now,emit=()=>{},limit=512}={}){
 const events=[];let dropped=0,sequence=0;
 return {record(stage,candidate,context={}){
  const description=describeIceCandidate(candidate);
  // Exact candidate tuple, rather than a shortened hash, correlates both reports.
  const key=JSON.stringify([description.candidate,description.sdpMid,description.sdpMLineIndex,description.usernameFragment]);
  const row={sequence:++sequence,at:now(),stage,key,...context,candidate:description};
  events.push(row);if(events.length>limit){events.shift();dropped++;}try{emit(structuredClone(row));}catch{}
 },snapshot:()=>structuredClone({limit,dropped,events})};
}
