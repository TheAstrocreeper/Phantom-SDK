/** Event-driven RTC evidence, retained after retirement. No polling or timers. */
export function createRtcDiagnostics({now=Date.now,emit=()=>{},maxConnections=32,maxEvents=32,maxStats=128}={}) {
 const records=[],tracked=new WeakMap();let serial=0,droppedConnections=0;
 const types=new Set(['candidate-pair','local-candidate','remote-candidate','transport','sctp-transport','data-channel']);
 const fields=['id','type','timestamp','state','selected','nominated','localCandidateId','remoteCandidateId','transportId','selectedCandidatePairId','dtlsState','iceState','iceRole','requestsSent','requestsReceived','responsesSent','responsesReceived','consentRequestsSent','bytesSent','bytesReceived','packetsSent','packetsReceived','currentRoundTripTime','totalRoundTripTime','address','ip','port','protocol','candidateType','priority','networkType','relayProtocol','tcpType','usernameFragment','label','dataChannelIdentifier','messagesSent','messagesReceived'];
 const text=v=>typeof v==='string'?v.slice(0,512):v;
 function ufrags(description){return [...new Set([...String(description?.sdp??'').matchAll(/^a:ice-ufrag:([^\r\n]+)/gm)].map(m=>m[1].slice(0,256)))].slice(0,16);}
 function summary(pc){return {localIceUfrags:ufrags(pc.localDescription),remoteIceUfrags:ufrags(pc.remoteDescription),connectionState:pc.connectionState,iceConnectionState:pc.iceConnectionState,iceGatheringState:pc.iceGatheringState,signalingState:pc.signalingState,sctpState:pc.sctp?.state??null,dtlsState:pc.sctp?.transport?.state??null,localDescriptionType:pc.localDescription?.type??null,remoteDescriptionType:pc.remoteDescription?.type??null};}
 function announce(r,includeStats=false){try{emit(structuredClone({id:r.id,peer:r.peer,pipe:r.pipe,state:r.state,context:r.context,reason:r.lastReason,at:r.updatedAt,retired:r.retired,...(includeStats?{stats:r.stats,statsError:r.statsError}:{})}));}catch{}}
 function stats(pc,item,terminal=false){
  if(item.inFlight&&!terminal){item.pending=true;return;}
  if(item.inFlight>=2)return;
  if(typeof pc.getStats!=='function'){item.record.statsError='getStats unavailable';announce(item.record,true);return;}
  item.inFlight++;const serial=++item.statsSerial,requestedAt=now(),requestedReason=item.record.lastReason;let request;
  // Invoke before close(), even though the browser resolves the promise later.
  try{request=pc.getStats();}catch(e){item.record.statsError=String(e?.message??e).slice(0,512);item.inFlight--;announce(item.record,true);return;}
  Promise.resolve(request).then(report=>{
   if(!records.includes(item.record)||serial<item.appliedSerial)return;
   item.appliedSerial=serial;const rows=[];let count=0;
   report.forEach(row=>{if(!types.has(row.type))return;count++;if(rows.length>=maxStats)return;const out={};for(const k of fields)if(['string','number','boolean'].includes(typeof row[k]))out[k]=text(row[k]);rows.push(out);});
   item.record.stats={at:now(),requestedAt,requestedReason,rows,dropped:Math.max(0,count-rows.length)};
   delete item.record.statsError;announce(item.record,true);
  }).catch(e=>{if(records.includes(item.record)&&serial>=item.appliedSerial){item.record.statsError=String(e?.message??e).slice(0,512);announce(item.record,true);}}).finally(()=>{
   item.inFlight--;
   // Coalesce live state changes; retirement never starts a follow-up loop.
   if(!item.inFlight&&item.pending&&!item.record.retired){item.pending=false;item.requestedAt=now();stats(pc,item);}
  });
 }
 function track(pc,{peer,pipe,context=()=>({})}){
  const record={id:++serial,peer,pipe,createdAt:now(),retired:false,events:[],droppedEvents:0};
  tracked.set(pc,{record,context,inFlight:0,pending:false,statsSerial:0,appliedSerial:0});records.push(record);
  if(records.length>maxConnections){records.shift();droppedConnections++;}
  capture(pc,'created',false);return record.id;
 }
 function capture(pc,reason,withStats=true,detail){
  const item=tracked.get(pc);if(!item||item.record.retired||!records.includes(item.record))return;
  const r=item.record;r.state=summary(pc);r.context=item.context();r.lastReason=reason;r.updatedAt=now();
  const event={at:r.updatedAt,reason,state:{...r.state},context:{...r.context},...(detail?{detail}: {})};
  r.events.push(event);if(r.events.length>maxEvents){r.events.shift();r.droppedEvents++;}
  announce(r);if(withStats){item.requestedAt=now();stats(pc,item);}
 }
 function retire(pc,reason){if(!pc)return;const item=tracked.get(pc);if(!item||item.record.retired)return;capture(pc,'before_close:'+reason,false);item.record.retired=true;item.record.retirementReason=reason;stats(pc,item,true);}
 return {track,capture,retire,connectionId:pc=>tracked.get(pc)?.record.id??null,snapshot:()=>structuredClone({limits:{connections:maxConnections,eventsPerConnection:maxEvents,statsPerConnection:maxStats},droppedConnections,connections:records})};
}
