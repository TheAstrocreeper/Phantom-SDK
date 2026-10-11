/** An in-flight SCTP channel is not failed merely because SDP became stable. */
export function sysBirthDisposition(pc,dc) {
 if(['failed','closed'].includes(pc?.connectionState)||['failed','closed'].includes(pc?.iceConnectionState)||pc?.signalingState==='closed')return 'failed';
 if(dc?.readyState==='open')return 'open';
 if(dc?.readyState==='connecting' && (pc?.localDescription||pc?.remoteDescription))return 'waiting';
 return 'create';
}
export function isCurrentSysBirth(registry,key,entry) {return registry.get(key)===entry && entry?.pc?.signalingState!=='closed';}

/** Keep one birth operation per peer, independent of mutable routing/dial flags. */
export function runSysBirth(flights,key,operation) {
 const existing=flights.get(key);
 if(existing)return existing;
 const flight=Promise.resolve().then(operation).finally(()=>{
  if(flights.get(key)===flight)flights.delete(key);
 });
 flights.set(key,flight);
 return flight;
}

/** Successful setup stays complete even if that channel is lost later. */
export function completeSysBirth(entry) {
 entry.sdkSysBirthCompleted=true;
 const deadline=entry.sdkSysBirthDeadline;
 if(deadline){deadline.state='completed';deadline.cancel(deadline.timer);entry.sdkSysBirthDeadline=null;}
}
export function armSysBirthDeadline(entry,{schedule,cancel},delay,onExpire) {
 if(entry.sdkSysBirthCompleted)return;
 const old=entry.sdkSysBirthDeadline;
 if(old){old.state='retired';old.cancel(old.timer);}
 const deadline={state:'waiting',cancel,timer:null};
 entry.sdkSysBirthDeadline=deadline;
 deadline.timer=schedule(()=>{
  if(deadline.state!=='waiting'||entry.sdkSysBirthDeadline!==deadline)return;
  deadline.state='expired';entry.sdkSysBirthDeadline=null;
  onExpire();
 },delay);
}
