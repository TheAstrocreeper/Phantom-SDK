// Code vocabulary is derived from SDK source at build time. Never export raw text.
export function createRecoveryProjector(codes=[]) {
 const known=new Set([...codes,"new","checking","connected","completed","disconnected","failed","closed","stable","have-local-offer","have-remote-offer","have-local-pranswer","have-remote-pranswer"]),prefixes=[...known].filter(c=>c.endsWith('_')).sort((a,b)=>b.length-a.length);
 function code(value){if(value==null||value==='')return 'none';const text=String(value);if(known.has(text))return text;const head=text.split(/[: |]/)[0];if(known.has(head))return head;const prefix=prefixes.find(p=>text.startsWith(p));return prefix?prefix+'[detail omitted]':'unrecognized';}
 const number=value=>Number.isFinite(value)?value:0;
 return function project(snapshot={},state={},now=Date.now()){
  const c=snapshot.concierge??{},b=snapshot.bootstrap??{},a=c.sysAttempt??{},m=c.membershipAudit?.topology??{};
  return {
   state:code(state.state),reason:code(state.reason),nextAction:code(state.nextAction),role:code(state.role),admitted:state.admitted===true,
   resources:{earlyIceCandidates:number(snapshot.recoveryResources?.earlyIceCandidates),earlyIcePeers:number(snapshot.recoveryResources?.earlyIcePeers),queuedSignals:number(snapshot.recoveryResources?.queuedSignals),activeQueues:number(snapshot.recoveryResources?.activeQueues),availabilityReceipts:number(snapshot.recoveryResources?.availabilityReceipts),pendingAvailability:number(snapshot.recoveryResources?.pendingAvailability),ownedTimeouts:number(snapshot.runtimeResources?.ownedTimeouts),ownedIntervals:number(snapshot.runtimeResources?.ownedIntervals)},
   coreEvent:code(snapshot.reason),doorway:{state:code(b.doorwayState),fault:code(b.doorwayFault),attempts:number(b.attempts),phase:code(b.doorwayDeadlinePhase)},
   system:{ready:c.sysReady===true,pipe:code(c.sysPipeState),unavailable:code(c.sysUnavailableReason),fault:code(c.sysLastErr),attempt:{state:code(a.state),event:code(a.lastEvent),fault:code(a.faultReason),recoveryCount:number(a.recoveryCount),ageMs:a.startedAt?Math.max(0,now-a.startedAt):0}},
   systemTransports:(snapshot.sysPairs??[]).slice(0,8).map(p=>({pc:code(p.pcState),ice:code(p.iceState),signaling:code(p.signalingState),channel:code(p.dcState),attempt:code(p.attemptState),pendingRemoteIce:number(p.pendingRemoteIce)})),
   membership:{committedCount:number(m.committedCount),pendingCount:number(m.pendingCount),terminalCount:String(m.terminalKeys??'').split(',').filter(k=>k&&k!=='none').length,epoch:number(m.committedEpoch),stage:code(c.membershipAudit?.stage)},
   chat:(snapshot.chatPairs??[]).slice(0,8).map(p=>({relationship:code(p.relationship),owner:p.offerOwner===true,pc:code(p.pcState),ice:code(p.iceState),signaling:code(p.signalingState),channel:code(p.dcState),state:code(p.chatState),fault:code(p.chatFault),reconnecting:p.reconnecting===true}))
  };
 };
}
