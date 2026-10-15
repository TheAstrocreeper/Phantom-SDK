/** Bounded signaling retained during a leadership handoff. Retention confers no authority. */
export function createDeferredSysOffers({now=Date.now,setTimeout=globalThis.setTimeout,clearTimeout=globalThis.clearTimeout,
 emit=()=>{},heldReason='leadership_not_committed',resumedReason='leadership_committed',maxPeers=128,maxCandidates=64,lifetimeMs=15000,maxSdpBytes=65536}={}) {
 const rows=new Map();
 const token=meta=>String(meta.bootId)+':'+String(meta.sysAttemptId);
 function note(action,row,reason){emit({action,kind:row.kind,peer:row.peer,attemptId:row.meta.sysAttemptId,bootId:row.meta.bootId,candidates:row.ice.length,reason});}
 function drop(peer,reason='discarded'){const row=rows.get(peer);if(!row)return;rows.delete(peer);clearTimeout(row.timer);note('dropped',row,reason);}
 return {
  has:peer=>rows.has(peer),
  hold(peer,offer,meta,kind='handoff'){
   if(!/^[0-9a-f]{16}$/.test(peer)||offer?.type!=='offer'||typeof offer.sdp!=='string'||new TextEncoder().encode(offer.sdp).length>maxSdpBytes||
      typeof meta?.room!=='string'||meta.room.length>256||typeof meta?.bootId!=='string'||!meta.bootId||meta.bootId.length>256||typeof meta.sysAttemptId!=='string'||!meta.sysAttemptId||meta.sysAttemptId.length>256)return false;
   let successorProof;
   if(meta.successorProof!==undefined){
    try{const wire=JSON.stringify(meta.successorProof);if(new TextEncoder().encode(wire).length>maxSdpBytes)return false;successorProof=JSON.parse(wire);}catch{return false;}
   }
   const previous=rows.get(peer),nextToken=token(meta);
   if(previous){
    if(previous.retired.has(nextToken))return false;
    if(token(previous.meta)===nextToken)return previous.offer.sdp===offer.sdp;
   }else if(rows.size>=maxPeers)return false;
   const retired=new Set(previous?.retired??[]);if(previous){retired.add(token(previous.meta));while(retired.size>16)retired.delete(retired.values().next().value);drop(peer,'superseded');}
   const row={peer,kind,offer:{type:'offer',sdp:offer.sdp},meta:{room:meta.room,pipe:'sys',bootId:meta.bootId,sysAttemptId:meta.sysAttemptId,sysCarrier:meta.sysCarrier===true,recoveryPeerPipe:meta.recoveryPeerPipe===true,...(successorProof?{successorProof}:{})},ice:[],seen:new Set(),retired,receivedAt:now(),timer:null};
   row.timer=setTimeout(()=>{if(rows.get(peer)===row)drop(peer,'expired');},lifetimeMs);
   rows.set(peer,row);note('held',row,kind==='successor'?'old_leader_pipe_open':heldReason);return true;
  },
  ice(peer,candidate,meta){
   const row=rows.get(peer);if(!row)return 'missing';
   if(meta?.bootId!==row.meta.bootId||meta?.sysAttemptId!==row.meta.sysAttemptId)return 'mismatch';
   const c=typeof candidate?.toJSON==='function'?candidate.toJSON():candidate;
   if(!c||typeof c.candidate!=='string'||c.candidate.length>2048||
      (c.sdpMid!=null&&(typeof c.sdpMid!=='string'||c.sdpMid.length>64))||
      (c.sdpMLineIndex!=null&&(!Number.isInteger(c.sdpMLineIndex)||c.sdpMLineIndex<0||c.sdpMLineIndex>64))||
      (c.usernameFragment!=null&&(typeof c.usernameFragment!=='string'||c.usernameFragment.length>256)))return 'invalid';
   const clean={candidate:c.candidate,sdpMid:c.sdpMid??null,sdpMLineIndex:c.sdpMLineIndex??null,usernameFragment:c.usernameFragment??null};
   const key=JSON.stringify(clean);if(row.seen.has(key))return 'duplicate';
   if(row.ice.length>=maxCandidates)return 'full';
   row.seen.add(key);row.ice.push({candidate:clean,meta:{...row.meta}});return 'held';
  },
  resume({isLeader,eligible,accept,waiting=()=>false}){
   for(const [peer,row]of rows){
    if(!isLeader())return;
    if(now()-row.receivedAt>=lifetimeMs){drop(peer,'expired');continue;}
    if(!eligible(row)){drop(peer,'eligibility_changed');continue;}
    if(waiting(row))continue;
    rows.delete(peer);clearTimeout(row.timer);note('resumed',row,row.kind==='successor'?'old_leader_pipe_lost':resumedReason);accept(row);
   }
  },
  drop,
  clear(reason='room_closed'){for(const peer of rows.keys())drop(peer,reason);},
  snapshot:()=>[...rows.values()].map(row=>({peer:row.peer,kind:row.kind,attemptId:row.meta.sysAttemptId,bootId:row.meta.bootId,receivedAt:row.receivedAt,expiresAt:row.receivedAt+lifetimeMs,candidates:row.ice.length}))
 };
}
