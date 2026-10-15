/** Accept the promoted historical mirror over the recovery pipe, never a doorway claim. */
export function acceptsSuccessorStatus({base,message,sender,self,recoveryTarget,leaderPipeOpen}){
 if(!base||!message||!Array.isArray(base.roster)||!Array.isArray(message.roster)||leaderPipeOpen||!sender||sender!==base.syscon2||sender!==recoveryTarget||self===sender)return false;
 if(message.from!==sender||message.syscon1!==sender||message.role!=='syscon1'||message.room!==base.room||Number(message.epoch)<=Number(base.epoch)||!(Number(message.rosterSeq)>0))return false;
 const previous=(base.roster??[]).map(r=>r.hKey).filter(k=>k!==base.syscon1).sort();
 const next=(message.roster??[]).map(r=>r.hKey).sort();
 // The successor may already have admitted new peers before a survivor
 // reconnects. Keep every historical survivor and exclude the departed
 // leader; added members do not invalidate the trusted mirror's status.
 return previous.includes(self)&&new Set(next).size===next.length&&
  next.every(k=>typeof k==='string'&&/^[0-9a-f]{16}$/.test(k))&&
  !next.includes(base.syscon1)&&previous.every(k=>next.includes(k))&&
  !!message.syscon2&&message.syscon2!==sender&&next.includes(message.syscon2);
}
