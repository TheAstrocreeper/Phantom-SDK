/** Accept the promoted historical mirror over the recovery pipe, never a doorway claim. */
export function acceptsSuccessorStatus({base,message,sender,self,recoveryTarget,leaderPipeOpen}){
 if(!base||!message||!Array.isArray(base.roster)||!Array.isArray(message.roster)||leaderPipeOpen||!sender||sender!==base.syscon2||sender!==recoveryTarget||self===sender)return false;
 if(message.from!==sender||message.syscon1!==sender||message.role!=='syscon1'||message.room!==base.room||Number(message.epoch)<=Number(base.epoch)||!(Number(message.rosterSeq)>0))return false;
 const previous=(base.roster??[]).map(r=>r.hKey).filter(k=>k!==base.syscon1).sort();
 const next=(message.roster??[]).map(r=>r.hKey).sort();
 return previous.includes(self)&&next.length===previous.length&&next.every((k,i)=>k===previous[i])&&
  !!message.syscon2&&message.syscon2!==sender&&next.includes(message.syscon2);
}
