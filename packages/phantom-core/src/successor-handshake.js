/** Eligibility to open a recovery pipe only. Does not commit leadership. */
export function acceptsSuccessorHandshake({proof,base,sender,self,room,leaderPipeOpen,bootId}) {
 if(!proof||!base||leaderPipeOpen||!bootId||sender===self)return false;
 const keys=rows=>Array.isArray(rows)?rows.map(r=>r.hKey||r.from).sort().join(','):'';
 return proof.room===room && proof.from===sender && proof.to===self && proof.bootId===bootId &&
  sender===base.syscon2 && proof.baseOne===base.syscon1 && proof.baseTwo===base.syscon2 &&
  proof.baseEpoch===base.epoch && proof.baseSeq===base.rosterSeq &&
  keys(proof.baseRoster)===keys(base.roster) && base.roster.some(r=>(r.hKey||r.from)===self) &&
  proof.terminalLeader===base.syscon1;
}
