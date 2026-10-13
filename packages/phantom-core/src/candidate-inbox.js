// Candidates may precede their offer. No timers or transport allocation here.
export function createCandidateInbox({maxPeers=128,perPeer=64,total=1024}={}) {
  const peers=new Map();let count=0;
  return {
    put(peer,boot,attempt,candidate) {
      if(!peer||!boot||boot.length>256||!attempt||attempt.length>256||
         typeof candidate?.candidate!=="string"||candidate.candidate.length>4096)return false;
      const value={candidate:candidate.candidate};
      for(const field of ["sdpMid","usernameFragment"]) {
        const text=candidate[field];
        if(text!=null&&(typeof text!=="string"||text.length>256))return false;
        if(text!==undefined)value[field]=text;
      }
      if(candidate.sdpMLineIndex!==undefined) {
        const index=candidate.sdpMLineIndex;
        if(index!==null&&(!Number.isInteger(index)||index<0||index>65535))return false;
        value.sdpMLineIndex=index;
      }
      const rows=peers.get(peer);
      if(count>=total||(rows?.length||0)>=perPeer||(!rows&&peers.size>=maxPeers))return false;
      if(!rows)peers.set(peer,[]);
      peers.get(peer).push({boot,attempt,candidate:value});count++;return true;
    },
    take(peer,boot,attempt) {
      const rows=peers.get(peer)||[];
      peers.delete(peer);count-=rows.length;
      return rows.filter(row=>row.boot===boot&&row.attempt===attempt).map(row=>row.candidate);
    },
    drop(peer){const rows=peers.get(peer)||[];peers.delete(peer);count-=rows.length;},
    clear(){peers.clear();count=0;},
    snapshot(){return {earlyIceCandidates:count,earlyIcePeers:peers.size,earlyIceLimit:total};}
  };
}
