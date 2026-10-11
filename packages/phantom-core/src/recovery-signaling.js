const types=new Set(['rtc_offer','rtc_answer','rtc_ice']);
// Transport eligibility only. Offer ownership and successor proofs stay in core.
export function canCarrySysRecovery({packet,room,self,remote,member,terminal,accepted,ready,current=true}) {
 return !!(current&&member&&!terminal&&accepted&&ready&&packet&&types.has(packet.type)&&
  packet.videoId===room&&packet.from===self&&packet.to===remote&&
  packet.pipe==='sys'&&packet.sysCarrier===true&&typeof packet.bootId==='string'&&packet.bootId);
}
