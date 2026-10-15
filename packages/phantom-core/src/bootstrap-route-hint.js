/** Correlated routing advice for a pending bootstrap attempt. Never commits authority. */
export function acceptsBootstrapRouteHint({packet,room,self,bootId,role,committed,expectedPeer,attemptId,awaitingAnswer,
 livePeers,visited,knownResponderBootId}) {
 const peer=value=>typeof value==='string'&&/^[0-9a-f]{16}$/.test(value);
 return !!(packet&&packet.type==='rtc_route_hint'&&packet.pipe==='sys'&&role==='peer'&&!committed&&awaitingAnswer&&
  packet.videoId===room&&packet.to===self&&packet.from===expectedPeer&&peer(packet.from)&&
  packet.sysAttemptId===attemptId&&typeof attemptId==='string'&&attemptId&&packet.requestBootId===bootId&&bootId&&
  (!knownResponderBootId||packet.bootId===knownResponderBootId)&&
  peer(packet.leader)&&packet.leader!==self&&packet.leader!==expectedPeer&&
  livePeers.has(packet.from)&&livePeers.has(packet.leader)&&!visited.has(packet.leader));
}
