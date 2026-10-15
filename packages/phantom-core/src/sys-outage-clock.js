/** Connection-local evidence; observing disconnection never authorizes removal. */
export function createSysOutageClock({now=Date.now}={}) {
  const starts=new WeakMap();
  function observe(pc) {
    if (!pc) return;
    const connection=pc.connectionState, ice=pc.iceConnectionState;
    // Aggregate connected state establishes restoration even if an ICE
    // callback still exposes the previous disconnected state.
    if (connection==='connected' || ((ice==='connected'||ice==='completed') && connection!=='disconnected' && connection!=='failed')) {
      starts.delete(pc);
    } else if (connection==='disconnected' || ice==='disconnected') {
      if (!starts.has(pc)) starts.set(pc,now());
    }
  }
  function confirmedStart(pc) {
    if (!pc || pc.connectionState==='closed' || pc.iceConnectionState==='closed' ||
        (pc.connectionState!=='failed' && pc.iceConnectionState!=='failed')) return null;
    return starts.get(pc) ?? null;
  }
  return {observe,confirmedStart};
}
