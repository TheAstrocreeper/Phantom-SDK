/** One on-demand, privacy-limited observation; never creates or changes a connection. */
export async function scoutConnection(pc, {channel, runtime = globalThis} = {}) {
  if (!pc || typeof pc !== 'object') throw new TypeError('Supply a peer connection');
  const number = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;
  const state = value => typeof value === 'string' ? value : null;
  const limit = value => value === Infinity ? {status:'unlimited',bytes:null} : Number.isFinite(value) && value >= 0 ? {status:'known',bytes:value} : {status:'unknown',bytes:null};
  const sctp = pc.sctp;
  const result = {
    version: 1,
    connection: {state:state(pc.connectionState), iceState:state(pc.iceConnectionState), sctpState:state(sctp?.state)},
    messageLimit: limit(sctp?.maxMessageSize),
    maxChannels: number(sctp?.maxChannels),
    channel: channel ? {state:state(channel.readyState), ordered:typeof channel.ordered === 'boolean' ? channel.ordered : null, bufferedBytes:number(channel.bufferedAmount), lowThresholdBytes:number(channel.bufferedAmountLowThreshold), maxRetransmits:number(channel.maxRetransmits), maxPacketLifeTime:number(channel.maxPacketLifeTime)} : null,
    features: {webCrypto:typeof runtime.crypto?.subtle?.digest === 'function', blob:typeof runtime.Blob === 'function', streams:typeof runtime.WritableStream === 'function'},
    route: null,
    statsStatus: 'unavailable',
  };
  if (typeof pc.getStats !== 'function') return result;
  try {
    const report = await pc.getStats();
    const transports = [];
    report.forEach(row => { if (row.type === 'transport' && row.selectedCandidatePairId) transports.push(row); });
    // Do not guess which transport carries SCTP if multiple routes are present.
    const pair = transports.length === 1 && report.get(transports[0].selectedCandidatePairId);
    const local = pair && report.get(pair.localCandidateId), remote = pair && report.get(pair.remoteCandidateId);
    if (pair) result.route = {localType:state(local?.candidateType), remoteType:state(remote?.candidateType), protocol:state(local?.protocol), rttSeconds:number(pair.currentRoundTripTime), bytesSent:number(pair.bytesSent), bytesReceived:number(pair.bytesReceived)};
    result.statsStatus = 'available';
  } catch { result.statsStatus = 'error'; }
  return result;
}
