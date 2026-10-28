const TRANSPORT_COUNTERS = ['packetsSent','packetsReceived','bytesSent','bytesReceived','packetsDiscardedOnSend','bytesDiscardedOnSend'];
const counter = value => Number.isSafeInteger(value) && value >= 0 ? value : null;

/** Counters describe the selected ICE pair, not kernel receive losses. Missing
 * values and discontinuities must never turn into a reassuring zero. */
export function compareScoutTransport(previous, current) {
  const reason = !previous || !current ? 'missing_observation'
    : !previous.transportId || !current.transportId || !previous.pairId || !current.pairId ? 'route_identity_unavailable'
    : previous.transportId !== current.transportId || previous.pairId !== current.pairId ? 'selected_route_changed'
    : !Number.isFinite(previous.timestampMs) || !Number.isFinite(current.timestampMs) || current.timestampMs <= previous.timestampMs ? 'non_increasing_stats_timestamp' : null;
  const counters = Object.fromEntries(TRANSPORT_COUNTERS.map(name => {
    const before = counter(previous?.[name]), after = counter(current?.[name]);
    return [name, reason ? {status:'unknown',value:null,reason}
      : before === null || after === null ? {status:'unknown',value:null,reason:'counter_unavailable'}
      : after < before ? {status:'unknown',value:null,reason:'counter_reset'}
      : {status:'observed',value:after-before}];
  }));
  return {scope:'selected_ice_candidate_pair',elapsedMs:reason ? null : current.timestampMs-previous.timestampMs,counters,
    kernelReceiveDrops:{status:'unavailable',reason:'not_exposed_by_browser_webrtc_api'},
    socketReceiveBufferBytes:{status:'unavailable',reason:'not_exposed_by_browser_webrtc_api'}};
}

/** One on-demand observation; raw stats can contain network identifiers and stay local. */
export async function scoutConnection(pc, {channel, runtime = globalThis} = {}) {
  if (!pc || typeof pc !== 'object') throw new TypeError('Supply a peer connection');
  const number = value => typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;
  const state = value => typeof value === 'string' ? value : null;
  const limit = value => value === Infinity ? {status:'unlimited',bytes:null} : Number.isFinite(value) && value >= 0 ? {status:'known',bytes:value} : {status:'unknown',bytes:null};
  const sctp = pc.sctp;
  const result = {
    version: 1,
    connection: {state:state(pc.connectionState), iceState:state(pc.iceConnectionState), iceGatheringState:state(pc.iceGatheringState), signalingState:state(pc.signalingState), sctpState:state(sctp?.state), dtlsState:state(sctp?.transport?.state)},
    messageLimit: limit(sctp?.maxMessageSize),
    maxChannels: number(sctp?.maxChannels),
    channel: channel ? {state:state(channel.readyState), ordered:typeof channel.ordered === 'boolean' ? channel.ordered : null, bufferedBytes:number(channel.bufferedAmount), lowThresholdBytes:number(channel.bufferedAmountLowThreshold), maxRetransmits:number(channel.maxRetransmits), maxPacketLifeTime:number(channel.maxPacketLifeTime)} : null,
    features: {webCrypto:typeof runtime.crypto?.subtle?.digest === 'function', blob:typeof runtime.Blob === 'function', streams:typeof runtime.WritableStream === 'function', indexedDB:!!runtime.indexedDB, fileSystemAccess:typeof runtime.showSaveFilePicker === 'function', serviceWorker:!!runtime.navigator?.serviceWorker},
    environment: {userAgent:state(runtime.navigator?.userAgent), platform:state(runtime.navigator?.platform), languages:Array.isArray(runtime.navigator?.languages)?runtime.navigator.languages.filter(x=>typeof x==='string').slice(0,32):null, hardwareConcurrency:number(runtime.navigator?.hardwareConcurrency), deviceMemoryGiB:number(runtime.navigator?.deviceMemory), secureContext:typeof runtime.isSecureContext==='boolean'?runtime.isSecureContext:null, network:runtime.navigator?.connection?{effectiveType:state(runtime.navigator.connection.effectiveType), downlinkMbps:number(runtime.navigator.connection.downlink), rttMs:number(runtime.navigator.connection.rtt), saveData:typeof runtime.navigator.connection.saveData==='boolean'?runtime.navigator.connection.saveData:null}:null},
    route: null,
    transportCounters: null,
    statsStatus: 'unavailable',
    stats: {rows:[], total:0, truncated:false, containsNetworkIdentifiers:true},
  };
  if (typeof pc.getStats !== 'function') return result;
  try {
    const report = await pc.getStats();
    const transports = [];
    report.forEach(row => {
      result.stats.total++;
      if (result.stats.rows.length < 512) {
        // RTCStats are structured data; serialize to detach from the live report.
        try { result.stats.rows.push(JSON.parse(JSON.stringify(row))); } catch { result.stats.rows.push({id:state(row.id),type:state(row.type),serializationError:true}); }
      }
      if (row.type === 'transport' && row.selectedCandidatePairId) transports.push(row);
    });
    result.stats.truncated = result.stats.total > result.stats.rows.length;
    // Do not guess which transport carries SCTP if multiple routes are present.
    const pair = transports.length === 1 && report.get(transports[0].selectedCandidatePairId);
    const local = pair && report.get(pair.localCandidateId), remote = pair && report.get(pair.remoteCandidateId);
    if (pair) {
      result.route = {localType:state(local?.candidateType), remoteType:state(remote?.candidateType), protocol:state(local?.protocol), rttSeconds:number(pair.currentRoundTripTime), bytesSent:number(pair.bytesSent), bytesReceived:number(pair.bytesReceived)};
      result.transportCounters = {transportId:state(transports[0].id),pairId:state(transports[0].selectedCandidatePairId),timestampMs:number(pair.timestamp),
        ...Object.fromEntries(TRANSPORT_COUNTERS.map(name=>[name,counter(pair[name])]))};
    }
    result.statsStatus = 'available';
  } catch { result.statsStatus = 'error'; }
  return result;
}
