/** Shareable capability report. Raw stats, addresses and device fingerprints stay local. */
export function createCapabilityReport(observation, {protocols = [],maxChunkBytes=65536} = {}) {
 if (!observation || observation.version !== 1 || !Number.isInteger(maxChunkBytes) || maxChunkBytes<1024 || maxChunkBytes>262144 || !Array.isArray(protocols) || protocols.some(p => typeof p !== 'string' || !/^[a-z0-9][a-z0-9._-]{0,63}$/i.test(p)) || protocols.length > 32) throw new TypeError('Invalid scout observation or protocols');
 const value=observation.messageLimit;
 const messageLimit=value?.status==='known' && Number.isSafeInteger(value.bytes) && value.bytes>0 ? {status:'known',bytes:value.bytes} : value?.status==='unlimited' ? {status:'unlimited',bytes:null} : {status:'unknown',bytes:null};
 return {version:1,messageLimit,maxChunkBytes,protocols:[...new Set(protocols)].sort()};
}

/** Produce a proposal only; callers must negotiate it and validate each actual connection. */
export function planConnection({local,remote,defaultMessageBytes=65536,maxPayloadBytes=65536,framingBytes=0,protocolPreference=[]}={}) {
 if (!local || local.version!==1 || !remote || remote.version!==1 || !Array.isArray(local.protocols) || !Array.isArray(remote.protocols) || local.protocols.length>32 || remote.protocols.length>32 || [...local.protocols,...remote.protocols].some(p=>typeof p!=='string'||!/^[a-z0-9][a-z0-9._-]{0,63}$/i.test(p))) throw new TypeError('Two valid v1 capability reports required');
 if (![defaultMessageBytes,maxPayloadBytes].every(n=>Number.isSafeInteger(n)&&n>0) || !Number.isSafeInteger(framingBytes) || framingBytes<0 || !Array.isArray(protocolPreference) || protocolPreference.length>32 || protocolPreference.some(p=>typeof p!=='string')) throw new TypeError('Invalid planner bounds');
 const reasons=[], limits=[];
 for (const [side,report] of [['local',local],['remote',remote]]) {
  const limit=report.messageLimit;
  if(limit?.status==='known' && Number.isSafeInteger(limit.bytes) && limit.bytes>0) limits.push(limit.bytes);
  else if(limit?.status==='unlimited' && limit.bytes===null) {}
  else { limits.push(defaultMessageBytes);reasons.push(`${side}_message_limit_unknown`); }
 }
 const protocol=protocolPreference.find(p=>typeof p==='string' && local.protocols?.includes(p) && remote.protocols?.includes(p))??null;
 if(!protocol) reasons.push('no_common_protocol');
 if(!limits.length) reasons.push('message_size_default_for_unlimited');
 const messageBytes=limits.length?Math.min(...limits):defaultMessageBytes;
 const chunkCaps=[];
 for(const [side,report] of [['local',local],['remote',remote]]){
  if(Number.isInteger(report.maxChunkBytes)&&report.maxChunkBytes>=1024&&report.maxChunkBytes<=262144)chunkCaps.push(report.maxChunkBytes);
  else{chunkCaps.push(65536);reasons.push(`${side}_chunk_limit_unknown`);}
 }
 const payloadBytes=Math.max(0,Math.min(maxPayloadBytes,...chunkCaps,messageBytes-framingBytes));
 if(!payloadBytes) reasons.push('framing_exceeds_limit');
 else if(payloadBytes<1024) reasons.push('payload_below_minimum');
 return {version:1,status:protocol&&payloadBytes>=1024?'proposed':'unavailable',protocol,messageBytes,payloadBytes,framingBytes,reasons,scope:'observed_connections_only',requiresAgreement:true,validateTargetConnection:true};
}
