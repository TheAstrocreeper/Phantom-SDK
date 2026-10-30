/** Private, bounded control envelopes. Never emit offers into diagnostics/chat. */
export function normalizeBulkControl(value, from, to) {
  try {
    if (!value || value.version !== 1 || !/^[0-9a-f]{32}$/.test(value.sessionId)) return null;
    const base = {version:1, type:value.type, sessionId:value.sessionId};
    let result, keys;
    if (value.type === 'request') {
      if (value.purpose !== 'probe' || !Number.isSafeInteger(value.bytes) || value.bytes < 1 || value.bytes > 2147483648) return null;
      result = {...base,purpose:'probe',bytes:value.bytes}; keys=['version','type','sessionId','purpose','bytes'];
    } else if (value.type === 'offer') {
      const offer=value.offer;
      if (!offer || offer.protocol !== 'phantom-bulk-quic-v1' || offer.sessionId !== value.sessionId ||
          !/^[0-9a-f]{16}$/.test(offer.from) || !/^[0-9a-f]{16}$/.test(offer.to) || offer.from===offer.to ||
          (from !== undefined && offer.from !== from) || (to !== undefined && offer.to !== to) ||
          typeof offer.certificateDer !== 'string' || !/^[0-9a-f]{2,8192}$/.test(offer.certificateDer) || offer.certificateDer.length%2 ||
          typeof offer.iceSdp !== 'string' || !offer.iceSdp.length || offer.iceSdp.length>4095 || !/^[\x20-\x7e\r\n]+$/.test(offer.iceSdp) ||
          Object.keys(offer).some(k=>!['protocol','sessionId','from','to','certificateDer','iceSdp'].includes(k))) return null;
      result={...base,offer:{protocol:offer.protocol,sessionId:offer.sessionId,from:offer.from,to:offer.to,certificateDer:offer.certificateDer,iceSdp:offer.iceSdp}};
      keys=['version','type','sessionId','offer'];
    } else if (value.type === 'cancel') {
      if (!['complete','closed','declined','busy','unavailable','failed','timeout'].includes(value.reason)) return null;
      result={...base,reason:value.reason}; keys=['version','type','sessionId','reason'];
    } else return null;
    if (Object.keys(value).some(key=>!keys.includes(key)) || JSON.stringify(result).length>16384) return null;
    return result;
  } catch { return null; }
}
