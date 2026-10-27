/** Validate actual data-channel messages, including any transport wrapper bytes. */
export function fitsMessageLimits({connections,payloadBytes,headerBytes,wrapperBytes=0,unknownPayloadLimit=65536}) {
 if(!Array.isArray(connections)||!connections.length||![payloadBytes,headerBytes,wrapperBytes,unknownPayloadLimit].every(x=>Number.isSafeInteger(x)&&x>=0))return false;
 return connections.every(pc=>{
  const limit=pc?.sctp?.maxMessageSize;
  const known=typeof limit==='number'&&(Number.isFinite(limit)||limit===Infinity)&&limit>0;
  return known?payloadBytes+wrapperBytes<=limit&&headerBytes+wrapperBytes<=limit:payloadBytes<=unknownPayloadLimit;
 });
}

/** Smallest usable message payload across the actual target connections. */
export function maxMessagePayload(connections,wrapperBytes=0,unknownLimit=65536) {
 if(!Array.isArray(connections)||!connections.length||!Number.isSafeInteger(wrapperBytes)||wrapperBytes<0)return 0;
 return Math.max(0,Math.min(...connections.map(pc=>{const n=pc?.sctp?.maxMessageSize;return typeof n==='number'&&(Number.isFinite(n)||n===Infinity)&&n>0?n:unknownLimit;}))-wrapperBytes);
}
