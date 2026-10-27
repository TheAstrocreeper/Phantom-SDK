/** Validate actual data-channel messages, including any transport wrapper bytes. */
export function fitsMessageLimits({connections,payloadBytes,headerBytes,wrapperBytes=0,unknownPayloadLimit=65536}) {
 if(!Array.isArray(connections)||!connections.length||![payloadBytes,headerBytes,wrapperBytes,unknownPayloadLimit].every(x=>Number.isSafeInteger(x)&&x>=0))return false;
 return connections.every(pc=>{
  const limit=pc?.sctp?.maxMessageSize;
  const known=typeof limit==='number'&&(Number.isFinite(limit)||limit===Infinity)&&limit>0;
  return known?payloadBytes+wrapperBytes<=limit&&headerBytes+wrapperBytes<=limit:payloadBytes<=unknownPayloadLimit;
 });
}
