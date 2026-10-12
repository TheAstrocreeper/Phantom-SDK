import {ed25519} from '@noble/curves/ed25519.js';
import {hexToBytes} from '@noble/hashes/utils.js';

// Sort object keys so transport serialization cannot alter signature semantics.
export function canonical(value) {
 if(value===null||typeof value!=='object')return JSON.stringify(value);
 if(Array.isArray(value))return '['+value.map(canonical).join(',')+']';
 return '{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+canonical(value[key])).join(',')+'}';
}
export function peerProofStatement(proof) {
 return new TextEncoder().encode(canonical(['phantom-peer-admission-v1',proof.networkId,proof.peerPublicKey,proof.kind,proof.credential,proof.payload,proof.issuedAt,proof.nonce]));
}
export function verifyPeerProof(proof,{networkId,kind,payload,now=Date.now()}={}) {
 try {
  if(proof?.version!==1||proof.networkId!==networkId||proof.kind!==kind||
    !/^[0-9a-f]{64}$/.test(proof.peerPublicKey)||!/^[0-9a-f]{128}$/.test(proof.signature)||
    !Number.isSafeInteger(proof.issuedAt)||proof.issuedAt>now+10000||
    typeof proof.nonce!=='string'||proof.nonce.length>128||!proof.nonce||
    (kind!=='announce'&&now-proof.issuedAt>60000))return false;
  if(payload!==undefined&&canonical(payload)!==canonical(proof.payload))return false;
  const message=peerProofStatement(proof);
  if(message.length>131072)return false;
  return ed25519.verify(hexToBytes(proof.signature),message,hexToBytes(proof.peerPublicKey));
 }catch{return false;}
}
