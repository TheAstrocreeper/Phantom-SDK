import { schnorr } from '@noble/curves/secp256k1.js';
import { ed25519 } from '@noble/curves/ed25519.js';
import { sha256 } from '@noble/hashes/sha2.js';
import { bytesToHex, hexToBytes } from '@noble/hashes/utils.js';
const utf8 = value => new TextEncoder().encode(value);
const binding = (room,key) => utf8(JSON.stringify(['phantom-nostr-binding-v1',room,key]));
function hash(e) { return bytesToHex(sha256(utf8(JSON.stringify([0,e.pubkey,e.created_at,e.kind,e.tags,e.content])))); }
export function createNostrIdentity() {
 const nostrSecret=schnorr.utils.randomSecretKey(), secret=ed25519.utils.randomSecretKey();
 const nostrKey=bytesToHex(schnorr.getPublicKey(nostrSecret)), publicKey=bytesToHex(ed25519.getPublicKey(secret));
 return Object.freeze({nostrKey,publicKey,
  sign(room,payload,{kind=20001,now=Date.now()}={}) {
   const content=JSON.stringify({...payload,protocol:'phantom-nostr-v1',roomId:room,s7pPublicKey:publicKey,
    identityBindingSig:bytesToHex(ed25519.sign(binding(room,nostrKey),secret)),nonce:crypto.randomUUID()});
   const e={pubkey:nostrKey,created_at:Math.floor(now/1000),kind,tags:[['t',room]],content};
   e.id=hash(e);e.sig=bytesToHex(schnorr.sign(hexToBytes(e.id),nostrSecret));return e;
  }
 });
}
export function verifyNostrEnvelope(event,room,{kind=20001,now=Date.now(),maxAgeMs=60000}={}) {
 try {
  if(!event || event.kind!==kind || !Number.isSafeInteger(event.created_at) || !Array.isArray(event.tags) ||
   !event.tags.every(t=>Array.isArray(t)&&t.every(v=>typeof v==='string')) || typeof event.content!=='string' || event.content.length>131072)return null;
  if(!/^[0-9a-f]{64}$/.test(event.pubkey)||!/^[0-9a-f]{64}$/.test(event.id)||!/^[0-9a-f]{128}$/.test(event.sig))return null;
  if(event.id!==hash(event) || !schnorr.verify(hexToBytes(event.sig),hexToBytes(event.id),hexToBytes(event.pubkey)))return null;
  if(!event.tags.some(t=>t[0]==='t'&&t[1]===room))return null;
  const age=now-event.created_at*1000;if(age < -10000 || age>maxAgeMs)return null;
  const p=JSON.parse(event.content);
  if(p.protocol!=='phantom-nostr-v1'||p.roomId!==room||!['ANNOUNCEMENT','SIGNAL'].includes(p.type)||typeof p.nonce!=='string')return null;
  if(!/^[0-9a-f]{64}$/.test(p.s7pPublicKey)||!/^[0-9a-f]{128}$/.test(p.identityBindingSig))return null;
  if(!ed25519.verify(hexToBytes(p.identityBindingSig),binding(room,event.pubkey),hexToBytes(p.s7pPublicKey)))return null;
  return {...p,nostrKey:event.pubkey,eventId:event.id};
 } catch {return null;}
}
