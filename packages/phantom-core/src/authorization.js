/** Admin proofs are independent of transport identities and elected roles. */
import {membershipRevocationStatement} from './membership-proof.js';
import {admissionStatement} from './admission.js';
const hex = bytes => Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
function bytes(value, length) {
  if (typeof value !== 'string' || !new RegExp(`^[0-9a-f]{${length * 2}}$`).test(value)) throw new TypeError('Invalid public key or signature');
  return Uint8Array.from(value.match(/../g), pair => parseInt(pair, 16));
}
function statement(record) {
  if (![1,2].includes(record?.version) || typeof record.networkId !== 'string' || !record.networkId.trim() || record.networkId.length>256 ||
      !['content.publish', 'content.revoke', 'network.retire'].includes(record.action) ||
      typeof record.target !== 'string' || !record.target.trim() || record.target.length>512 ||
      typeof record.operationId !== 'string' || !record.operationId.trim() || record.operationId.length>128) throw new TypeError('Invalid admin operation');
  if (record.action === 'network.retire' && record.target !== record.networkId) throw new TypeError('Retirement target must match networkId');
  const fields=[record.networkId,record.action,record.target,record.operationId];
  if(record.version===2){
    if(record.action!=='content.publish'||!Array.isArray(record.recipients)||record.recipients.length>128||
        record.recipients.some((id,i)=>typeof id!=='string'||! /^[0-9a-f]{16}$/.test(id)||(i>0&&record.recipients[i-1]>=id)))throw new TypeError('Invalid publication recipients');
    return new TextEncoder().encode(JSON.stringify(['phantom-admin-v2',...fields,record.recipients]));
  }
  return new TextEncoder().encode(JSON.stringify(['phantom-admin-v1',...fields]));
}
export async function createAdminIdentity({ crypto = globalThis.crypto } = {}) {
  const keys = await crypto.subtle.generateKey({name:'Ed25519'}, true, ['sign','verify']);
  const publicKey = hex(new Uint8Array(await crypto.subtle.exportKey('raw', keys.publicKey)));
  return adminSigner(keys.privateKey, publicKey, crypto);
}
function adminSigner(privateKey, publicKey, crypto) {
  return Object.freeze({ publicKey,
    async signAdmission({networkId,peerPublicKey,expiresAt}) {
      const credential = {version:1,networkId,peerPublicKey,expiresAt};
      const signature = hex(new Uint8Array(await crypto.subtle.sign('Ed25519',privateKey,admissionStatement(credential))));
      return Object.freeze({...credential,signature});
    },
    async signMembership({networkId,peerPublicKey,membershipId,expiresAt}) {
      const credential = {version:2,networkId,peerPublicKey,membershipId,expiresAt};
      const signature = hex(new Uint8Array(await crypto.subtle.sign('Ed25519',privateKey,admissionStatement(credential))));
      return Object.freeze({...credential,signature});
    },
    async signMembershipRevocation({networkId,peerPublicKey,membershipId,reason}) {
      const record={version:1,networkId,peerPublicKey,membershipId,reason};
      const signature=hex(new Uint8Array(await crypto.subtle.sign('Ed25519',privateKey,membershipRevocationStatement(record))));
      return Object.freeze({...record,signature});
    },
    async exportSeed() { return {version:1, publicKey, privateKey:hex(new Uint8Array(await crypto.subtle.exportKey('pkcs8',privateKey)))}; },
    async sign({networkId, action, target, operationId, recipients}) {
    if(recipients!==undefined){
      if(!Array.isArray(recipients)||recipients.length>128)throw new TypeError('Invalid publication recipients');
      recipients=Object.freeze([...new Set(recipients)].sort());
    }
    const record = {version:recipients===undefined?1:2, networkId, action, target, operationId, publicKey,...(recipients===undefined?{}:{recipients})};
    const signature = hex(new Uint8Array(await crypto.subtle.sign('Ed25519', privateKey, statement(record))));
    return Object.freeze({...record, signature});
  }});
}
/** A fixed developer-configured trust anchor. Verification does not execute operations. */
export function createAdminAuthorization({networkId, adminPublicKey, crypto = globalThis.crypto} = {}) {
  if (typeof networkId !== 'string' || !networkId.trim()) throw new TypeError('networkId required');
  const rawKey = bytes(adminPublicKey, 32);
  return Object.freeze({networkId, adminPublicKey, async verify(record) {
    try {
      // Freeze the verification input logically before asynchronous crypto work.
      record={...record,...(Array.isArray(record?.recipients)?{recipients:[...record.recipients]}:{})};
      if (record?.networkId !== networkId || record.publicKey !== adminPublicKey) return null;
      const message = statement(record);
      const signature = bytes(record.signature, 64);
      const key = await crypto.subtle.importKey('raw', rawKey, 'Ed25519', false, ['verify']);
      if (!await crypto.subtle.verify('Ed25519', key, signature, message)) return null;
      // Return only signed fields; never forward unsigned role/permission metadata.
      return Object.freeze({version:record.version,networkId,action:record.action,target:record.target,operationId:record.operationId,publicKey:adminPublicKey,signature:record.signature,...(record.version===2?{recipients:Object.freeze([...record.recipients])}:{})});
    } catch { return null; }
  }});
}

export async function importAdminIdentity(seed, {crypto=globalThis.crypto}={}) {
  if (seed?.version !== 1) throw new TypeError('Invalid admin seed');
  const publicBytes=bytes(seed.publicKey,32);
  const privateKey=await crypto.subtle.importKey('pkcs8',bytes(seed.privateKey,48),'Ed25519',true,['sign']);
  const publicKey=await crypto.subtle.importKey('raw',publicBytes,'Ed25519',false,['verify']);
  const check=new TextEncoder().encode('phantom-admin-seed-check');
  if (!await crypto.subtle.verify('Ed25519',publicKey,await crypto.subtle.sign('Ed25519',privateKey,check),check)) throw new TypeError('Admin seed keys do not match');
  return adminSigner(privateKey,seed.publicKey,crypto);
}
