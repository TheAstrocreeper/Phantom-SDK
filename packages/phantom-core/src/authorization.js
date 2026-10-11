/** Admin proofs are independent of transport identities and elected roles. */
const hex = bytes => Array.from(bytes, b => b.toString(16).padStart(2, '0')).join('');
function bytes(value, length) {
  if (typeof value !== 'string' || !new RegExp(`^[0-9a-f]{${length * 2}}$`).test(value)) throw new TypeError('Invalid public key or signature');
  return Uint8Array.from(value.match(/../g), pair => parseInt(pair, 16));
}
function statement(record) {
  if (record?.version !== 1 || typeof record.networkId !== 'string' || !record.networkId.trim() || record.networkId.length>256 ||
      !['content.publish', 'content.revoke', 'network.retire'].includes(record.action) ||
      typeof record.target !== 'string' || !record.target.trim() || record.target.length>512 ||
      typeof record.operationId !== 'string' || !record.operationId.trim() || record.operationId.length>128) throw new TypeError('Invalid admin operation');
  if (record.action === 'network.retire' && record.target !== record.networkId) throw new TypeError('Retirement target must match networkId');
  return new TextEncoder().encode(JSON.stringify(['phantom-admin-v1', record.networkId, record.action, record.target, record.operationId]));
}
export async function createAdminIdentity({ crypto = globalThis.crypto } = {}) {
  const keys = await crypto.subtle.generateKey({name:'Ed25519'}, true, ['sign','verify']);
  const publicKey = hex(new Uint8Array(await crypto.subtle.exportKey('raw', keys.publicKey)));
  return adminSigner(keys.privateKey, publicKey, crypto);
}
function adminSigner(privateKey, publicKey, crypto) {
  return Object.freeze({ publicKey,
    async exportSeed() { return {version:1, publicKey, privateKey:hex(new Uint8Array(await crypto.subtle.exportKey('pkcs8',privateKey)))}; },
    async sign({networkId, action, target, operationId}) {
    const record = {version:1, networkId, action, target, operationId, publicKey};
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
      if (record?.networkId !== networkId || record.publicKey !== adminPublicKey) return null;
      const message = statement(record);
      const signature = bytes(record.signature, 64);
      const key = await crypto.subtle.importKey('raw', rawKey, 'Ed25519', false, ['verify']);
      if (!await crypto.subtle.verify('Ed25519', key, signature, message)) return null;
      // Return only signed fields; never forward unsigned role/permission metadata.
      return Object.freeze({version:1,networkId,action:record.action,target:record.target,operationId:record.operationId,publicKey:adminPublicKey,signature:record.signature});
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
