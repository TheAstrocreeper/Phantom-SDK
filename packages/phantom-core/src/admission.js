/** Developer-owned policy; credentials travel over discovery, never contain secrets. */
export class AdmissionError extends Error {
  constructor(message = 'Network authorization denied') {
    super(message); this.name = 'AdmissionError'; this.code = 'PHANTOM_ADMISSION_DENIED';
  }
}

function bytes(value, length) {
  if (typeof value !== 'string' || !new RegExp(`^[0-9a-f]{${length*2}}$`).test(value)) throw new AdmissionError('Invalid admission signature or key');
  return Uint8Array.from(value.match(/../g), pair => parseInt(pair,16));
}
export function admissionStatement(credential) {
  if (![1,2].includes(credential?.version) || !/^[0-9a-f]{64}$/.test(credential.networkId) ||
      !/^[0-9a-f]{64}$/.test(credential.peerPublicKey) || !Number.isSafeInteger(credential.expiresAt)) throw new AdmissionError('Invalid admission credential');
  if (credential.version === 2) {
    if (typeof credential.membershipId !== 'string' || !/^[0-9a-f-]{36}$/.test(credential.membershipId)) throw new AdmissionError('Invalid membership ID');
    return new TextEncoder().encode(JSON.stringify(['phantom-membership-v1',credential.networkId,credential.peerPublicKey,credential.membershipId,credential.expiresAt]));
  }
  return new TextEncoder().encode(JSON.stringify(['phantom-admission-v1',credential.networkId,credential.peerPublicKey,credential.expiresAt]));
}
/** Optional offline verification helper. The trust key comes from the app, not the joiner. */
export function createAdmissionVerifier({authorityPublicKey, crypto = globalThis.crypto} = {}) {
  const raw = bytes(authorityPublicKey,32);
  let key;
  return async ({networkId,peerPublicKey,credential}) => {
    try {
      if (credential?.networkId !== networkId || credential.peerPublicKey !== peerPublicKey) return false;
      const message = admissionStatement(credential);
      key ??= crypto.subtle.importKey('raw',raw,'Ed25519',false,['verify']);
      return await crypto.subtle.verify('Ed25519',await key,bytes(credential.signature,64),message);
    } catch { return false; }
  };
}

export function createAdmissionCheckpoint({policy, networkId, peerPublicKey, now = Date.now, timeoutMs = 5000}) {
  if (!policy || typeof policy.authorize !== 'function' || typeof policy.verify !== 'function')
    throw new TypeError('admission requires authorize and verify functions');
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new TypeError('Positive authorization timeout required');
  const approved = new Map();
  function credentialCopy(value) {
    // Expiry is enforced by Phantom in addition to the developer's policy.
    if (!value || !Number.isSafeInteger(value.expiresAt) || value.expiresAt <= now()) throw new AdmissionError('Missing or expired admission credential');
    const encoded = JSON.stringify(value);
    if (!encoded || encoded.length > 8192) throw new AdmissionError('Admission credential too large');
    return JSON.parse(encoded);
  }
  async function bounded(callback, input, signal) {
    if (signal?.aborted) throw new AdmissionError('Authorization aborted');
    const controller = new AbortController();
    let timer, abort;
    const stop = new Promise((_, reject) => {
      abort = () => { controller.abort(); reject(new AdmissionError('Authorization aborted')); };
      signal?.addEventListener('abort', abort, {once:true});
      timer = setTimeout(() => { controller.abort(); reject(new AdmissionError('Authorization timed out')); }, timeoutMs);
    });
    try { return await Promise.race([Promise.resolve().then(() => callback({...input, signal:controller.signal})), stop]); }
    finally { clearTimeout(timer); signal?.removeEventListener('abort', abort); }
  }
  async function verify(publicKey, value, signal) {
    if (!/^[0-9a-f]{64}$/.test(publicKey)) return false;
    try {
      const credential = credentialCopy(value);
      if (await bounded(policy.verify, {networkId, peerPublicKey:publicKey, credential}, signal) !== true) return false;
      credentialCopy(credential);
      approved.set(publicKey.slice(0,16), {publicKey, expiresAt:credential.expiresAt});
      return true;
    } catch { return false; }
  }
  return {
    async authorize({operation, signal} = {}) {
      const value = await bounded(policy.authorize, {networkId, peerPublicKey, operation}, signal);
      const credential = credentialCopy(value);
      if (!await verify(peerPublicKey, credential, signal)) throw new AdmissionError();
      return credential;
    },
    verify,
    allows(peerId) {
      const record = approved.get(peerId);
      return !!record && record.expiresAt > now();
    },
    forget(peerId) { approved.delete(peerId); },
  };
}
