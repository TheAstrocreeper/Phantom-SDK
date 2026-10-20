import {AdmissionError, createAdmissionVerifier} from './admission.js';

const hex = value => Array.from(value, b => b.toString(16).padStart(2, '0')).join('');
const validKey = key => typeof key === 'string' && /^[0-9a-f]{64}$/.test(key);
const clone = value => structuredClone(value);

/** Single authoritative writer. transaction must durably and atomically commit before resolving.
 * Never run separate authorities over non-transactional read/write storage.
 */
export async function createMembershipAuthority({networkId, adminIdentity, store,
  verifyPeerRequest, crypto = globalThis.crypto, now = Date.now} = {}) {
  if (!validKey(networkId) || !validKey(adminIdentity?.publicKey) || typeof adminIdentity.signMembership !== 'function' || typeof adminIdentity.signMembershipRevocation !== 'function')
    throw new TypeError('Network ID and membership-capable admin identity required');
  if (typeof store?.transaction !== 'function') throw new TypeError('Atomic durable membership store required');
  if (typeof verifyPeerRequest !== 'function') throw new TypeError('Authenticated peer request verifier required');
  const listeners=new Set();
  const notify=record=>{for(const fn of listeners)try{fn(clone(record));}catch{}};
  const verifier = createAdmissionVerifier({authorityPublicKey:adminIdentity.publicKey, crypto});
  const scope = `phantom-membership-v1:${networkId}:${adminIdentity.publicKey}`;
  const digest = async token => {
    if (typeof token !== 'string' || !/^[0-9a-f]{64}$/.test(token)) throw new AdmissionError('Invalid invitation token');
    return hex(new Uint8Array(await crypto.subtle.digest('SHA-256', new TextEncoder().encode(token))));
  };
  const transaction = fn => store.transaction(scope, async stored => {
    const state = stored ?? {version:1, invites:{}, members:{}, revocations:{}};
    if (state.version !== 1 || !state.invites || !state.members) throw new Error('Invalid membership store');
    state.revocations??={};
    const result = await fn(state);
    return {state, result};
  });
  async function authenticate(request, action, fields) {
    if (!validKey(request?.peerPublicKey) || await verifyPeerRequest({networkId, action, ...fields, request:clone(request)}) !== true)
      throw new AdmissionError('Invalid signed membership request');
    return request.peerPublicKey;
  }
  async function grant(peerPublicKey, membershipId, expiresAt) {
    return adminIdentity.signMembership({networkId, peerPublicKey, membershipId, expiresAt});
  }
  async function revoke(peerPublicKey,membershipId,reason) {
    if(!validKey(peerPublicKey)||typeof membershipId!=='string')throw new TypeError('Peer key and membership ID required');
    const record=await transaction(async state=>{
      const member=state.members[peerPublicKey];
      if(!member||member.membershipId!==membershipId)throw new AdmissionError('Membership does not match');
      const key=peerPublicKey+':'+membershipId;
      if(state.revocations[key])return state.revocations[key];
      const proof=await adminIdentity.signMembershipRevocation({networkId,peerPublicKey,membershipId,reason});
      member.revoked=true;state.revocations[key]=proof;return proof;
    });
    notify(record);return record;
  }
  return Object.freeze({
    networkId, authorityPublicKey:adminIdentity.publicKey,
    async issueInvite({ttlMs = 300000, membershipTtlMs = 86400000} = {}) {
      if (!Number.isSafeInteger(ttlMs) || ttlMs <= 0 || !Number.isSafeInteger(membershipTtlMs) || membershipTtlMs <= 0)
        throw new TypeError('Positive invitation and membership lifetimes required');
      const token = hex(crypto.getRandomValues(new Uint8Array(32)));
      const id = await digest(token), expiresAt = now() + ttlMs;
      if (!Number.isSafeInteger(expiresAt) || !Number.isSafeInteger(now() + membershipTtlMs)) throw new TypeError('Invalid lifetime');
      await transaction(state => {
        if (state.invites[id]) throw new Error('Invitation collision');
        state.invites[id] = {expiresAt, membershipTtlMs, consumedBy:null, membershipId:null};
      });
      return Object.freeze({networkId, token, expiresAt});
    },
    async redeem({token, request, reconnectRequest} = {}) {
      request = clone(request);
      const id = await digest(token);
      // Authenticate the token digest, never broadcast the bearer token in peer proofs.
      const peerPublicKey = await authenticate(request, 'membership.redeem', {inviteId:id});
      // Explicit, independently signed recovery may reuse an active grant, never
      // extend it or turn an old/revoked invitation into a new membership.
      const recovering = reconnectRequest != null;
      if (recovering && await authenticate(clone(reconnectRequest), 'membership.reconnect', {}) !== peerPublicKey)
        throw new AdmissionError('Reconnect identity does not match invitation claim');
      return transaction(async state => {
        const invite = state.invites[id];
        if (!invite) throw new AdmissionError('Unknown invitation');
        if (invite.consumedBy) {
          const member = state.members[peerPublicKey];
          // Retry after a lost response is idempotent only for the original active membership.
          if (invite.consumedBy !== peerPublicKey || member?.membershipId !== invite.membershipId || member.revoked || member.expiresAt <= now())
            throw new AdmissionError('Invitation already consumed');
          return grant(peerPublicKey, member.membershipId, member.expiresAt);
        }
        if (invite.expiresAt <= now()) throw new AdmissionError('Invitation expired');
        const previous = state.members[peerPublicKey];
        if (previous && !previous.revoked && previous.expiresAt > now()) {
          if (!recovering) throw new AdmissionError('Peer already has active membership');
          const credential = await grant(peerPublicKey, previous.membershipId, previous.expiresAt);
          invite.consumedBy = peerPublicKey; invite.membershipId = previous.membershipId;
          return credential;
        }
        const member = {membershipId:crypto.randomUUID(), expiresAt:now() + invite.membershipTtlMs, revoked:false};
        const credential = await grant(peerPublicKey, member.membershipId, member.expiresAt);
        invite.consumedBy = peerPublicKey; invite.membershipId = member.membershipId;
        state.members[peerPublicKey] = member;
        return credential;
      });
    },
    async reconnect({request} = {}) {
      request = clone(request);
      const peerPublicKey = await authenticate(request, 'membership.reconnect', {});
      return transaction(state => {
        const member = state.members[peerPublicKey];
        if (!member || member.revoked || member.expiresAt <= now()) throw new AdmissionError('Membership inactive');
        return grant(peerPublicKey, member.membershipId, member.expiresAt);
      });
    },
    async leave({request} = {}) {
      request = clone(request);
      const peerPublicKey = await authenticate(request, 'membership.leave', {membershipId:request?.membershipId});
      return revoke(peerPublicKey, request.membershipId, 'left');
    },
    // Privileged local API; never expose as an unauthenticated RPC.
    async remove(peerPublicKey, membershipId) {
      return revoke(peerPublicKey, membershipId, 'removed');
    },
    async getRevocations() {return transaction(state=>Object.values(state.revocations));},
    onRevocation(fn) {
      if(typeof fn!=='function')throw new TypeError('Revocation listener required');
      listeners.add(fn);return ()=>listeners.delete(fn);
    },
    async verify({networkId:room, peerPublicKey, credential}) {
      const c = clone(credential);
      if (room !== networkId || c?.version !== 2 || c.expiresAt <= now() || !await verifier({networkId, peerPublicKey, credential:c})) return false;
      return transaction(state => {
        const member = state.members[peerPublicKey];
        return !!member && !member.revoked && member.expiresAt > now() && member.membershipId === c.membershipId && member.expiresAt === c.expiresAt;
      });
    },
  });
}

/** Test/ephemeral use only. Production authorities must supply durable atomic storage. */
export function createMemoryMembershipStore() {
  const records = new Map(); let chain = Promise.resolve();
  return {transaction(scope, update) {
    const result = chain.then(async () => {
      const {state, result} = await update(clone(records.get(scope)));
      records.set(scope, clone(state)); return clone(result);
    });
    chain = result.catch(() => {}); return result;
  }};
}
