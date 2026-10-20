const bytes = (value, length) => {
  if (typeof value !== 'string' || !new RegExp(`^[0-9a-f]{${length * 2}}$`).test(value)) throw new TypeError('Invalid key or signature');
  return Uint8Array.from(value.match(/../g), pair => parseInt(pair, 16));
};
export function membershipRevocationStatement(record) {
  bytes(record?.networkId,32); bytes(record?.peerPublicKey,32);
  if (record.version !== 1 || typeof record.membershipId !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(record.membershipId) ||
      !['left','removed'].includes(record.reason)) throw new TypeError('Invalid membership revocation');
  return new TextEncoder().encode(JSON.stringify(['phantom-membership-revocation-v1',record.networkId,record.peerPublicKey,record.membershipId,record.reason]));
}
export function createMembershipRevocationVerifier({authorityPublicKey,crypto=globalThis.crypto}={}) {
  const raw=bytes(authorityPublicKey,32);let key;
  return async record => {
    try {
      record=structuredClone(record);
      const message=membershipRevocationStatement(record);
      key??=crypto.subtle.importKey('raw',raw,'Ed25519',false,['verify']);
      return await crypto.subtle.verify('Ed25519',await key,bytes(record.signature,64),message);
    } catch {return false;}
  };
}
