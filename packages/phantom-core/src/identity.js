/** Network identifier, independent of display names. Not an ownership proof. */
export function createPeerIdentity({crypto=globalThis.crypto}={}) {
 if(typeof crypto?.getRandomValues!=='function')throw new Error('Secure randomness required');
 const bytes=crypto.getRandomValues(new Uint8Array(32));
 return Object.freeze({version:1,id:Array.from(bytes,b=>b.toString(16).padStart(2,'0')).join('')});
}
export function resolvePeerIdentity(identity,{crypto=globalThis.crypto}={}) {
 if(identity===undefined)return createPeerIdentity({crypto});
 if(identity?.version!==1 || typeof identity.id!=='string' || !/^[0-9a-f]{64}$/.test(identity.id))throw new TypeError('identity must be a saved {version:1,id:64-character lowercase hex} record');
 return Object.freeze({version:1,id:identity.id});
}
export function validatePeerName(name) {
 if(typeof name!=='string'||!name.trim()||name.trim().length>128||/[\u0000-\u001f\u007f]/u.test(name))throw new TypeError('name must be nonempty text, at most 128 characters, without control characters');
 return name.trim();
}
