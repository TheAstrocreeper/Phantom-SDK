/** Portable discovery coordinates; not an admission credential. */
function validateSpecs(doorways) {
  if(!Array.isArray(doorways)||!doorways.length||doorways.length>8)throw new TypeError('Supply 1–8 doorway descriptors');
  return doorways.map(spec=>{
    if(!spec||Array.isArray(spec)||typeof spec!=='object'||! /^[a-z][a-z0-9-]{0,31}$/.test(spec.type))throw new TypeError('Doorway descriptor needs a type');
    const encoded=JSON.stringify(spec,(_key,value)=>{if(['function','undefined','symbol','bigint'].includes(typeof value)||(typeof value==='number'&&!Number.isFinite(value)))throw new TypeError('Doorway descriptor must be JSON data');return value;});
    return JSON.parse(encoded);
  });
}
export function encodeInvite({networkId, relays, doorways, adminPublicKey}) {
  if (!/^[0-9a-f]{64}$/.test(networkId)) throw new TypeError('Invalid network ID');
  if(adminPublicKey!==undefined&&!/^[0-9a-f]{64}$/.test(adminPublicKey))throw new TypeError('Invalid admin public key');
  if(adminPublicKey!==undefined&&doorways===undefined)doorways=[{type:'nostr',relays}];
  if(doorways!==undefined){const params=new URLSearchParams();for(const spec of validateSpecs(doorways))params.append('doorway',JSON.stringify(spec));if(adminPublicKey)params.set('admin',adminPublicKey);const invite=`phantom://${adminPublicKey?'v3':'v2'}/${networkId}?${params}`;if(invite.length>8192)throw new TypeError('Invite too large');return invite;}
  if (!Array.isArray(relays) || !relays.length || relays.length > 8) throw new TypeError('Supply 1–8 Nostr relays');
  const params = new URLSearchParams();
  for (const relay of relays) {
    const url = new URL(relay);
    if (url.protocol !== 'wss:' || url.username || url.password || url.hash) throw new TypeError('Relay must be a credential-free wss URL');
    params.append('relay', url.href);
  }
  return `phantom://v1/${networkId}?${params}`;
}
export function parseInvite(invite) {
  if (typeof invite !== 'string' || invite.length > 8192) throw new TypeError('Invalid invite');
  const url = new URL(invite);
  if (url.protocol !== 'phantom:' || !['v1','v2','v3'].includes(url.hostname) || url.port || url.username || url.password || url.hash || [...url.searchParams.keys()].some(k => k !== (url.hostname==='v1'?'relay':'doorway') && !(url.hostname==='v3'&&k==='admin'))) throw new TypeError('Unsupported invite format');
  const result = url.hostname==='v1'?{networkId:url.pathname.slice(1),relays:url.searchParams.getAll('relay')}:{networkId:url.pathname.slice(1),doorways:validateSpecs(url.searchParams.getAll('doorway').map(value=>JSON.parse(value)))};
  if(url.hostname==='v3'){if(url.searchParams.getAll('admin').length!==1)throw new TypeError('Invite requires one admin key');result.adminPublicKey=url.searchParams.get('admin');if(!/^[0-9a-f]{64}$/.test(result.adminPublicKey))throw new TypeError('Invalid admin key');}
  encodeInvite(result);
  return result;
}
export function createInvite({relays, doorways, adminPublicKey, crypto = globalThis.crypto}) {
  const networkId = Array.from(crypto.getRandomValues(new Uint8Array(32)), b => b.toString(16).padStart(2,'0')).join('');
  return encodeInvite({networkId,relays,doorways,adminPublicKey});
}
export function inviteLink(invite, baseUrl) {
  parseInvite(invite);
  const url = new URL(baseUrl);
  if (!['https:','http:'].includes(url.protocol)) throw new TypeError('Browser link requires HTTP(S)');
  url.hash = new URLSearchParams({invite}).toString();
  return url.href;
}
