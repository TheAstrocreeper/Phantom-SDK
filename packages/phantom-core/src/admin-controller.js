import {createAdminAuthorization} from './authorization.js';

/** Isolated authority: no peer identity, doorway, roster membership or RTC connection. */
export function createAdminController({identity,store,crypto=globalThis.crypto}={}) {
  if(!identity?.publicKey||typeof identity.sign!=='function')throw new TypeError('Admin identity required');
  if(store&&(!store.load||!store.append))throw new TypeError('Supply both control store methods');
  const retained=new Map();let chain=Promise.resolve();
  async function getOrders(networkId){
    const policy=createAdminAuthorization({networkId,adminPublicKey:identity.publicKey,crypto});
    const records=retained.get(networkId)??new Map();
    if(store){const rows=await store.load(networkId);if(!Array.isArray(rows))throw Error('Invalid admin order store');for(const raw of rows){const proof=await policy.verify(raw);if(!proof)throw Error('Invalid retained admin order');records.set(proof.signature,proof);}}
    retained.set(networkId,records);return [...records.values()].map(r=>({...r}));
  }
  function issue(networkId,action,target){const result=chain.then(async()=>{
    const current=await getOrders(networkId);if(current.some(r=>r.action==='network.retire'))throw Error('Network retired');
    const proof=await identity.sign({networkId,action,target,operationId:crypto.randomUUID()});
    if(store)await store.append(networkId,proof);retained.get(networkId).set(proof.signature,proof);return proof;
  });chain=result.catch(()=>{});return result;}
  return Object.freeze({publicKey:identity.publicKey,getOrders,
    publish:(networkId,manifestId)=>{if(!/^sha256:[0-9a-f]{64}$/.test(manifestId))throw new TypeError('Verified manifest ID required');return issue(networkId,'content.publish',manifestId);},
    revoke:(networkId,target)=>issue(networkId,'content.revoke',target),
    retire:networkId=>issue(networkId,'network.retire',networkId)
  });
}
