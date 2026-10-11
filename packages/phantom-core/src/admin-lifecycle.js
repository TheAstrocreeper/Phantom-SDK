import {createAdminAuthorization} from './authorization.js';

/** Records are retained outside elected membership. Stores must atomically merge by signature. */
export function createControlStore(storage) {
 if(!storage?.getItem||!storage?.setItem)throw new TypeError('Persistent storage or a controlStore is required');
 const key=id=>'phantom-controls-v1:'+id;
 return {async load(id){const value=storage.getItem(key(id));return value?JSON.parse(value):[];},async append(id,record){const value=storage.getItem(key(id));const rows=value?JSON.parse(value):[];if(!rows.some(r=>r.signature===record.signature)){rows.push(record);storage.setItem(key(id),JSON.stringify(rows));}}};
}
export async function createAdminLifecycle({networkId,adminPublicKey,adminIdentity,store,crypto=globalThis.crypto,send=()=>{},emit=()=>{},onRetire=()=>{}}) {
 if(!adminPublicKey)return null;
 if(!store?.load||!store?.append)throw new TypeError('Admin networks require a durable controlStore');
 if(adminIdentity&&adminIdentity.publicKey!==adminPublicKey)throw new Error('Admin signer does not match invite');
 const policy=createAdminAuthorization({networkId,adminPublicKey,crypto});
 const records=new Map(),revoked=new Set();let retirement=null,chain=Promise.resolve();
 const retain=r=>{records.set(r.signature,r);if(r.action==='network.retire')retirement=r;else if(r.action==='content.revoke')revoked.add(r.target);};
 const saved=await store.load(networkId);
 if(!Array.isArray(saved))throw new Error('Invalid control store');
 for(const raw of saved){const r=await policy.verify(raw);if(!r)throw new Error('Invalid stored admin proof');retain(r);}
 async function apply(raw){const record=await policy.verify(raw);if(!record)return false;if(records.has(record.signature))return true;
  // Fail closed on persistence failure: no successful operation before its tombstone is retained.
  await store.append(networkId,record);retain(record);send(record);
  if(record.action==='network.retire')onRetire(record);
  emit(record.action==='network.retire'?'retired':record.action==='content.revoke'?'revoked':'published',record);
  return true;
 }
 return {
  get retired(){return !!retirement;},get retirement(){return retirement;},isRevoked:id=>revoked.has(id),
  records:()=>[...records.values()].map(r=>({...r})),
  apply(raw){const result=chain.then(()=>apply(raw));chain=result.catch(()=>{});return result;},
  async issue(action,target){if(!adminIdentity)throw new Error('Admin seed required');if(retirement)throw new Error('Network retired');const record=await adminIdentity.sign({networkId,action,target,operationId:crypto.randomUUID()});await this.apply(record);return record;},
  sync(to){for(const r of records.values())send(r,to);}
 };
}
