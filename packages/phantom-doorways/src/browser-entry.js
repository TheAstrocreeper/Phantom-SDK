import {createControlStore} from '../../phantom-core/src/admin-lifecycle.js';
import {withNativeEntry} from './native-entry.js';
import {createRelayManager} from './relay-manager.js';
import {createPhantomEntry} from '../../phantom-core/src/entry.js';
import {createNostrIdentity} from './nostr-crypto.js';
import {createNostrDoorway} from './nostr-doorway.js';
import {createLegacyDoorway} from './index.js';
import {adaptLegacyDoorway,validateDoorway} from './interface.js';
import {createCompositeDoorway} from './composite.js';
/** One entry API; service selection belongs to the adapter composition layer. */
export function createBrowserPhantom({adminIdentity,adminPublicKey=adminIdentity?.publicKey,controlStore,files,distribution,readAdminOrders,retireGraceMs,relays,doorways,doorwayMode='failover',doorwayAdmissionTimeoutMs=10000,adapters={},storage,WebSocket=globalThis.WebSocket,runtime=globalThis,onRelayProgress=()=>{},onDoorwayStatus=()=>{},relayFinder,relayMaxAgeMs,now,coreFactory}={}) {
 if(relays!==undefined&&doorways!==undefined)throw new TypeError('Use relays or doorways, not both');
 try{storage??=runtime.localStorage;}catch{}
 const manager=createRelayManager({storage,WebSocket,onProgress:onRelayProgress,...(relayFinder?{relayFinder}:{}),...(relayMaxAgeMs!==undefined?{maxAgeMs:relayMaxAgeMs}:{}),...(now?{now}: {})});
 let sdkIdentity;
 const configuration=doorways??[{type:'nostr',...(relays!==undefined?{relays}:{discover:true})}];
 function validateSpec(spec){
  if(!spec||typeof spec.type!=='string')throw new TypeError('Doorway type required');
  if(spec.type==='nostr') {if(!Array.isArray(spec.relays)||!spec.relays.length||spec.relays.length>8)throw new TypeError('Nostr doorway requires 1–8 relays');for(const value of spec.relays){const u=new URL(value);if(u.protocol!=='wss:'||u.username||u.password||u.hash)throw new TypeError('Nostr relay must use credential-free wss');}}
  else if(spec.type==='websocket'){const u=new URL(spec.url);if(u.protocol!=='wss:'||u.username||u.password||u.hash)throw new TypeError('WebSocket doorway must use credential-free wss');}
  else if(typeof adapters[spec.type]!=='function')throw new TypeError('No adapter registered for '+spec.type);
  return spec;
 }
 async function resolve(specs){if(!Array.isArray(specs)||!specs.length||specs.length>8)throw new TypeError('Supply 1–8 doorways');return Promise.all(specs.map(async raw=>{const spec=structuredClone(raw);if(spec.type==='nostr'&&!spec.relays){spec.relays=await manager.get();spec.discover??=true;}return validateSpec(spec);}));}
 async function openContext(context,types){
  if(adminPublicKey&&context.adminPublicKey!==adminPublicKey)throw new Error('Invite admin key does not match configured trust anchor');
  let specs=(context.doorways??[{type:'nostr',relays:context.relays}]).map(validateSpec);
  if(types!==undefined){if(!Array.isArray(types)||types.some(t=>typeof t!=='string'))throw new TypeError('doorwayTypes must be an array of adapter types');specs=specs.filter(s=>types.includes(s.type));if(!specs.length)throw new TypeError('No invited doorway matches doorwayTypes');}
  const identity=sdkIdentity??=createNostrIdentity();
  function build(spec){let adapter;
   if(spec.type==='nostr')adapter=createNostrDoorway({identity,relays:spec.relays,WebSocket});
   else if(spec.type==='websocket')adapter=adaptLegacyDoorway(createLegacyDoorway({url:spec.url,WebSocket}));
   else adapter=adapters[spec.type]({spec:structuredClone(spec),networkId:context.networkId,identity,WebSocket,runtime});
   return validateDoorway(adapter);
  }
  const initial=specs.map(build);
  const doorway=withNativeEntry(createCompositeDoorway({doorways:initial,mode:doorwayMode,admissionTimeoutMs:doorwayAdmissionTimeoutMs,recover:async index=>{
   const spec=specs[index];if(spec.type!=='nostr'||!spec.discover)return null;
   const fresh=await manager.refresh();const expanded=[...new Set([...spec.relays,...fresh])];
   if(!fresh.length||fresh.every(url=>spec.relays.includes(url)))return null;
   // Keep original invite coordinates while adding discovered alternatives.
   return build({...spec,relays:expanded});
  }}),{onStatus:onDoorwayStatus});
  return {identity:{version:1,id:identity.publicKey},runtime,doorway,adminIdentity,controlStore:context.adminPublicKey?(controlStore??createControlStore(storage)):undefined,files,distribution,readAdminOrders,retireGraceMs,onNetworkState:state=>doorway.observeNetworkState(state)};
 }
 return {
  join:(invite,options)=>createPhantomEntry({openContext:context=>openContext(context,options?.doorwayTypes),...(coreFactory?{coreFactory}:{})}).join(invite,options),
  async create(options){const selected=await resolve(configuration);if(options?.signal?.aborted)throw new Error('Join aborted');return createPhantomEntry({openContext,doorways:selected,adminPublicKey,...(coreFactory?{coreFactory}:{})}).create(options);},
  refreshRelays:()=>manager.refresh()
 };
}
