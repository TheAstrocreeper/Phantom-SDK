import {createControlStore} from '../../phantom-core/src/admin-lifecycle.js';
import {withNativeEntry} from './native-entry.js';
import {createRelayManager} from './relay-manager.js';
import {createPhantomEntry} from '../../phantom-core/src/entry.js';
import {verifyPeerProof} from './peer-proof.js';
import {createNostrIdentity} from './nostr-crypto.js';
import {createNostrDoorway} from './nostr-doorway.js';
import {createWebSocketDoorway} from './index.js';
import {adaptWebSocketDoorway,validateDoorway} from './interface.js';
import {createCompositeDoorway} from './composite.js';
import {createAdmissionCheckpoint, AdmissionError} from '../../phantom-core/src/admission.js';
import {withAdmissionDoorway} from './admission-doorway.js';
import {parseInvite, encodeInvite} from '../../phantom-core/src/invite.js';
/** Private reusable identity seed. Applications own its storage and custody. */
export function createBrowserIdentity(){return createNostrIdentity().exportSeed();}
/** One entry API; service selection belongs to the adapter composition layer. */
export function createBrowserPhantom({identity,admission,authorizationTimeoutMs=5000,adminIdentity,adminPublicKey=adminIdentity?.publicKey,controlStore,files,distribution,readAdminOrders,retireGraceMs,rtcConfiguration,messaging='manual',relays,doorways,doorwayMode='failover',doorwayAdmissionTimeoutMs=10000,adapters={},storage,WebSocket=globalThis.WebSocket,runtime=globalThis,onRelayProgress=()=>{},onDoorwayStatus=()=>{},relayFinder,relayMaxAgeMs,now,coreFactory}={}) {
 if(!['manual','automatic'].includes(messaging))throw new TypeError('messaging must be manual or automatic');
 if(rtcConfiguration!==undefined&&(!rtcConfiguration||typeof rtcConfiguration!=='object'||Array.isArray(rtcConfiguration)))throw new TypeError('rtcConfiguration must be an object');
 if(admission!==undefined&&(!admission||typeof admission.authorize!=='function'||typeof admission.verify!=='function'))throw new TypeError('admission requires authorize and verify functions');
 if(relays!==undefined&&doorways!==undefined)throw new TypeError('Use relays or doorways, not both');
 try{storage??=runtime.localStorage;}catch{}
 const manager=createRelayManager({storage,WebSocket,onProgress:onRelayProgress,...(relayFinder?{relayFinder}:{}),...(relayMaxAgeMs!==undefined?{maxAgeMs:relayMaxAgeMs}:{}),...(now?{now}: {})});
 let sdkIdentity=identity===undefined?undefined:createNostrIdentity(identity);
 const configuration=doorways??[{type:'nostr',...(relays!==undefined?{relays}:{discover:true})}];
 function validateSpec(spec){
  if(!spec||typeof spec.type!=='string')throw new TypeError('Doorway type required');
  if(spec.type==='nostr') {if(!Array.isArray(spec.relays)||!spec.relays.length||spec.relays.length>8)throw new TypeError('Nostr doorway requires 1–8 relays');for(const value of spec.relays){const u=new URL(value);if(u.protocol!=='wss:'||u.username||u.password||u.hash)throw new TypeError('Nostr relay must use credential-free wss');}}
  else if(spec.type==='websocket'){const u=new URL(spec.url);if(u.protocol!=='wss:'||u.username||u.password||u.hash)throw new TypeError('WebSocket doorway must use credential-free wss');}
  else if(typeof adapters[spec.type]!=='function')throw new TypeError('No adapter registered for '+spec.type);
  return spec;
 }
 async function resolve(specs){if(!Array.isArray(specs)||!specs.length||specs.length>8)throw new TypeError('Supply 1–8 doorways');return Promise.all(specs.map(async raw=>{const spec=structuredClone(raw);if(spec.type==='nostr'&&!spec.relays){spec.relays=await manager.get();spec.discover??=true;}return validateSpec(spec);}));}
 async function preflight(context,options,operation){
  if(options?.signal?.aborted)throw new AdmissionError('Authorization aborted');
  if(adminPublicKey&&context.adminPublicKey!==adminPublicKey)throw new Error('Invite admin key does not match configured trust anchor');
  if(!admission)return null;
  const identity=sdkIdentity??=createNostrIdentity();
  const checkpoint=createAdmissionCheckpoint({policy:admission,networkId:context.networkId,peerPublicKey:identity.publicKey,now:now??Date.now,timeoutMs:authorizationTimeoutMs});
  let credential;try{credential=await checkpoint.authorize({operation,signal:options?.signal});}catch(error){checkpoint.close();throw error;}
  return {checkpoint,credential};
 }
 async function openContext(context,types,access){
  if(adminPublicKey&&context.adminPublicKey!==adminPublicKey)throw new Error('Invite admin key does not match configured trust anchor');
  let specs=(context.doorways??[{type:'nostr',relays:context.relays}]).map(validateSpec);
  if(types!==undefined){if(!Array.isArray(types)||types.some(t=>typeof t!=='string'))throw new TypeError('doorwayTypes must be an array of adapter types');specs=specs.filter(s=>types.includes(s.type));if(!specs.length)throw new TypeError('No invited doorway matches doorwayTypes');}
  const identity=sdkIdentity??=createNostrIdentity();
  function build(spec){let adapter;
   if(spec.type==='nostr')adapter=createNostrDoorway({identity,relays:spec.relays,WebSocket,...(access?{canFollowRelayHint:peer=>access.checkpoint.allows(peer)}:{}),...(spec.discover?{refreshRelays:async()=>{if(access&&!access.checkpoint.allows(identity.publicKey.slice(0,16)))throw new AdmissionError('Local admission credential expired');return manager.refresh();}}:{})});
   else if(spec.type==='websocket')adapter=adaptWebSocketDoorway(createWebSocketDoorway({url:spec.url,WebSocket}));
   else adapter=adapters[spec.type]({spec:structuredClone(spec),networkId:context.networkId,identity,WebSocket,runtime});
   validateDoorway(adapter);
   return access?withAdmissionDoorway(adapter,{...access,identity,now:now??Date.now}):adapter;
  }
  const initial=specs.map(build);
  const doorway=withNativeEntry(createCompositeDoorway({doorways:initial,mode:doorwayMode,admissionTimeoutMs:doorwayAdmissionTimeoutMs,recover:async index=>{
   const spec=specs[index];if(spec.type!=='nostr'||!spec.discover)return null;
   if(access&&!access.checkpoint.allows(identity.publicKey.slice(0,16)))throw new AdmissionError('Local admission credential expired');
   const fresh=await manager.refresh();const expanded=[...new Set([...spec.relays,...fresh])].slice(0,8);
   if(!fresh.length||fresh.every(url=>spec.relays.includes(url)))return null;
   // Keep original invite coordinates while adding discovered alternatives.
   return build({...spec,relays:expanded});
  }}),{onStatus:onDoorwayStatus});
  const refreshMembership=admission?.refresh?()=>admission.refresh(context.networkId):undefined;
  const leaveMembership=admission?.leave?()=>admission.leave({
   networkId:context.networkId,peerPublicKey:identity.publicKey,credential:access.credential,
   request:{peerPublicKey:identity.publicKey,membershipId:access.credential.membershipId,
    proof:identity.signPeerProof({networkId:context.networkId,kind:'membership.leave',credential:null,
     payload:{action:'membership.leave',membershipId:access.credential.membershipId},issuedAt:(now??Date.now)(),nonce:crypto.randomUUID()})}
  }):undefined;
  return {canUsePeer:access&&admission?.enforceLiveMembership===true?peer=>access.checkpoint.allows(peer):undefined,refreshMembership,leaveMembership,admissionCheckpoint:access?.checkpoint,identity:{version:1,id:identity.publicKey},recoverySignaling:{sign:payload=>identity.signPeerProof({networkId:context.networkId,kind:'sys_recovery_bridge',payload,issuedAt:Date.now(),nonce:crypto.randomUUID()}),verify:(proof,payload,key)=>proof.peerPublicKey===key&&verifyPeerProof(proof,{networkId:context.networkId,kind:'sys_recovery_bridge',payload})},runtime,doorway,canAdmitPeer:access?peer=>access.checkpoint.allows(peer):undefined,adminIdentity,controlStore:context.adminPublicKey?(controlStore??createControlStore(storage)):undefined,files,distribution,rtcConfiguration,messaging,readAdminOrders,retireGraceMs,onNetworkState:state=>doorway.observeNetworkState(state)};
 }
 return {
  async join(invite,options){const context=parseInvite(invite);const access=await preflight(context,options,'join');try{return await createPhantomEntry({openContext:context=>openContext(context,options?.doorwayTypes,access),...(coreFactory?{coreFactory}:{})}).join(invite,options);}catch(error){access?.checkpoint.close();throw error;}},
  async create(options){
   const networkId=Array.from(runtime.crypto.getRandomValues(new Uint8Array(32)),b=>b.toString(16).padStart(2,'0')).join('');
   const context={networkId,adminPublicKey};const access=await preflight(context,options,'create');
   try{const selected=await resolve(configuration);if(options?.signal?.aborted)throw new Error('Join aborted');
   const invite=encodeInvite({...context,doorways:selected});
   return await createPhantomEntry({openContext:context=>openContext(context,undefined,access),...(coreFactory?{coreFactory}:{})}).startCreated(invite,options);}catch(error){access?.checkpoint.close();throw error;}
  },
  refreshRelays:()=>{if(admission)throw new AdmissionError('Restricted relay discovery runs only after authorization');return manager.refresh();}
 };
}
