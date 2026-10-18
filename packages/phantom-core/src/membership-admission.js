import {AdmissionError,createAdmissionVerifier} from './admission.js';
import {createMembershipRevocationVerifier} from './membership-proof.js';

/** Application callbacks carry authority traffic; SDK verifies and enforces membership.
 * readRevocations must return a current complete snapshot over trusted transport.
 */
export function createMembershipAdmission({authorityPublicKey,authorize,readRevocations,subscribeRevocations,leave,
  crypto=globalThis.crypto,now=Date.now,pollIntervalMs=5000,maxAgeMs=15000,runtime=globalThis}={}) {
  if(typeof authorize!=='function'||typeof readRevocations!=='function')throw new TypeError('Membership authorize and readRevocations callbacks required');
  if(leave!==undefined&&typeof leave!=='function')throw new TypeError('Invalid membership leave callback');
  if(subscribeRevocations!==undefined&&typeof subscribeRevocations!=='function')throw new TypeError('Invalid revocation subscription');
  if(!Number.isFinite(pollIntervalMs)||pollIntervalMs<=0||!Number.isFinite(maxAgeMs)||maxAgeMs<pollIntervalMs)throw new TypeError('Invalid membership refresh intervals');
  const verifyGrant=createAdmissionVerifier({authorityPublicKey,crypto});
  const verifyRevocation=createMembershipRevocationVerifier({authorityPublicKey,crypto});
  const rooms=new Map();
  function room(networkId) {
    if(!/^[0-9a-f]{64}$/.test(networkId))throw new TypeError('Invalid network ID');
    if(!rooms.has(networkId))rooms.set(networkId,{networkId,revoked:new Set(),listeners:new Set(),freshUntil:0,pending:null,timer:null,unsubscribe:null});
    return rooms.get(networkId);
  }
  const emit=(state,event)=>{for(const fn of state.listeners)try{fn(event);}catch{}};
  async function applyRevocation(raw) {
    const record=structuredClone(raw);
    if(!await verifyRevocation(record))throw new AdmissionError('Invalid membership revocation');
    const state=room(record.networkId),key=record.peerPublicKey+':'+record.membershipId;
    if(!state.revoked.has(key)){
      state.revoked.add(key);
      emit(state,{...record,type:'revoked'});
    }
    return true;
  }
  function fresh(state){return state.freshUntil>now();}
  async function refresh(networkId) {
    const state=room(networkId);
    if(state.pending)return state.pending;
    state.pending=(async()=>{
      // Freshness begins at request start, not at arrival of a potentially slow response.
      const started=now();
      const controller=new AbortController();let timer;
      let rows;try{rows=await Promise.race([Promise.resolve().then(()=>readRevocations({networkId,signal:controller.signal})),new Promise((_,reject)=>{timer=runtime.setTimeout(()=>{controller.abort();reject(new AdmissionError('Membership refresh timed out'));},Math.min(5000,maxAgeMs));})]);}finally{runtime.clearTimeout(timer);}
      if(!Array.isArray(rows)||rows.length>65536)throw new AdmissionError('Invalid membership revocation snapshot');
      for(const raw of rows){if(raw?.networkId!==networkId)throw new AdmissionError('Wrong revocation network');await applyRevocation(raw);}
      state.freshUntil=started+maxAgeMs;
      if(!fresh(state))throw new AdmissionError('Membership snapshot expired in transit');
      return true;
    })();
    try {return await state.pending;}
    finally {state.pending=null;}
  }
  function schedule(state) {
    if(!state.listeners.size)return;
    const delay=Math.max(1,Math.min(pollIntervalMs,state.freshUntil>now()?state.freshUntil-now():pollIntervalMs));
    state.timer=runtime.setTimeout(async()=>{
      state.timer=null;
      // Invalidate before awaiting a refresh that may hang or fail.
      if(!fresh(state))emit(state,{networkId:state.networkId,type:'stale'});
      // Do not block the next freshness deadline on an unresponsive authority.
      void refresh(state.networkId).catch(()=>{});
      schedule(state);
    },delay);
    state.timer?.unref?.();
  }
  function allows({networkId,peerPublicKey,credential}) {
    const state=room(networkId);
    return fresh(state)&&credential?.version===2&&credential.networkId===networkId&&credential.peerPublicKey===peerPublicKey&&
      credential.expiresAt>now()&&!state.revoked.has(peerPublicKey+':'+credential.membershipId);
  }
  return Object.freeze({
    enforceLiveMembership:true,
    async authorize(input) {
      return authorize(input);
    },
    async verify(input) {
      const credential=structuredClone(input.credential),value={...input,credential};
      if(credential?.version!==2||!await verifyGrant(value))return false;
      if(!fresh(room(input.networkId)))await refresh(input.networkId);
      return allows(value);
    },
    allows,refresh,applyRevocation,
    ...(leave?{async leave(input){const record=await leave(input);if(record?.networkId!==input.networkId||record.peerPublicKey!==input.peerPublicKey||record.membershipId!==input.credential?.membershipId)throw new AdmissionError('Leave response does not match membership');await applyRevocation(record);return record;}}:{}),
    subscribe({networkId,onChange}) {
      const state=room(networkId);state.listeners.add(onChange);
      if(state.listeners.size===1){
        try {state.unsubscribe=subscribeRevocations?.({networkId,onRevocation:raw=>{void applyRevocation(raw).catch(()=>{});}});if(state.unsubscribe!==undefined&&typeof state.unsubscribe!=='function')throw new TypeError('Revocation subscription must return an unsubscribe function');}
        catch(error){state.listeners.delete(onChange);throw error;}
        schedule(state);
      }
      return ()=>{
        state.listeners.delete(onChange);
        if(!state.listeners.size){runtime.clearTimeout(state.timer);state.timer=null;state.unsubscribe?.();state.unsubscribe=null;}
      };
    },
  });
}
