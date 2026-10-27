import {planConnection} from './connection-planner.js';

/** Proposal and acknowledgment on an authenticated control path; no timers. */
export function createPlanAgreement({send,allowed,localReport,remoteReport,newId,onDecision=()=>{}}) {
 if([send,allowed,localReport,remoteReport,newId,onDecision].some(f=>typeof f!=='function'))throw TypeError('Invalid plan agreement');
 const pending=new Map(),agreed=new Map();let closed=false;
 const validPeer=peer=>typeof peer==='string'&&/^[0-9a-f]{16}$/.test(peer)&&allowed(peer)===true;
 const validId=id=>typeof id==='string'&&/^[a-zA-Z0-9-]{1,64}$/.test(id);
 return {
  propose(peer,settings={}){
   if(closed||!validPeer(peer))throw Error('Peer unavailable');const remote=remoteReport(peer);if(!remote)throw Error('Peer capability report unavailable');
   const plan=planConnection({...settings,local:localReport(),remote});if(plan.status!=='proposed')throw Error('No compatible transfer plan');
   const id=newId();if(!validId(id)||pending.has(id)||pending.size>=128)throw Error('Unable to allocate plan');
   for(const [old,item] of pending)if(item.peer===peer)pending.delete(old);agreed.delete(peer);
   pending.set(id,{peer,plan});try{send(peer,{type:'planProposal',version:1,id,protocol:plan.protocol,payloadBytes:plan.payloadBytes,framingBytes:plan.framingBytes,messageBytes:plan.messageBytes});}catch(error){pending.delete(id);throw error;}return id;
  },
  receive(peer,body){
   if(closed||!validPeer(peer)||!body||body.version!==1||!validId(body.id))return false;
   if(body.type==='planProposal'){
    const local=localReport(),p=body.payloadBytes,f=body.framingBytes,m=body.messageBytes;
    const ownLimit=local.messageLimit?.status==='known'?local.messageLimit.bytes:65536;
    const accepted=typeof body.protocol==='string'&&local.protocols.includes(body.protocol)&&Number.isSafeInteger(p)&&p>=1024&&p<=local.maxChunkBytes&&Number.isSafeInteger(f)&&f>=0&&Number.isSafeInteger(m)&&m>0&&p+f<=m&&m<=ownLimit;
    send(peer,{type:'planDecision',version:1,id:body.id,accepted});return true;
   }
   if(body.type!=='planDecision'||typeof body.accepted!=='boolean')return false;
   const item=pending.get(body.id);if(!item||item.peer!==peer)return false;pending.delete(body.id);
   if(body.accepted)agreed.set(peer,{id:body.id,plan:item.plan});
   onDecision({peer,id:body.id,accepted:body.accepted,plan:structuredClone(item.plan)});return true;
  },
  get(peer){const value=agreed.get(peer);return value?structuredClone(value):null;},
  drop(peer){agreed.delete(peer);for(const [id,item] of pending)if(item.peer===peer)pending.delete(id);},
  clear(){pending.clear();agreed.clear();},
  close(){closed=true;pending.clear();agreed.clear();},
 };
}
