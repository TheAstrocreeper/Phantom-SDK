import {createDoorway,validateDoorway} from './interface.js';
import {DISCOVERY_TYPES,decodeDiscovery} from './discovery.js';
import {verifyPeerProof} from './peer-proof.js';
import {AdmissionError} from '../../phantom-core/src/admission.js';

const copy=value=>JSON.parse(JSON.stringify(value));
/** Shared gate: transports route proofs; they never decide who is authorized. */
export function withAdmissionDoorway(adapter,{checkpoint,credential,identity,now=Date.now}={}) {
 validateDoorway(adapter);
 if(typeof identity?.signPeerProof!=='function')throw new TypeError('Signing peer identity required');
 return createDoorway({discovery:true,presence:adapter.presence,open(context){
  const {roomId,peerId}=context;
  let closed=false,session,own,chain=Promise.resolve(),pending=0;
  const seen=new Set();
  function active(){
   if(closed||peerId!==identity.publicKey.slice(0,16)||!checkpoint.allows(peerId))throw new AdmissionError('Local admission credential missing or expired');
  }
  // Runs before adapter.open, including private-server socket construction.
  active();
  function proof(kind,payload){active();return identity.signPeerProof({networkId:roomId,kind,credential:copy(credential),payload:copy(payload),issuedAt:now(),nonce:globalThis.crypto.randomUUID()});}
  function enqueue(callback){
   if(closed||pending>=128)return;
   pending++;
   chain=chain.then(async()=>{if(!closed)await callback();}).catch(error=>{if(!closed)context.onError?.(error);}).finally(()=>pending--);
  }
  async function verified(value,kind,payload){
   if(!verifyPeerProof(value,{networkId:roomId,kind,payload,now:now()}))return false;
   const valid=await checkpoint.verify(value.peerPublicKey,value.credential);
   return !closed&&valid;
  }
  function discovery(hint){enqueue(async()=>{
   if(hint.roomId!==undefined&&hint.roomId!==roomId)return;
   if(!Array.isArray(hint.peers)||hint.peers.length>256)return;
   const rows=[];
   for(const row of hint.peers){
    const p=row?.admissionProof;
    if(row?.hKey===peerId&&own){rows.push(own);continue;}
    if(!await verified(p,'announce'))continue;
    const signed=p.payload,key=p.peerPublicKey.slice(0,16);
    if(!signed||signed.roomId!==roomId||signed.peerId!==key||typeof signed.bootId!=='string'||!Number.isSafeInteger(signed.t)||row.hKey!==key||
       (row.hex!==undefined&&row.hex!==p.peerPublicKey)||
       (row.bootId!==undefined&&row.bootId!==signed.bootId))continue;
    // Transport metadata cannot replace signed peer identity/incarnation fields.
    rows.push({...row,hKey:key,from:key,hex:p.peerPublicKey,handle:signed.handle,bootId:signed.bootId,roomJoinedAt:signed.t,videoId:roomId});
   }
   if(closed)return;
   const keys=new Set(rows.map(row=>row.hKey));
   const leader=keys.has(hint.leader)?hint.leader:rows[0]?.hKey??null;
   const mirror=keys.has(hint.mirror)&&hint.mirror!==leader?hint.mirror:rows.find(row=>row.hKey!==leader)?.hKey??null;
   context.onDiscovery({...hint,peers:rows,leader,mirror});
  });}
  function signal(packet){enqueue(async()=>{
   if(!packet||packet.videoId!==roomId||(packet.to&&packet.to!==peerId))return;
   const {admissionProof,...payload}=packet;
   if(!admissionProof||packet.from!==admissionProof.peerPublicKey?.slice(0,16)||!await verified(admissionProof,'signal',payload))return;
   const id=admissionProof.signature;
   if(seen.has(id))return;
   seen.add(id);if(seen.size>4096)seen.delete(seen.values().next().value);
   context.onSignal(payload);
  });}
  const callbacks={...context,authorization:{networkId:roomId,peerPublicKey:identity.publicKey,credential:copy(credential)},
   signAdmission:(kind,payload)=>proof(kind,payload),
   onDiscovery:hint=>discovery(copy(hint)),onSignal:packet=>signal(copy(packet)),
   onPacket:packet=>{const p=copy(packet);if(DISCOVERY_TYPES.has(p?.type))discovery(decodeDiscovery(p));else signal(p);},
   onClose:detail=>{closed=true;context.onClose?.(detail);},
  };
  session=adapter.open(callbacks);
  const typed=adapter.protocol==='phantom-doorway-v2';
  const send=packet=>typed?session.sendSignal(packet):session.send(packet);
  return {
   announce(value){
    active();if(value.roomId!==roomId||value.peerId!==peerId)throw new AdmissionError('Announcement scope mismatch');
    const signed=copy(value),admissionProof=proof('announce',signed);
    own={hKey:peerId,from:peerId,hex:identity.publicKey,handle:signed.handle,bootId:signed.bootId,roomJoinedAt:signed.t,videoId:roomId,admissionProof};
    if(typed)return session.announce({...signed,admissionProof});
    const {roomId:room,peerId:key,...fields}=signed;
    return session.send({...fields,admissionProof,type:'concierge_connect',videoId:room,from:key});
   },
   reconcile(value){active();const payload=copy({...value,videoId:roomId,from:peerId});const packet={...payload,admissionProof:proof('signal',payload)};return typed?session.reconcile(packet):session.send(packet);},
   sendSignal(value){active();const payload=copy(value);if(payload.from!==peerId||payload.videoId!==roomId)throw new AdmissionError('Signal scope mismatch');return send({...payload,admissionProof:proof('signal',payload)});},
   publishAuthority(value){active();return session.publishAuthority?.({...value,admissionProof:proof('authority',value)});},
   observeNetworkState:state=>session.observeNetworkState?.(state),
   close(){if(closed)return;closed=true;seen.clear();session.close();},
  };
 }});
}
