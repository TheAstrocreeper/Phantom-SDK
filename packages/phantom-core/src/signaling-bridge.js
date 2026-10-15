const types=new Set(['rtc_offer','rtc_answer','rtc_ice']);
/** One intermediary, end-to-end authenticated SYS signaling. No authority grants. */
export function createSignalingBridge({self,room,member,peers,send,deliver,authentication,bindIdentity=()=>false,status=()=>{},now=Date.now}){
 const seen=new Map(),routes=new Map(),lifetime=15000;
 function prune(){const t=now();for(const [k,v] of seen)if(t-v>=lifetime)seen.delete(k);for(const [k,v] of routes)if(t-v.at>=lifetime)routes.delete(k);}
 function valid(packet){return packet&&types.has(packet.type)&&packet.videoId===room()&&packet.pipe==='sys'&&packet.sysCarrier===true&&typeof packet.sysAttemptId==='string'&&packet.sysAttemptId.length<=256&&typeof packet.bootId==='string'&&packet.bootId&&packet.from!==packet.to&&member(packet.from)&&member(packet.to);}
 function transmit(packet){
  if(!authentication||packet.from!==self()||!valid(packet))return false;prune();
  const cached=routes.get(packet.to),available=peers().filter(p=>p!==packet.to&&p!==self());
  const candidates=[...new Set([cached?.peer,...available].filter(Boolean))].filter(p=>available.includes(p)).slice(0,3);
  if(!candidates.length)return false;
  let proof;try{const wire=JSON.stringify(packet);if(wire.length>98304)return false;proof=authentication.sign({packet:JSON.parse(wire)});}catch(error){status({action:'failed',reason:'wire_encoding_or_signing_failed',errorName:error?.name??'Error',from:packet.from,to:packet.to,signalType:packet.type,attemptId:packet.sysAttemptId});return false;}if(!proof||proof.peerPublicKey?.slice(0,16)!==packet.from||JSON.stringify(proof).length>98304)return false;
  let sent=false;for(const peer of candidates)if(send(peer,{t:'SYS_BRIDGE_RTC',hops:0,proof})){sent=true;status({action:'sent',peer,from:packet.from,to:packet.to,signalType:packet.type,attemptId:packet.sysAttemptId});}return sent;
 }
 function receive(envelope,via){
  const proof=envelope?.proof,packet=proof?.payload?.packet;
  const reject=reason=>{status({action:'rejected',reason,peer:String(via).slice(0,16),from:typeof packet?.from==='string'?packet.from.slice(0,16):undefined,to:typeof packet?.to==='string'?packet.to.slice(0,16):undefined,signalType:typeof packet?.type==='string'?packet.type.slice(0,32):undefined,attemptId:typeof packet?.sysAttemptId==='string'?packet.sysAttemptId.slice(0,256):undefined});return false;};
  if(!authentication)return reject('authentication_missing');
  if(!peers().includes(via))return reject('carrier_not_ready');
  if(![0,1].includes(envelope?.hops))return reject('hop_limit');
  if(JSON.stringify(envelope).length>98304)return reject('envelope_limit');
  prune();
  if(!valid(packet))return reject(!packet?'packet_missing':!member(packet.from)?'sender_not_committed':!member(packet.to)?'target_not_committed':'packet_invalid');
  if(!Number.isSafeInteger(proof.issuedAt)||proof.issuedAt>now()+5000||now()-proof.issuedAt>=lifetime)return reject('expired_or_invalid_time');
  if(typeof proof.nonce!=='string'||!proof.nonce||proof.nonce.length>128)return reject('nonce_invalid');
  let source=member(packet.from);
  if(typeof source.hex!=='string'&&envelope.hops===1){
   const attestation=envelope.identityProof,carrier=member(via),binding=attestation?.payload;
   if(!carrier?.hex||attestation?.peerPublicKey!==carrier.hex||attestation.peerPublicKey.slice(0,16)!==via||!Number.isSafeInteger(attestation.issuedAt)||attestation.issuedAt>now()+5000||now()-attestation.issuedAt>=lifetime||binding?.room!==room()||binding.from!==via||binding.source!==packet.from||binding.publicKey!==proof.peerPublicKey||binding.bootId!==packet.bootId||binding.nonce!==proof.nonce||!authentication.verify(attestation,binding,carrier.hex))return reject('identity_attestation_invalid');
   if(!bindIdentity(packet.from,binding.publicKey))return reject('identity_binding_rejected');source=member(packet.from);
  }
  if(typeof source.hex!=='string')return reject('sender_key_missing');
  if(proof.peerPublicKey!==source.hex||proof.peerPublicKey.slice(0,16)!==packet.from)return reject('sender_key_mismatch');
  if(source.bootId&&source.bootId!==packet.bootId)return reject('sender_incarnation_mismatch');
  if(!authentication.verify(proof,{packet},source.hex))return reject('signature_invalid');
  if(envelope.hops===0&&packet.from!==via)return reject('immediate_sender_mismatch');
  if(envelope.hops===1&&packet.to!==self())return reject('foreign_forwarded_target');
  const id=packet.from+':'+proof.nonce;
  if(seen.has(id))return reject('duplicate');
  if(seen.size>=256)return reject('deduplication_limit');seen.set(id,now());
  if(packet.to===self()){
   if(envelope.hops===1){if(routes.size>=64&&!routes.has(packet.from))routes.delete(routes.keys().next().value);routes.set(packet.from,{peer:via,at:now()});}
   status({action:'received',peer:via,from:packet.from,to:packet.to,signalType:packet.type,attemptId:packet.sysAttemptId});deliver(packet);return true;
  }
  if(envelope.hops!==0||packet.from===self()||packet.to===via)return reject('forwarding_scope_invalid');
  if(!peers().includes(packet.to))return reject('target_pipe_not_ready');
  let identityProof;try{identityProof=authentication.sign({room:room(),from:self(),source:packet.from,publicKey:source.hex,bootId:packet.bootId,nonce:proof.nonce});}catch{return reject('identity_attestation_signing_failed');}
  const forwarded={t:'SYS_BRIDGE_RTC',hops:1,proof,identityProof};if(JSON.stringify(forwarded).length>98304)return reject('envelope_limit');
  if(!send(packet.to,forwarded))return reject('forward_send_failed');
  status({action:'forwarded',peer:via,from:packet.from,to:packet.to,signalType:packet.type,attemptId:packet.sysAttemptId});return true;
 }
 return {send:transmit,receive,clear(){seen.clear();routes.clear();}};
}
