import {createDoorway} from './interface.js';
import {openNostrRelaySet} from './nostr.js';
/** Experimental compatibility discovery. Candidates are hints, not committed authority. */
export function createNostrDoorway({identity,relays,admission,WebSocket=globalThis.WebSocket,onStatus=()=>{},busFactory=openNostrRelaySet}) {
 let blocked=false;const sessions=new Set();
 const doorway=createDoorway({discovery:true,open({roomId,peerId,onOpen,onDiscovery,onSignal,onClose,onError,onPublication=()=>{},onStatus:sessionStatus=()=>{}}) {
  if(blocked)throw new Error('Nostr doorway deliberately offline');
  if(admission&&!admission.checkpoint.allows(peerId))throw new Error('Local admission credential expired');
  if(peerId!==identity.publicKey.slice(0,16))throw new Error('Core identity must match Nostr-bound Ed25519 identity');
  let own=null,closed=false,epoch=0,topology='',bus,authorityHint=null;
  const members=new Map(),routes=new Map();
  let verificationChain=Promise.resolve(),pendingVerification=0;
  function authorized(p,callback){
   if(!admission){callback(p);return;}
   if(closed||pendingVerification>=128)return;
   pendingVerification++;
   verificationChain=verificationChain.then(async()=>{
    if(closed)return;
    const valid=await admission.checkpoint.verify(p.s7pPublicKey,p.admissionCredential);
    if(closed)return;
    if(valid)callback(p);
    else sessionStatus({type:'admission_denied',peerId:p.s7pPublicKey?.slice(0,16)});
   }).catch(error=>{if(!closed)onError(error);}).finally(()=>pendingVerification--);
  }
  function active(){if(admission&&!admission.checkpoint.allows(peerId))throw new Error('Local admission credential expired');}
  function roster() {
   if(!own||closed)return;
   const list=[...members.values()].filter(p=>!admission||admission.checkpoint.allows(p.hKey)).sort((a,b)=>a.roomJoinedAt-b.roomJoinedAt||a.hKey.localeCompare(b.hKey));
   const fingerprint=list.map(p=>`${p.hKey}:${p.bootId}`).join(',');
   if(fingerprint!==topology){topology=fingerprint;epoch++;}
   const permitted=key=>key&&(!admission||admission.checkpoint.allows(key));
   onDiscovery({kind:'concierge_route',roomId,epoch,leader:permitted(authorityHint?.syscon1)?authorityHint.syscon1:list[0]?.hKey,mirror:permitted(authorityHint?.syscon2)?authorityHint.syscon2:list[1]?.hKey??null,peers:list});
  }
  function announce() {if(own&&!closed){active();bus.announce({row:own,discoveryEpoch:epoch,...(admission?{admissionCredential:admission.credential}:{}),...(authorityHint?.from===peerId?{authorityHint}: {})});}}
  bus=busFactory({roomId,identity,relays,WebSocket,onError,
   onStatus:s=>{if(closed)return;onStatus(s);sessionStatus(s);if(s.type==='publish')onPublication({id:s.messageId??s.eventId,state:s.accepted?'accepted':'rejected',surface:s.url,reason:s.reason});if(s.type==='open')onOpen();if(s.type==='exhausted')onClose({code:1006,reason:'nostr_relays_exhausted',wasClean:false});},
   onSync:()=>{if(!closed)roster();}, // Open already publishes the cached announcement.
   onAnnouncement:p=>authorized(p,p=>{
    if(closed||!p.row)return;
    const key=p.s7pPublicKey.slice(0,16),row=p.row;
    if(row.hKey!==key||row.hex!==p.s7pPublicKey||typeof row.bootId!=='string'||!Number.isSafeInteger(row.roomJoinedAt))return;
    if(routes.has(key)&&routes.get(key).s7pPublicKey!==p.s7pPublicKey){onError(new Error('Short peer ID collision'));return;}
    if(p.departed){if(members.get(key)?.bootId===row.bootId){members.delete(key);routes.delete(key);roster();}return;}
    const hint=p.authorityHint;
    if(hint && hint.room===roomId && hint.from===key && hint.syscon1===key &&
       Number.isSafeInteger(hint.epoch) && Number.isSafeInteger(hint.rosterSeq) && Array.isArray(hint.roster) &&
       hint.roster.some(r=>r.hKey===key) && (!authorityHint || hint.epoch>authorityHint.epoch ||
         (hint.epoch===authorityHint.epoch && hint.rosterSeq>authorityHint.rosterSeq))) {
      authorityHint=hint;epoch=Math.max(epoch,hint.epoch);roster();
    }
    const changed=members.get(key)?.bootId!==row.bootId;
    routes.set(key,p);members.set(key,{...row,videoId:roomId});
    if(changed){roster();announce();}
   }),
   onSignal:p=>authorized(p,p=>{
    if(closed)return;
    const control=p.control,key=p.s7pPublicKey.slice(0,16);
    // Authenticate packet authorship against the signed identity, not a copied handle.
    if(control.from!==key||control.videoId!==roomId)return;
    if(control.to&&control.to!==peerId)return;
    onSignal(control);
   })
  });
  const session={
   announce({roomId:room,peerId:key,handle,bootId,t}) {
    active();
    if(room!==roomId||key!==peerId)throw new Error('Announcement scope mismatch');
    if(!own)own={hKey:peerId,from:peerId,hex:identity.publicKey,handle,bootId,roomJoinedAt:t,videoId:roomId};
    members.set(peerId,own);roster();announce();
   },
   publishAuthority(snapshot){
    if(snapshot.from!==peerId||snapshot.syscon1!==peerId||snapshot.room!==roomId)throw new Error('Authority discovery owner mismatch');
    authorityHint=structuredClone(snapshot);epoch=Math.max(epoch,snapshot.epoch);roster();announce();
   },
   reconcile(){roster();announce();},
   sendSignal(control){
    active();
    if(control.from!==peerId||control.videoId!==roomId)throw new Error('Signal sender or room mismatch');
    const target=routes.get(control.to);
    if(!target)throw new Error('No verified Nostr route for target peer');
    bus.sendSignal(target,control,admission?{admissionCredential:admission.credential}:undefined);
   },
   close(){if(closed)return;closed=true;try{if(own&&!blocked&&bus.depart&&(!admission||admission.checkpoint.allows(peerId)))bus.depart({row:own,...(admission?{admissionCredential:admission.credential}:{})});else bus.close();}catch{bus.close();}sessions.delete(session);},
   cut(){session.close();onClose({code:1006,reason:'demo_doorway_cut',wasClean:false});}
  };sessions.add(session);return session;
 }});
 return {...doorway,cut(){blocked=true;for(const s of [...sessions])s.cut();},get offline(){return blocked;}};
}
