import {createDoorway} from './interface.js';
import {normalizeRelays} from './relay-probe.js';
import {openNostrRelaySet} from './nostr.js';
/** Experimental compatibility discovery. Candidates are hints, not committed authority. */
export function createNostrDoorway({identity,relays,WebSocket=globalThis.WebSocket,onStatus=()=>{},busFactory=openNostrRelaySet,refreshRelays,canFollowRelayHint=()=>true}) {
 let blocked=false;const sessions=new Set();
 const doorway=createDoorway({discovery:true,open({roomId,peerId,onOpen,onDiscovery,onSignal,onClose,onError,onPublication=()=>{},onStatus:sessionStatus=()=>{}}) {
  if(blocked)throw new Error('Nostr doorway deliberately offline');
  if(peerId!==identity.publicKey.slice(0,16))throw new Error('Core identity must match Nostr-bound Ed25519 identity');
  let own=null,closed=false,epoch=0,topology='',bus,authorityHint=null;
  let sharedRelays=normalizeRelays(relays,8),expanded=false,refreshing=false;
  function expand(values){if(closed||!bus?.addRelays)return;const next=normalizeRelays([...sharedRelays,...values],8);if(next.length===sharedRelays.length)return;sharedRelays=bus.addRelays(next);signalStatus('relay_set_expanded',{relays:[...sharedRelays]});announce();}
  function prepareAlternatives(){if(!refreshRelays||expanded||refreshing)return;refreshing=true;Promise.resolve().then(()=>refreshRelays()).then(values=>{if(!closed){expanded=true;expand(values);}}).catch(error=>{if(!closed)onError(error);}).finally(()=>{refreshing=false;});}
  const members=new Map(),routes=new Map(),pendingSignals=[],incarnations=new Map();
  function observeIncarnation(key,row,departed) {
   let known=incarnations.get(key);
   if(known&&(row.roomJoinedAt<known.joinedAt||known.retired.has(row.bootId)))return false;
   if(!known){
    if(incarnations.size>=4096)incarnations.delete(incarnations.keys().next().value);
    known={joinedAt:row.roomJoinedAt,bootId:row.bootId,retired:new Set()};incarnations.set(key,known);
   }
   if(known.bootId!==row.bootId){known.retired.add(known.bootId);known.bootId=row.bootId;}
   known.joinedAt=Math.max(known.joinedAt,row.roomJoinedAt);
   if(departed)known.retired.add(row.bootId);
   // Bound remembered boot IDs for long-running sessions.
   if(known.retired.size>32)known.retired.delete(known.retired.values().next().value);
   return true;
  }
  function signalStatus(type,detail){onStatus({type,...detail});sessionStatus({type,...detail});}
  function pruneSignals(){for(let i=pendingSignals.length-1;i>=0;i--)if(Date.now()-pendingSignals[i].at>=15000){const [item]=pendingSignals.splice(i,1);signalStatus('signal_discarded',{reason:'discovery_wait_expired',from:item.p.control.from,attemptId:item.p.control.sysAttemptId});}}
  function deliver(p){const key=p.s7pPublicKey.slice(0,16),row=members.get(key),route=routes.get(key);
   if(!row||!route||route.s7pPublicKey!==p.s7pPublicKey)return false;
   if(p.control.bootId&&p.control.bootId!==row.bootId){signalStatus('signal_discarded',{reason:'sender_incarnation_mismatch',from:key,attemptId:p.control.sysAttemptId});return true;}
   signalStatus('signal_delivered',{from:key,to:peerId,signalType:p.control.type,attemptId:p.control.sysAttemptId});onSignal(p.control);return true;
  }
  function resumeSignals(key){pruneSignals();for(let i=0;i<pendingSignals.length;){if(pendingSignals[i].p.control.from!==key){i++;continue;}const [item]=pendingSignals.splice(i,1);deliver(item.p);}}

  function roster() {
   if(!own||closed)return;
   const list=[...members.values()].sort((a,b)=>a.roomJoinedAt-b.roomJoinedAt||a.hKey.localeCompare(b.hKey));
   const fingerprint=list.map(p=>`${p.hKey}:${p.bootId}`).join(',');
   if(fingerprint!==topology){topology=fingerprint;epoch++;}
   onDiscovery({kind:'concierge_route',roomId,epoch,leader:authorityHint?.syscon1??list[0]?.hKey,mirror:authorityHint?.syscon2===null?null:authorityHint?.syscon2??list[1]?.hKey??null,peers:list});
  }
  function announce() {if(own&&!closed)bus.announce({row:own,...(refreshRelays?{relays:sharedRelays}:{}),discoveryEpoch:epoch,...(authorityHint?.from===peerId?{authorityHint}: {})});}
  bus=busFactory({roomId,identity,relays,WebSocket,onError,
   onStatus:s=>{if(closed)return;onStatus(s);sessionStatus(s);if(s.type==='publish')onPublication({id:s.messageId??s.eventId,state:s.accepted?'accepted':'rejected',surface:s.url,reason:s.reason});if(s.type==='write_blocked'||s.type==='publication_paused')prepareAlternatives();if(s.type==='open')onOpen();if(s.type==='exhausted')onClose({code:1006,reason:'nostr_relays_exhausted',wasClean:false});},
   onSync:()=>{if(!closed)roster();}, // Open already publishes the cached announcement.
   onAnnouncement:p=>{
    if(closed||!p.row)return;
    const key=p.s7pPublicKey.slice(0,16),row=p.row;
    if(row.hKey!==key||row.hex!==p.s7pPublicKey||typeof row.bootId!=='string'||!Number.isSafeInteger(row.roomJoinedAt))return;
    if(routes.has(key)&&routes.get(key).s7pPublicKey!==p.s7pPublicKey){onError(new Error('Short peer ID collision'));return;}
    if(!observeIncarnation(key,row,p.departed===true)){signalStatus('announcement_discarded',{reason:'retired_or_older_incarnation',from:key});return;}
    if(p.departed){if(members.get(key)?.bootId===row.bootId){members.delete(key);routes.delete(key);for(let i=pendingSignals.length-1;i>=0;i--)if(pendingSignals[i].p.control.from===key)pendingSignals.splice(i,1);roster();}return;}
    const hint=p.authorityHint;
    if(hint && hint.room===roomId && hint.from===key && hint.syscon1===key &&
       Number.isSafeInteger(hint.epoch) && Number.isSafeInteger(hint.rosterSeq) && Array.isArray(hint.roster) &&
       hint.roster.some(r=>r.hKey===key) && (!authorityHint || hint.epoch>authorityHint.epoch ||
         (hint.epoch===authorityHint.epoch && hint.rosterSeq>authorityHint.rosterSeq))) {
      authorityHint=hint;epoch=Math.max(epoch,hint.epoch);roster();
    }
    const changed=members.get(key)?.bootId!==row.bootId;
    routes.set(key,p);members.set(key,{...row,videoId:roomId});
    if(refreshRelays&&canFollowRelayHint(key)&&Array.isArray(p.relays)&&p.relays.length<=8)expand(p.relays);
    if(changed){roster();announce();}resumeSignals(key);
   },
   onSignal:p=>{
    if(closed)return;
    const control=p.control,key=p.s7pPublicKey.slice(0,16);
    // Authenticate packet authorship against the signed identity, not a copied handle.
    if(control.from!==key||control.videoId!==roomId)return;
    if(control.to&&control.to!==peerId)return;
    pruneSignals();if(deliver(p))return;
    if(pendingSignals.length>=64||pendingSignals.filter(item=>item.p.control.from===key).length>=16||JSON.stringify(control).length>65536){signalStatus('signal_discarded',{reason:'discovery_queue_limit',from:key});return;}
    pendingSignals.push({p:{s7pPublicKey:p.s7pPublicKey,control:structuredClone(control)},at:Date.now()});signalStatus('signal_deferred',{reason:'waiting_for_verified_discovery',from:key,signalType:control.type,attemptId:control.sysAttemptId});
   }
  });
  const session={
   announce({roomId:room,peerId:key,handle,bootId,t,admissionProof}) {
    if(room!==roomId||key!==peerId)throw new Error('Announcement scope mismatch');
    if(!own)own={hKey:peerId,from:peerId,hex:identity.publicKey,handle,bootId,roomJoinedAt:t,videoId:roomId,...(admissionProof?{admissionProof}:{})};
    members.set(peerId,own);roster();announce();
   },
   publishAuthority(snapshot){
    if(snapshot.from!==peerId||snapshot.syscon1!==peerId||snapshot.room!==roomId)throw new Error('Authority discovery owner mismatch');
    authorityHint=structuredClone(snapshot);epoch=Math.max(epoch,snapshot.epoch);roster();announce();
   },
   reconcile(){roster();announce();},
   sendSignal(control){
    if(control.from!==peerId||control.videoId!==roomId)throw new Error('Signal sender or room mismatch');
    const target=routes.get(control.to);
    if(!target)throw new Error('No verified Nostr route for target peer');
    bus.sendSignal(target,control);
   },
   close(){if(closed)return;closed=true;pendingSignals.length=0;incarnations.clear();try{if(own&&!blocked&&bus.depart)bus.depart({row:own});else bus.close();}catch{bus.close();}sessions.delete(session);},
   cut(){session.close();onClose({code:1006,reason:'demo_doorway_cut',wasClean:false});}
  };sessions.add(session);return session;
 }});
 return {...doorway,cut(){blocked=true;for(const s of [...sessions])s.cut();},get offline(){return blocked;}};
}
