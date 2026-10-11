import {createDoorway,validateDoorway} from './interface.js';
/** Shared discovery surface. Parallel paths avoid splitting bootstrap by provider. */
export function createCompositeDoorway({doorways,recover,openTimeoutMs=8000,mode='parallel',admissionTimeoutMs=10000}={}) {
 if(!['parallel','failover'].includes(mode))throw new TypeError('Doorway mode must be parallel or failover');
 if(!Array.isArray(doorways)||!doorways.length)throw new TypeError('At least one doorway required');
 for(const d of doorways){validateDoorway(d);if(d.protocol!=='phantom-doorway-v2')throw new TypeError('Composite requires typed doorway adapters');}
 return createDoorway({discovery:true,presence:'discovery-hints',open(context){
  const slots=doorways.map(adapter=>({adapter,session:null,open:false,ended:false,retried:false,generation:0,hint:null,peers:new Map()}));
  let closed=false,opened=false,admitted=false,own,authority,epoch=0,fingerprint='';const seen=new Set();
  const status=(type,detail={})=>{if(!closed)context.onStatus?.({type,...detail});};
  function roster(source){if(closed||!own||slots.length===1||mode==='failover')return;const rows=new Map([[own.peerId,{hKey:own.peerId,from:own.peerId,handle:own.handle,bootId:own.bootId,roomJoinedAt:own.t}]]);
   for(const slot of slots)for(const [key,row]of slot.peers){const old=rows.get(key);if(old?.hex&&row.hex&&old.hex!==row.hex){status('identity_conflict',{peerId:key});continue;}if(!old||Number(row.roomJoinedAt)<Number(old.roomJoinedAt))rows.set(key,{...old,...row});}
   const peers=[...rows.values()].sort((a,b)=>Number(a.roomJoinedAt)-Number(b.roomJoinedAt)||a.hKey.localeCompare(b.hKey));
   const next=JSON.stringify(peers.map(p=>[p.hKey,p.bootId]));if(next!==fingerprint){fingerprint=next;if(!authority)epoch++;}
   // Provider visibility cannot advance an already committed SYS epoch.
   epoch=authority?.epoch??epoch;
   const leader=authority?.syscon1??peers[0]?.hKey,mirror=authority?.syscon2??peers[1]?.hKey??null;
   const extensions={...(source?.extensions??{})};
   delete extensions.syscon1EndpointId;delete extensions.syscon2EndpointId;
   function endpoint(key){if(!key)return undefined;for(const slot of slots){
    const row=slot.peers.get(key);if(row?.endpointId)return row.endpointId;
    if(slot.hint?.leader===key&&slot.hint.extensions?.syscon1EndpointId)return slot.hint.extensions.syscon1EndpointId;
    if(slot.hint?.mirror===key&&slot.hint.extensions?.syscon2EndpointId)return slot.hint.extensions.syscon2EndpointId;
   }}
   const oneEndpoint=endpoint(leader),twoEndpoint=endpoint(mirror);
   if(oneEndpoint)extensions.syscon1EndpointId=oneEndpoint;
   if(twoEndpoint)extensions.syscon2EndpointId=twoEndpoint;
   context.onDiscovery({kind:source?.kind??'concierge_route',roomId:context.roomId,epoch,leader,mirror,peers,...(Object.keys(extensions).length?{extensions}:{})});
  }
  function allEnded(){if(!closed&&slots.every(s=>s.ended)){status('exhausted');context.onClose?.({code:1006,reason:'all_doorways_exhausted',wasClean:false});}}
  function stop(index,reason){const slot=slots[index];if(closed||slot.ended)return;slot.generation++;slot.open=false;clearTimeout(slot.timer);clearTimeout(slot.admissionTimer);try{slot.session?.close();}catch{}slot.peers.clear();slot.hint=null;status('doorway_unavailable',{index,reason});roster();
   if(mode==='failover'&&index+1<slots.length){slot.ended=true;start(index+1);return;}
   if(recover&&!slot.retried){slot.retried=true;Promise.resolve().then(()=>recover(index)).then(adapter=>{if(closed)return;if(adapter){validateDoorway(adapter);if(adapter.protocol!=='phantom-doorway-v2')throw new TypeError('Recovery requires typed doorway adapter');slot.adapter=adapter;start(index);}else{slot.ended=true;allEnded();}}).catch(error=>{status('recovery_failed',{index,reason:error.message});slot.ended=true;allEnded();});}
   else{slot.ended=true;allEnded();}
  }
  function start(index){const slot=slots[index],token=++slot.generation;slot.ended=false;const live=()=>!closed&&token===slot.generation;
   slot.timer=setTimeout(()=>{if(live()&&!slot.open)stop(index,'open_timeout');},openTimeoutMs);
   try{slot.session=slot.adapter.open({...context,
    onOpen:()=>{if(!live()||slot.open)return;clearTimeout(slot.timer);slot.open=true;if(mode==='failover'&&!admitted&&index+1<slots.length)slot.admissionTimer=setTimeout(()=>{if(live()&&!admitted)stop(index,'admission_timeout');},admissionTimeoutMs);status('doorway_open',{index});if(!opened){opened=true;context.onOpen();}queueMicrotask(()=>{if(!live())return;try{if(own)slot.session.announce(own);if(authority)slot.session.publishAuthority?.(authority);}catch(error){status('doorway_error',{index,reason:error.message});}});},
    onDiscovery:hint=>{if(!live())return;if(hint.roomId!==undefined&&hint.roomId!==context.roomId)return;if(slots.length===1||mode==='failover'){context.onDiscovery(hint);return;}slot.hint=structuredClone(hint);
    const rows=Array.isArray(hint.peers)?hint.peers.filter(p=>p?.hKey):[];
    // Legacy switchboards can report only anchors. Those are reachability hints,
    // not a replacement for committed network authority or full identity proof.
    // Missing legacy birth times must not tie the existing leader with a newcomer.
    // This affects provisional routing only; committed authority above takes precedence.
    for(const key of [hint.leader,hint.mirror])if(key&&!rows.some(p=>p.hKey===key)){
      rows.push(slot.peers.get(key)??{hKey:key,from:key,roomJoinedAt:key===hint.leader?0:own?.t??0});
    }
    if(Array.isArray(hint.peers)||hint.leader||hint.mirror){slot.peers=new Map(rows.map(p=>[p.hKey,{...p,roomJoinedAt:Number.isSafeInteger(p.roomJoinedAt)?p.roomJoinedAt:Number.isSafeInteger(p.t)?p.t:slot.peers.get(p.hKey)?.roomJoinedAt??(p.hKey===hint.leader?0:own?.t??0)}]));roster(hint);}},
    onSignal:packet=>{if(!live()||packet?.videoId!==context.roomId)return;if(packet.to&&packet.to!==context.peerId)return;const id=JSON.stringify(packet);if(seen.has(id))return;seen.add(id);if(seen.size>4096)seen.delete(seen.values().next().value);context.onSignal(packet);},
    onClose:detail=>{if(live())stop(index,detail?.reason??'closed');},
    onError:error=>{if(live())status('doorway_error',{index,reason:error?.message??'transport_error'});},
    onStatus:s=>{if(!live())return;status('doorway_status',{index,detail:s});if(s.type==='exhausted')stop(index,'exhausted');},
    onPublication:receipt=>{if(live())context.onPublication?.({...receipt,doorwayIndex:index});}
   });for(const method of ['announce','reconcile','sendSignal','close'])if(typeof slot.session?.[method]!=='function')throw new TypeError('Invalid doorway session: '+method);
   }catch(error){stop(index,error.message);}
  }
  if(mode==='failover')start(0);else for(let i=0;i<slots.length;i++)start(i);
  return {
   observeNetworkState(state){if(state.state==='admitted'){admitted=true;for(const s of slots)clearTimeout(s.admissionTimer);}},
   announce(value){if(closed)throw new Error('Doorway closed');own=structuredClone(value);for(const s of slots)if(s.open)try{s.session.announce(own);}catch(e){status('doorway_error',{reason:e.message});}roster();},
   reconcile(value){for(const s of slots)if(s.open)try{s.session.reconcile(value);}catch(e){status('doorway_error',{reason:e.message});}roster();},
   sendSignal(packet){let sent=0;for(const s of slots)if(s.open)try{s.session.sendSignal(packet);sent++;}catch(e){status('signal_path_unavailable',{reason:e.message});}if(!sent)throw new Error('No doorway accepted signaling send');},
   publishAuthority(snapshot){authority=structuredClone(snapshot);for(const s of slots)if(s.open)try{s.session.publishAuthority?.(snapshot);}catch(e){status('authority_hint_error',{reason:e.message});}roster();},
   close(){if(closed)return;closed=true;for(const s of slots){s.generation++;clearTimeout(s.timer);clearTimeout(s.admissionTimer);try{s.session?.close();}catch{}}seen.clear();}
  };
 }});
}
