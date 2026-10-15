import { verifyNostrEnvelope } from './nostr-crypto.js';
/** Signed discovery/signaling bus. Does not elect leaders or confer room authority. */
export function openNostrBus({roomId,identity,relays,WebSocket=globalThis.WebSocket,
 onAnnouncement=()=>{},onSignal=()=>{},onSync=()=>{},onStatus=()=>{},onError=()=>{}}) {
 if(!roomId || !identity?.sign || !Array.isArray(relays)||!relays.length)throw new TypeError('Room, signing identity and explicit relays required');
 const urls=relays.map(url=>{const u=new URL(url);if(!['ws:','wss:'].includes(u.protocol))throw new TypeError('WebSocket relay required');return u.href;});
 let socket,closed=false,generation=0,index=0,chain=Promise.resolve(),announcement=null,departureId=null,writeBlocked=false,announcementFingerprint=null,announcementAt=0;
 const seen=new Set(),pending=new Map();
 const subscription=`phantom-${crypto.randomUUID()}`;
 function status(type,detail={}){onStatus({type,...detail});}
 function publish(payload) {
  if(closed||socket?.readyState!==1)throw new Error('Relay is not open');
  if(writeBlocked)throw new Error('Relay publication path rejected for this session');
  const event=identity.sign(roomId,payload);pending.set(event.id,{event,sentAt:Date.now()});
  if(pending.size>256){pending.delete(event.id);throw new Error('Too many unacknowledged relay publications');}
  socket.send(JSON.stringify(['EVENT',event]));return event.id;
 }
 function connect() {
  const token=++generation;
  if(index>=urls.length){status('exhausted');return;}
  const url=urls[index++];status('connecting',{url});
  try{socket=new WebSocket(url);}catch(error){onError(error);connect();return;}
  const current=socket;
  current.onopen=()=>{
   if(closed||token!==generation)return;
   current.send(JSON.stringify(['REQ',subscription,{kinds:[20001],'#t':[roomId],since:Math.floor(Date.now()/1000)-60,limit:100}]));
   announcementAt=0;if(announcement&&!writeBlocked){announcementAt=Date.now();publish(announcement);}status('open',{url});
  };
  current.onmessage=({data})=>{
   chain=chain.then(()=>{
    if(closed||token!==generation)return;
    let packet;try{packet=JSON.parse(data);}catch{return;}
    if(!Array.isArray(packet))return;
    if(packet[0]==='OK'){
     if(!pending.has(packet[1]))return;
     const {event,sentAt}=pending.get(packet[1]);
     const payload=JSON.parse(event.content);
     pending.delete(packet[1]);
     status('publish',{url,ackMs:Date.now()-sentAt,eventBytes:new TextEncoder().encode(JSON.stringify(event)).length,eventId:packet[1],messageId:payload.messageId,packetType:payload.type,signalType:payload.control?.type,attemptId:payload.control?.sysAttemptId,from:payload.control?.from,to:payload.control?.to,accepted:packet[2]===true,reason:packet[3]});
     if(packet[2]===false&&!writeBlocked){writeBlocked=true;pending.clear();status('write_blocked',{url,reason:packet[3]??'publication_rejected'});}
     if(packet[1]===departureId){closed=true;generation++;current.close();pending.clear();}
     return;
    }
    if(packet[0]==='NOTICE'){status('notice',{url,reason:packet[1]});return;}
    if(packet[0]==='AUTH'){status('authentication_required',{url});return;}
    if(packet[1]!==subscription)return;
    if(packet[0]==='EOSE'){onSync();return;} // Subscription remains open.
    if(packet[0]==='CLOSED'){status('subscription_closed',{reason:packet[2]});current.close();return;}
    if(packet[0]!=='EVENT')return;
    const p=verifyNostrEnvelope(packet[2],roomId);
    if(!p||seen.has(p.eventId)||p.nostrKey===identity.nostrKey)return;
    seen.add(p.eventId);if(seen.size>4096)seen.delete(seen.values().next().value);
    if(p.type==='ANNOUNCEMENT'){onAnnouncement(p);return;}
    if(p.targetNostrKey!==identity.nostrKey||p.targetS7pKey!==identity.publicKey)return;
    if(!p.control||typeof p.control!=='object'||Array.isArray(p.control))return;
    status('signal_received',{url,eventId:p.eventId,messageId:p.messageId,signalType:p.control.type,attemptId:p.control.sysAttemptId,from:p.control.from,to:p.control.to});
    onSignal(p);
   }).catch(onError);
  };
  current.onerror=()=>current.close();
  current.onclose=()=>{if(closed||token!==generation)return;pending.clear();status('closed',{url});if(departureId){closed=true;return;}connect();};
 }
 connect();
 return {
  announce(fields={}){const next={...fields,type:'ANNOUNCEMENT'},fingerprint=JSON.stringify(next);const changed=fingerprint!==announcementFingerprint;announcement=next;announcementFingerprint=fingerprint;if(!writeBlocked&&socket?.readyState===1&&(changed||Date.now()-announcementAt>=15000)){announcementAt=Date.now();return publish(announcement);}},
  sendSignal(target,control,messageId=crypto.randomUUID()){return publish({messageId,type:'SIGNAL',targetNostrKey:target.nostrKey,targetS7pKey:target.s7pPublicKey,control});},
  depart(fields){if(closed)return;if(writeBlocked||socket?.readyState!==1){closed=true;generation++;socket?.close();pending.clear();return;}departureId=publish({...fields,type:'ANNOUNCEMENT',departed:true});},
  reconnect(){if(closed)throw new Error('Bus closed');generation++;socket?.close();pending.clear();index=0;connect();},
  close(){if(closed)return;closed=true;generation++;if(socket?.readyState===1)socket.send(JSON.stringify(['CLOSE',subscription]));socket?.close();pending.clear();seen.clear();}
 };
}

/** A relay list is a shared meeting set, not an assumption of relay federation. */
export function openNostrRelaySet(config) {
 const seen=new Set(),children=[],urls=[],open=new Set(),publications=new Map(),exhausted=new Set(),blocked=new Set();let closed=false,announced;
 function submit(child,url,publication){
  if(publication.attempted.has(url))return;
  publication.attempted.add(url);
  try{child.sendSignal(publication.target,publication.control,publication.messageId);}catch(error){publication.attempted.delete(url);config.onError?.(error);}
 }
 function receive(callback,p){const key=JSON.stringify([p.nostrKey,p.type,p.messageId||p.eventId]);if(closed||seen.has(key))return;seen.add(key);if(seen.size>4096)seen.delete(seen.values().next().value);callback?.(p);}
 function addRelay(url) {
  if(closed||urls.includes(url)||urls.length>=8)return;urls.push(url);
  const child=openNostrBus({...config,relays:[url],
   onAnnouncement:p=>receive(config.onAnnouncement,p),onSignal:p=>receive(config.onSignal,p),
   onStatus:s=>{
    if(closed)return;
    if(s.type==='open'){if(blocked.has(url)){config.onStatus?.({...s,type:'relay_open',readOnly:true});return;}const first=open.size===0;open.add(url);config.onStatus?.({...s,type:first?'open':'relay_open'});for(const p of publications.values())submit(child,url,p);}
    else if(s.type==='write_blocked'){blocked.add(url);open.delete(url);config.onStatus?.(s);if(blocked.size+ [...exhausted].filter(value=>!blocked.has(value)).length===urls.length){publications.clear();config.onStatus?.({type:'exhausted',reason:'all_publication_paths_rejected'});}}
    else if(s.type==='closed'){open.delete(url);config.onStatus?.({...s,type:'relay_closed'});}
    else if(s.type==='exhausted'){exhausted.add(url);config.onStatus?.({...s,type:'relay_exhausted',url});if(new Set([...blocked,...exhausted]).size===urls.length)config.onStatus?.({type:'exhausted'});}
    else {if(s.type==='publish'&&s.accepted&&s.messageId)publications.delete(s.messageId);config.onStatus?.({...s,url});}
   }
  });children.push(child);if(announced)child.announce(announced);
 }
 for(const url of [...new Set(config.relays)])addRelay(url);
 return {
  addRelays(values){for(const value of values){try{const u=new URL(value);if(u.protocol==='wss:'&&!u.username&&!u.password&&!u.hash)addRelay(u.href);}catch{}}return [...urls];},
  announce(p){announced=p;for(const b of children)b.announce(p);},
  sendSignal(target,control){
   if(closed)throw new Error('Relay set closed');
   if(!open.size&&new Set([...blocked,...exhausted]).size===urls.length)throw new Error('No open relays');
   if(publications.size>=256)throw new Error('Too many unaccepted signaling publications');
   const p={messageId:crypto.randomUUID(),target:structuredClone(target),control:structuredClone(control),attempted:new Set()};
   publications.set(p.messageId,p);
   if(!open.size)config.onStatus?.({type:'signal_queued',messageId:p.messageId,reason:'relay_paths_opening',signalType:p.control.type,attemptId:p.control.sysAttemptId,pending:publications.size});
   for(const [i,url] of urls.entries())if(open.has(url))submit(children[i],url,p);
   return p.messageId;
  },
  depart(p){publications.clear();closed=true;for(const b of children)b.depart(p);seen.clear();},
  reconnect(){publications.clear();open.clear();exhausted.clear();for(const b of children){b.reconnect();if(announced)b.announce(announced);}},
  close(){publications.clear();closed=true;for(const b of children)b.close();seen.clear();}
 };
}
