import {createAdmissionVerifier} from '../../../packages/phantom-core/src/admission.js';
import {verifyPeerProof} from '../../../packages/phantom-doorways/src/peer-proof.js';

const MAX_MESSAGE_BYTES = 128 * 1024;
const MAX_SOCKETS = 64;
const JOIN_TIMEOUT_MS = 10_000;
const REPLACEMENT_GRACE_MS = 10_000;
const PEER = /^[0-9a-f]{16}$/;
const NETWORK = /^[0-9a-f]{64}$/;
const SIGNALS = new Set(['rtc_offer','rtc_answer','rtc_ice','rtc_route_hint','veil_invite','veil_accept','veil_deny','sys_block','sys_unblock','sys_unblock_ack']);
const encoder = new TextEncoder();

function json(value,status=200) {
 return new Response(JSON.stringify(value),{status,headers:{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'}});
}
function mode(env) {
 const value=env.ADMISSION_MODE??'restricted';
 if(!['open','restricted'].includes(value))throw new Error('ADMISSION_MODE must be open or restricted');
 if(value==='restricted'&&!NETWORK.test(env.ADMISSION_AUTHORITY_PUBLIC_KEY??''))throw new Error('Configure ADMISSION_AUTHORITY_PUBLIC_KEY');
 return value;
}
function permittedOrigin(request,env) {
 const origin=request.headers.get('Origin');
 const allowed=String(env.ALLOWED_ORIGINS??'').split(',').map(s=>s.trim()).filter(Boolean);
 return !origin||!allowed.length||allowed.includes(origin);
}

/** One Durable Object per network. File and application data stay on peers. */
export default {
 async fetch(request,env) {
  const url=new URL(request.url);
  if(url.pathname==='/health'&&request.method==='GET')return json({service:'phantom-doorway',version:1});
  if(!['/ws','/discover'].includes(url.pathname)||request.method!=='GET')return json({error:'not_found'},404);
  if(request.headers.get('Upgrade')?.toLowerCase()!=='websocket')return json({error:'websocket_required'},426);
  if(!permittedOrigin(request,env))return json({error:'origin_denied'},403);
  if(!NETWORK.test(url.searchParams.get('v')??'')||!PEER.test(url.searchParams.get('to')??''))return json({error:'invalid_network_or_peer'},400);
  try{mode(env);}catch{return json({error:'admission_not_configured'},503);}
  const id=env.SWITCHBOARD.idFromName('v:'+url.searchParams.get('v'));
  return env.SWITCHBOARD.get(id).fetch(request);
 }
};

export class PhantomDoorway {
 constructor(state,env) {
  this.state=state;this.env=env;
  this.peers=new Map();this.sockets=new Map();this.pending=new Map();
  this.concierge={syscon1:null,syscon2:null,epoch:0};this.networkId=null;
  this.restricted=mode(env)==='restricted';
  this.verifyGrant=this.restricted?createAdmissionVerifier({authorityPublicKey:env.ADMISSION_AUTHORITY_PUBLIC_KEY}):null;
  this.ready=state.blockConcurrencyWhile(()=>this.restore());
 }
 async restore() {
  const saved=await this.state.storage.get(['networkId','concierge','pending']);
  this.networkId=saved.get('networkId')??null;
  this.concierge=saved.get('concierge')??this.concierge;
  this.pending=new Map(saved.get('pending')??[]);
  const live=this.state.getWebSockets().filter(socket=>socket.readyState===1).sort((a,b)=>this.attachment(a).connectedAt-this.attachment(b).connectedAt);
  for(const socket of live){
   const a=this.attachment(socket);
   if(!a.joined||a.networkId!==this.networkId)continue;
   const peer=await this.state.storage.get('peer:'+a.endpointId);
   if(!peer||peer.hKey!==a.hKey)continue;
   const previous=this.sockets.get(a.hKey);
   if(previous){this.unjoin(previous);previous.close(4000,'doorway_replaced');}
   this.sockets.set(a.hKey,socket);this.peers.set(a.hKey,peer);
  }
  for(const [key,pending]of this.pending){
   if(this.peers.has(key)){await this.state.storage.delete('peer:'+pending.endpointId);this.pending.delete(key);continue;}
   const peer=await this.state.storage.get('peer:'+pending.endpointId);
   if(peer)this.peers.set(key,peer);
  }
  this.refresh();await this.persist();await this.schedule();
 }
 async fetch(request) {
  await this.ready;
  const url=new URL(request.url),networkId=url.searchParams.get('v'),hKey=url.searchParams.get('to');
  if(!['/ws','/discover'].includes(url.pathname)||request.method!=='GET')return json({error:'not_found'},404);
  if(request.headers.get('Upgrade')?.toLowerCase()!=='websocket')return json({error:'websocket_required'},426);
  if(!NETWORK.test(networkId??'')||!PEER.test(hKey??'')||!permittedOrigin(request,this.env))return json({error:'invalid_request'},400);
  if(this.networkId&&this.networkId!==networkId)return json({error:'network_mismatch'},409);
  if(this.state.getWebSockets().filter(s=>s.readyState===1).length>=MAX_SOCKETS||(!this.peers.has(hKey)&&this.peers.size>=MAX_SOCKETS))return json({error:'capacity'},503);
  const [client,server]=Object.values(new WebSocketPair());
  const at=Date.now();
  // Identity is only a claim until concierge_connect is verified. Never replace a peer here.
  server.serializeAttachment({hKey,networkId,endpointId:crypto.randomUUID(),connectedAt:at,joinDeadline:at+JOIN_TIMEOUT_MS,joined:false});
  this.state.acceptWebSocket(server);
  await this.schedule();
  this.send(server,{type:'hello',connectionChallenge:this.attachment(server).endpointId,ts:at});
  return new Response(null,{status:101,webSocket:client});
 }
 async webSocketMessage(socket,message) {
  await this.ready;
  if(typeof message!=='string'||encoder.encode(message).length>MAX_MESSAGE_BYTES){socket.close(1009,'message_too_large');return;}
  let packet;try{packet=JSON.parse(message);}catch{return;}
  if(!packet||Array.isArray(packet)||typeof packet!=='object')return;
  const a=this.attachment(socket);
  if(packet.type==='ping'){this.send(socket,{type:'pong',ts:Date.now()});return;}
  if(packet.type==='concierge_connect'){await this.state.blockConcurrencyWhile(()=>this.join(socket,packet,a));return;}
  // Only the current, joined endpoint can route packets or mutate its own lease.
  if(!a.joined||this.sockets.get(a.hKey)!==socket||packet.from!==a.hKey||packet.videoId!==a.networkId)return;
  if(this.restricted&&!await this.validProof(packet.admissionProof,'signal',this.withoutProof(packet),a))return;
  if(packet.type==='presence'){this.send(socket,this.discovery('roster'));return;}
  if(packet.type==='doorway_reconcile'){this.send(socket,this.discovery('concierge_route'));return;}
  if(packet.type==='doorway_exit'){await this.remove(a.hKey,a.endpointId);this.unjoin(socket);socket.close(1000,'room_exit');return;}
  // Authority publications are optional reachability hints, not server room authority.
  if(packet.type==='doorway_authority')return;
  if(!SIGNALS.has(packet.type)||!PEER.test(packet.to??'')||packet.to===a.hKey)return;
  const target=this.sockets.get(packet.to);
  if(target?.readyState===1)this.send(target,packet); // Preserve signed fields verbatim.
  else if(!this.restricted&&packet.type==='rtc_offer'&&packet.sysCarrier===true)
   this.send(socket,{type:'rtc_undeliverable',signalType:'rtc_offer',target:packet.to,sysCarrier:true,epoch:Number(packet.epoch||0),at:Date.now()});
 }
 async join(socket,packet,a) {
  if(socket.readyState!==1)return;
  if(packet.from!==a.hKey||packet.videoId!==a.networkId||typeof packet.bootId!=='string'||packet.bootId.length>128||
    typeof packet.handle!=='string'||packet.handle.length>128||!Number.isSafeInteger(packet.t))return;
  const {type,videoId,from,admissionProof,...identity}=packet;
  const signed={...identity,roomId:videoId,peerId:from};
  if(this.restricted&&packet.connectionChallenge!==a.endpointId){socket.close(1008,'challenge_mismatch');return;}
  if(this.restricted&&!await this.validProof(admissionProof,'announce',signed,a)){socket.close(1008,'admission_denied');return;}
  if(this.networkId&&this.networkId!==a.networkId)return;
  if(!this.peers.has(a.hKey)&&this.peers.size>=MAX_SOCKETS){socket.close(1013,'capacity');return;}
  const previous=this.sockets.get(a.hKey),old=this.peers.get(a.hKey);
  const row={hKey:a.hKey,from:a.hKey,handle:packet.handle,bootId:packet.bootId,endpointId:a.endpointId,roomJoinedAt:packet.t,lastSeen:Date.now(),
   ...(admissionProof?{hex:admissionProof.peerPublicKey,admissionProof}: {})};
  // Proofs can exceed Cloudflare's socket-attachment limit: keep them in durable storage.
  await this.state.storage.put('peer:'+a.endpointId,row);
  this.networkId=a.networkId;
  socket.serializeAttachment({...a,joined:true});
  this.sockets.set(a.hKey,socket);this.peers.set(a.hKey,row);this.pending.delete(a.hKey);
  if(old&&old.endpointId!==a.endpointId)await this.state.storage.delete('peer:'+old.endpointId);
  if(previous&&previous!==socket){this.unjoin(previous);previous.close(4000,'doorway_replaced');}
  const first=!this.concierge.syscon1;this.refresh();await this.persist();await this.schedule();
  this.send(socket,this.discovery(first?'concierge_bootstrap_needed':'concierge_route'));
  this.broadcast(this.discovery('concierge_update'),socket);
 }
 async validProof(proof,kind,payload,a) {
  if(!verifyPeerProof(proof,{networkId:a.networkId,kind,payload})||proof.peerPublicKey.slice(0,16)!==a.hKey)return false;
  const c=proof.credential;
  if(!Number.isSafeInteger(c?.expiresAt)||c.expiresAt<=Date.now()||JSON.stringify(c).length>8192)return false;
  return this.verifyGrant({networkId:a.networkId,peerPublicKey:proof.peerPublicKey,credential:c});
 }
 async webSocketClose(socket) {
  await this.ready;
  const a=this.attachment(socket);this.unjoin(socket);
  if(!a.joined){await this.schedule();return;}
  const currentSocket=this.sockets.get(a.hKey),currentPeer=this.peers.get(a.hKey);
  if((currentSocket&&currentSocket!==socket)||(currentPeer&&currentPeer.endpointId!==a.endpointId))return;
  // A wake caused by close can omit that closed socket from getWebSockets().
  const peer=currentPeer??await this.state.storage.get('peer:'+a.endpointId);
  if(!peer||peer.endpointId!==a.endpointId)return;
  this.peers.set(a.hKey,peer);
  this.sockets.delete(a.hKey);
  this.pending.set(a.hKey,{endpointId:a.endpointId,dueAt:Date.now()+REPLACEMENT_GRACE_MS});
  this.refresh();
  await this.persist();await this.schedule();
 }
 async webSocketError() { /* An error alone is not proof of peer departure. */ }
 async alarm() {
  await this.ready;
  const now=Date.now();
  for(const socket of this.state.getWebSockets()){
   const a=this.attachment(socket);
   if(!a.joined&&socket.readyState===1&&a.joinDeadline<=now)socket.close(1008,'join_timeout');
  }
  for(const [key,p]of [...this.pending]){
   if(p.dueAt>now)continue;
   const current=this.peers.get(key);
   if(current?.endpointId===p.endpointId&&!this.sockets.has(key))await this.remove(key,p.endpointId);
   else{this.pending.delete(key);await this.state.storage.delete('peer:'+p.endpointId);}
  }
  await this.persist();await this.schedule();
 }
 async remove(key,endpointId) {
  if(this.peers.get(key)?.endpointId!==endpointId)return;
  this.peers.delete(key);this.sockets.delete(key);this.pending.delete(key);
  await this.state.storage.delete('peer:'+endpointId);
  this.refresh();await this.persist();this.broadcast(this.discovery('concierge_update'));
 }
 attachment(socket){try{return socket.deserializeAttachment()??{};}catch{return {};}}
 unjoin(socket){socket.serializeAttachment({...this.attachment(socket),joined:false});}
 withoutProof(packet){const {admissionProof,...payload}=packet;return payload;}
 refresh() {
  const keys=[...this.peers.keys()].sort();
  const old=this.concierge;
  const one=keys.includes(old.syscon1)?old.syscon1:keys.includes(old.syscon2)?old.syscon2:keys[0]??null;
  const two=keys.includes(old.syscon2)&&old.syscon2!==one?old.syscon2:keys.find(k=>k!==one)??null;
  if(one!==old.syscon1||two!==old.syscon2)this.concierge={syscon1:one,syscon2:two,epoch:old.epoch+1};
 }
 discovery(type) {
  const list=[...this.peers.values()].sort((a,b)=>a.roomJoinedAt-b.roomJoinedAt||a.hKey.localeCompare(b.hKey));
  return {type,videoId:this.networkId,...this.concierge,list,
   syscon1EndpointId:this.peers.get(this.concierge.syscon1)?.endpointId??null,
   syscon2EndpointId:this.peers.get(this.concierge.syscon2)?.endpointId??null,t:Date.now()};
 }
 async persist(){await this.state.storage.put({networkId:this.networkId,concierge:this.concierge,pending:[...this.pending]});}
 async schedule() {
  const times=[...this.pending.values()].map(p=>p.dueAt);
  for(const socket of this.state.getWebSockets()){const a=this.attachment(socket);if(socket.readyState===1&&!a.joined)times.push(a.joinDeadline);}
  if(times.length)await this.state.storage.setAlarm(Math.min(...times));
  else await this.state.storage.deleteAlarm();
 }
 send(socket,packet){try{socket.send(JSON.stringify(packet));}catch{}}
 broadcast(packet,except){for(const socket of this.sockets.values())if(socket!==except&&socket.readyState===1)this.send(socket,packet);}
}
