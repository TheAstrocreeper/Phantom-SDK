import {createDoorway} from './interface.js';
import {verifyPeerProof} from './peer-proof.js';
export {createShortPairingHost,createShortPairingClient,createLanPairingHost,createLanPairingClient,createPinPairingHost,createPinPairingClient} from './short-pairing-prototype.js';
export {encodeLanPairingCode,parseLanPairingCode,newLanPairingPassword} from './lan-pairing-code.js';
const LIMIT = 65536;
const encode = bytes => { let text=''; for(const b of bytes) text+=String.fromCharCode(b); return btoa(text); };
const decode = text => Uint8Array.from(atob(text),c=>c.charCodeAt(0));
export {validateLocalEndpoint} from './local-endpoint.js';
import {validateLocalEndpoint} from './local-endpoint.js';
/** Encrypted ordered messages, signed identities, no discovery service or retry loop. */
export async function createLocalDoorways({identity,secret,hostKey,host,transport,onError=()=>{}}) {
  if(!/^[a-f0-9]{64}$/.test(secret)||! /^[a-f0-9]{64}$/.test(hostKey)||
    !/^[a-f0-9]{64}$/.test(identity?.publicKey)||typeof identity?.signPeerProof!=='function'||
    typeof host!=='boolean'||(host&&identity.publicKey!==hostKey)||
    typeof transport?.send!=='function'||typeof transport?.listen!=='function')throw new TypeError('Invalid local doorway configuration');
  const key=await crypto.subtle.importKey('raw',Uint8Array.from(secret.match(/../g),h=>parseInt(h,16)),'AES-GCM',false,['encrypt','decrypt']);
  const scope='phantom-local/2/'+hostKey;
  const sessions=new Map(), remote=new Map();
  let closed=false, remoteKey=host?null:hostKey, outgoing=0, incoming=0, sending=Promise.resolve(), reading=Promise.resolve(), pendingSend=0,pendingRead=0;
  function fail(error) { if(closed)return; try {onError(error);} finally {close();} }
  function send(message) {
    if(closed)return;
    if(++pendingSend>64){pendingSend--;fail(Error('Local signaling queue full'));return;}
    const sequence=++outgoing;
    sending=sending.then(async()=>{
      if(closed)return;
      const payload={sequence,host:!!host,message};
      const proof=identity.signPeerProof({networkId:scope,kind:'signal',credential:null,payload,issuedAt:Date.now(),nonce:crypto.randomUUID()});
      const iv=crypto.getRandomValues(new Uint8Array(12));
      const cipher=new Uint8Array(await crypto.subtle.encrypt({name:'AES-GCM',iv,additionalData:new TextEncoder().encode(scope)},key,new TextEncoder().encode(JSON.stringify(proof))));
      const bytes=new Uint8Array(12+cipher.length);bytes.set(iv);bytes.set(cipher,12);
      const envelope=encode(bytes);if(envelope.length>LIMIT)throw Error('Local signal too large');
      if(!closed)await transport.send(envelope);
    }).catch(fail).finally(()=>pendingSend--);
  }
  function discovery(entry) {
    if(closed||!entry.own)return;
    const rows=[entry.own,remote.get(entry.context.roomId)].filter(Boolean).map(value=>({...value,hKey:value.peerId,from:value.peerId,hex:value.hex,roomJoinedAt:value.t,videoId:value.roomId}));
    rows.sort((a,b)=>Number(b.hex===hostKey)-Number(a.hex===hostKey));
    entry.context.onDiscovery({kind:'concierge_route',roomId:entry.context.roomId,epoch:1,leader:rows[0]?.hKey,mirror:rows[1]?.hKey??null,peers:rows});
  }
  function receive(envelope) {
    if(closed)return;
    if(++pendingRead>64){pendingRead--;fail(Error('Local signaling queue full'));return;}
    reading=reading.then(async()=>{
      if(closed)return;
      if(typeof envelope!=='string'||envelope.length>LIMIT)throw Error('Invalid local envelope');
      const bytes=decode(envelope);
      const plain=await crypto.subtle.decrypt({name:'AES-GCM',iv:bytes.slice(0,12),additionalData:new TextEncoder().encode(scope)},key,bytes.slice(12));
      if(closed)return;
      const proof=JSON.parse(new TextDecoder().decode(plain)), payload=proof.payload;
      if(!verifyPeerProof(proof,{networkId:scope,kind:'signal'}) || payload.host===!!host || payload.sequence!==incoming+1 || (remoteKey&&proof.peerPublicKey!==remoteKey))throw Error('Local signal authentication failed');
      remoteKey??=proof.peerPublicKey;incoming++;
      const message=payload.message, {roomId,kind,value}=message;
      if(typeof roomId!=='string'||roomId.length>256)throw Error('Invalid local room');
      const entry=sessions.get(roomId);
      if(kind==='announce'){
        if(value?.roomId!==roomId||value.peerId!==remoteKey.slice(0,16)||value.hex!==remoteKey)throw Error('Invalid local announcement');
        if(!remote.has(roomId)&&remote.size>=16)throw Error('Too many local rooms');
        const previous=remote.get(roomId);remote.set(roomId,value);
        if(entry){discovery(entry);if(entry.own&&JSON.stringify(previous)!==JSON.stringify(value))send({roomId,kind:'announce',value:entry.own});}
      }else if(kind==='reconcile'){
        if(entry?.own)send({roomId,kind:'announce',value:entry.own});
      }else if(kind==='signal'){
        if(value?.from!==remoteKey.slice(0,16)||value.videoId!==roomId||(value.to&&value.to!==identity.publicKey.slice(0,16)))throw Error('Invalid local signal scope');
        entry?.context.onSignal(value);
      }else if(kind==='close'){remote.delete(roomId);if(entry)discovery(entry);}
      else throw Error('Invalid local message');
    }).catch(fail).finally(()=>pendingRead--);
  }
  let unlisten;
  unlisten=transport.listen(receive,fail);
  function close(){if(closed)return;closed=true;unlisten?.();transport.close?.();for(const entry of sessions.values())entry.context.onClose?.({reason:'local_closed'});sessions.clear();remote.clear();}
  return {close,doorway(){return {...createDoorway({discovery:true,open(context){
    if(closed||sessions.has(context.roomId)||sessions.size>=16)throw Error('Local channel unavailable');
    const entry={context,own:null};sessions.set(context.roomId,entry);
    queueMicrotask(()=>{if(!closed&&sessions.get(context.roomId)===entry){context.onOpen();send({roomId:context.roomId,kind:'reconcile'});}});
    return {
      announce(value){entry.own={...value,hex:identity.publicKey};discovery(entry);send({roomId:context.roomId,kind:'announce',value:entry.own});},
      reconcile(){send({roomId:context.roomId,kind:'reconcile'});},
      sendSignal(value){send({roomId:context.roomId,kind:'signal',value});},
      publishAuthority(){},
      close(){if(sessions.get(context.roomId)!==entry)return;sessions.delete(context.roomId);send({roomId:context.roomId,kind:'close'});},
    };
  }}),eventDriven:true};}};
}
export function browserLocalTransport({endpoint,token}, {fetch=globalThis.fetch}={}) {
  validateLocalEndpoint(endpoint);
  if(typeof token!=='string'||! /^[A-F0-9]{32}$/.test(token))throw new TypeError('Invalid local capability');
  const controller=new AbortController(), client=crypto.randomUUID();
  async function request(path,body='') {
    const response=await fetch(endpoint+path,{method:'POST',body,headers:{Authorization:`Bearer ${token}`,'Content-Type':'text/plain','X-Phantom-Client':client},signal:controller.signal,mode:'cors',credentials:'omit',redirect:'error',cache:'no-store',referrerPolicy:'no-referrer',targetAddressSpace:'local'});
    if(!response.ok)throw Error(`Local connection returned HTTP ${response.status}. Start a new invitation.`);
    return response;
  }
  return {send:async envelope=>{await request('/send',envelope);},close:()=>controller.abort(),listen(receive,fail){
    void (async()=>{try{while(!controller.signal.aborted){
      const response=await request('/receive'),reader=response.body.getReader();let size=0,text='';
      try{for(;;){const {done,value}=await reader.read();if(done)break;size+=value.length;if(size>LIMIT)throw Error('Local response too large');text+=new TextDecoder().decode(value);}}finally{await reader.cancel().catch(()=>{});}
      if(!controller.signal.aborted)receive(text);
    }}catch(error){if(!controller.signal.aborted)fail(error);}})();
    return ()=>controller.abort();
  }};
}
