import test from 'node:test';
import assert from 'node:assert/strict';
import {createNostrIdentity} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {createNostrDoorway} from '../packages/phantom-doorways/src/nostr-doorway.js';
const turn=()=>new Promise(resolve=>setImmediate(resolve));
test('relay hints respect authorization, URL bounds, and session close',async()=>{
 const identity=createNostrIdentity(),other=createNostrIdentity();let callbacks,resolveRefresh;const added=[];let authorized=false;
 const session=createNostrDoorway({identity,relays:['wss://shared.example/'],refreshRelays:()=>new Promise(resolve=>{resolveRefresh=resolve;}),canFollowRelayHint:()=>authorized,busFactory:config=>{callbacks=config;return {announce(){},close(){},addRelays:values=>{added.push(values);return values;}};}}).open({roomId:'room',peerId:identity.publicKey.slice(0,16),onDiscovery(){},onSignal(){},onOpen(){},onClose(){},onError(){}});
 session.announce({roomId:'room',peerId:identity.publicKey.slice(0,16),bootId:'self',t:1});
 const p={s7pPublicKey:other.publicKey,row:{hKey:other.publicKey.slice(0,16),hex:other.publicKey,bootId:'remote',roomJoinedAt:2},relays:['wss://alternative.example/','ws://insecure.example/','wss://user:password@secret.example/']};
 callbacks.onAnnouncement(p);assert.equal(added.length,0);
 authorized=true;callbacks.onAnnouncement(p);assert.deepEqual(added[0],['wss://shared.example/','wss://alternative.example/']);
 callbacks.onAnnouncement({...p,relays:Array(9).fill('wss://overflow.example/')});assert.equal(added.length,1);
 callbacks.onStatus({type:'write_blocked'});await turn();session.close();resolveRefresh(['wss://late.example/']);await turn();assert.equal(added.length,1);
});
function relays(){
 return class Socket {
  static all=[];static rejected=new Set();
  constructor(url){this.url=url;this.readyState=0;Socket.all.push(this);queueMicrotask(()=>{this.readyState=1;this.onopen?.();});}
  send(raw){const p=JSON.parse(raw);if(p[0]==='REQ'){this.subscription=p[1];return;}if(p[0]!=='EVENT')return;
   const event=p[1],accepted=!Socket.rejected.has(this.url+':'+event.pubkey);
   queueMicrotask(()=>{this.onmessage?.({data:JSON.stringify(['OK',event.id,accepted,accepted?'':'rate limited'])});
    if(accepted)for(const socket of Socket.all)if(socket.url===this.url&&socket.readyState===1&&socket.subscription)socket.onmessage?.({data:JSON.stringify(['EVENT',socket.subscription,event])});
   });
  }
  close(){this.readyState=3;this.onclose?.();}
 };
}
test('partial write rejection shares alternatives before last common relay is lost',async()=>{
 const Socket=relays(),a=createNostrIdentity(),b=createNostrIdentity(),room='coordination',old=['wss://restricted.example/','wss://shared.example/'],replacement='wss://replacement.example/';
 const sessions=[],received=[[],[]],statuses=[[],[]];let refreshes=0;
 for(const [i,identity] of [a,b].entries()){
  const doorway=createNostrDoorway({identity,relays:old,WebSocket:Socket,refreshRelays:async()=>{refreshes++;return i===0?[replacement]:[];}});
  sessions.push(doorway.open({roomId:room,peerId:identity.publicKey.slice(0,16),onOpen(){},onDiscovery(){},onClose(){},onError:e=>{throw e;},onSignal:p=>received[i].push(p),onStatus:s=>statuses[i].push(s)}));
  sessions[i].announce({roomId:room,peerId:identity.publicKey.slice(0,16),bootId:'boot-'+i,t:i+1});
 }
 try{
  for(let i=0;i<8;i++)await turn();
  Socket.rejected.add(old[0]+':'+a.nostrKey);
  sessions[0].publishAuthority({room,from:a.publicKey.slice(0,16),syscon1:a.publicKey.slice(0,16),syscon2:b.publicKey.slice(0,16),epoch:1,rosterSeq:1,roster:[{hKey:a.publicKey.slice(0,16)},{hKey:b.publicKey.slice(0,16)}]});
  for(let i=0;i<12;i++)await turn();
  assert.equal(Socket.all.filter(s=>s.url===replacement&&s.readyState===1).length,2,'healthy peer must follow the signed alternative coordinates');
  assert.equal(refreshes,1,'only the degraded peer needs a directory refresh');
  Socket.rejected.add(old[1]+':'+a.nostrKey);
  const offer={type:'rtc_offer',videoId:room,from:a.publicKey.slice(0,16),to:b.publicKey.slice(0,16),sysAttemptId:'recovery'};
  sessions[0].sendSignal(offer);for(let i=0;i<8;i++)await turn();
  assert.deepEqual(received[1],[offer]);
  const answer={...offer,type:'rtc_answer',from:offer.to,to:offer.from};sessions[1].sendSignal(answer);for(let i=0;i<8;i++)await turn();
  assert.deepEqual(received[0],[answer]);assert.equal(statuses[0].some(s=>s.type==='exhausted'),false);
 }finally{for(const s of sessions)s.close();}
});
