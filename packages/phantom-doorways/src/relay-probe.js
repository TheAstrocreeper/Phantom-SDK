import {createNostrIdentity} from './nostr-crypto.js';
import {openNostrBus} from './nostr.js';
export function normalizeRelays(values,limit=64) {
 const result=[];
 for(const value of values){try{const u=new URL(value);if(u.protocol!=='wss:'||u.username||u.password||u.hash)continue;if(!result.includes(u.href))result.push(u.href);if(result.length>=limit)break;}catch{}}
 return result;
}
/** Two independent subscriptions and signing identities; no WebRTC/NAT claim. */
export function probeRelay(url,{timeoutMs=8000,secondPeerDelayMs=0,idleReconnectMs=null,payloadBytes=0,WebSocket=globalThis.WebSocket,identityFactory=createNostrIdentity,busFactory=openNostrBus}={}) {
 if(!Number.isInteger(payloadBytes)||payloadBytes<0||payloadBytes>16384)throw new RangeError('Probe payload must be 0 to 16384 ASCII bytes');
 if(idleReconnectMs!==null&&(!Number.isFinite(idleReconnectMs)||idleReconnectMs<0||idleReconnectMs>=timeoutMs))throw new RangeError('Idle interval must be within timeout');
 if(!Number.isFinite(secondPeerDelayMs)||secondPeerDelayMs<0||secondPeerDelayMs>=timeoutMs)throw new RangeError('Second peer delay must be within the timeout');
 if(normalizeRelays([url]).length!==1) return Promise.resolve({url,passed:false,reason:'invalid_relay'});
 return new Promise(resolve=>{
  const start=Date.now(),roomId=crypto.randomUUID(),identities=[identityFactory(),identityFactory()];
  const buses=[],opened=new Set(),announced=new Set(),received=new Set(),accepted=new Set();let sent=false,done=false,secondTimer,phaseTimer,stage='initial';const stages=[];
  const diagnostics=[],timeline=[];
  const record=(peer,type,detail={})=>{if(timeline.length<128)timeline.push({atMs:Date.now()-start,peer,type,stage,...detail});};
  const announcement=i=>({probe:roomId,probeStage:stage,row:{hKey:identities[i].publicKey.slice(0,16),hex:identities[i].publicKey,bootId:roomId,roomJoinedAt:start}});
  const finish=(passed,reason)=>{if(done)return;done=true;clearTimeout(timer);clearTimeout(secondTimer);clearTimeout(phaseTimer);for(const bus of buses)try{bus.close();}catch{}const timings=[0,1].map(peer=>{const first=type=>timeline.find(e=>e.peer===peer&&e.type===type)?.atMs;const sent=timeline.find(e=>e.peer===1-peer&&e.type==='signal_sent')?.atMs;const delivered=first('signal_delivered');return {peer,openedMs:first('open'),eoseMs:first('subscription_eose'),discoveredMs:first('peer_discovered'),deliveryMs:delivered===undefined||sent===undefined?undefined:delivered-sent};});resolve({url,passed,reason,secondPeerDelayMs,idleReconnectMs,payloadBytes,stages,timings,latencyMs:Date.now()-start,announcementsReceived:announced.size,signalsReceived:received.size,signalsAccepted:accepted.size,timeline,diagnostics});};
  const timer=setTimeout(()=>finish(false,'delivery_timeout'),timeoutMs);
  function reset(){sent=false;received.clear();accepted.clear();}
  function check(){if(done||received.size!==2||accepted.size!==2||stage==='idle_wait')return;
   stages.push({stage,completedMs:Date.now()-start});
   if(idleReconnectMs===null||stage==='reconnect'){finish(true,idleReconnectMs===null?'bidirectional_signed_delivery':'idle_and_reconnect_delivery');return;}
   if(stage==='initial'){stage='idle_wait';record(null,'idle_started',{idleMs:idleReconnectMs});phaseTimer=setTimeout(()=>{stage='after_idle';reset();send();},idleReconnectMs);return;}
   stage='reconnect';reset();announced.clear();opened.delete(1);record(1,'reconnect_requested');
   try{buses[1].reconnect();buses[1].announce(announcement(1));}catch(e){finish(false,e.message);}
  }
  function send(){if(done||stage==='idle_wait'||sent||announced.size!==2||opened.size!==2)return;sent=true;for(let i=0;i<2;i++){const other=identities[1-i];try{record(i,'signal_sent');buses[i].sendSignal({nostrKey:other.nostrKey,s7pPublicKey:other.publicKey},{type:'phantom_doorway_probe',from:identities[i].publicKey.slice(0,16),to:other.publicKey.slice(0,16),videoId:roomId,probe:roomId,probeStage:stage,...(payloadBytes?{padding:'x'.repeat(payloadBytes)}:{})},`probe-${stage}-${i}`);}catch(e){finish(false,e.message);}}}
  function openPeer(i){if(done)return;try{
   const identity=identities[i],other=identities[1-i];
   const bus=busFactory({roomId,identity,relays:[url],WebSocket,
    onAnnouncement:p=>{if(p.s7pPublicKey===other.publicKey&&p.probe===roomId&&(stage!=='reconnect'||p.probeStage===stage)){if(!announced.has(i)){announced.add(i);record(i,'peer_discovered');buses[i]?.announce({...announcement(i),probeReply:true});}send();}},
    onSignal:p=>{if(p.s7pPublicKey===other.publicKey&&p.control?.probe===roomId&&p.control.probeStage===stage&&(payloadBytes===0||p.control.padding==='x'.repeat(payloadBytes))&&p.control.to===identity.publicKey.slice(0,16)){received.add(i);record(i,'signal_delivered');check();}},
    onSync:()=>record(i,'subscription_eose'),
    onStatus:s=>{record(i,s.type,s);if(s.type==='open'){opened.add(i);if(i===0&&secondPeerDelayMs&&!secondTimer)secondTimer=setTimeout(()=>openPeer(1),secondPeerDelayMs);send();}if(s.type==='write_blocked'||s.type==='publication_paused')finish(false,'publication_rejected: '+s.reason);if(s.type==='publish'&&s.packetType==='SIGNAL'){if(s.accepted){accepted.add(i);check();}else finish(false,'publication_rejected: '+s.reason);}if(s.type==='subscription_closed')finish(false,'subscription_closed: '+s.reason);if(s.type==='exhausted')finish(false,'connection_exhausted');},
    onError:e=>{record(i,'error',{message:e.message});if(diagnostics.length<5)diagnostics.push(e.message);}
   });buses[i]=bus;if(done){bus.close();return;}bus.announce(announcement(i));
  }catch(e){finish(false,e.message);}}
  openPeer(0);if(!secondPeerDelayMs)openPeer(1);
 });
}
export async function probeRelays(urls,{concurrency=4,onResult=()=>{},...options}={}) {
 const results=new Array(urls.length);let next=0;
 await Promise.all(Array.from({length:Math.min(concurrency,urls.length)},async()=>{while(next<urls.length){const i=next++;results[i]=await probeRelay(urls[i],options);onResult(results[i]);}}));
 return results;
}
