import {createNostrIdentity} from './nostr-crypto.js';
import {openNostrBus} from './nostr.js';
export function normalizeRelays(values,limit=64) {
 const result=[];
 for(const value of values){try{const u=new URL(value);if(u.protocol!=='wss:'||u.username||u.password||u.hash)continue;if(!result.includes(u.href))result.push(u.href);if(result.length>=limit)break;}catch{}}
 return result;
}
/** Two independent subscriptions and signing identities; no WebRTC/NAT claim. */
export function probeRelay(url,{timeoutMs=8000,WebSocket=globalThis.WebSocket,identityFactory=createNostrIdentity,busFactory=openNostrBus}={}) {
 if(normalizeRelays([url]).length!==1) return Promise.resolve({url,passed:false,reason:'invalid_relay'});
 return new Promise(resolve=>{
  const start=Date.now(),roomId=crypto.randomUUID(),identities=[identityFactory(),identityFactory()];
  const buses=[],opened=new Set(),announced=new Set(),received=new Set(),accepted=new Set();let sent=false,done=false;
  const diagnostics=[];
  const finish=(passed,reason)=>{if(done)return;done=true;clearTimeout(timer);for(const bus of buses)try{bus.close();}catch{}resolve({url,passed,reason,latencyMs:Date.now()-start,announcementsReceived:announced.size,signalsReceived:received.size,signalsAccepted:accepted.size,diagnostics});};
  const timer=setTimeout(()=>finish(false,'delivery_timeout'),timeoutMs);
  function check(){if(received.size===2&&accepted.size===2)finish(true,'bidirectional_signed_delivery');}
  function send(){if(sent||announced.size!==2||opened.size!==2)return;sent=true;for(let i=0;i<2;i++){const other=identities[1-i];try{buses[i].sendSignal({nostrKey:other.nostrKey,s7pPublicKey:other.publicKey},{type:'phantom_doorway_probe',from:identities[i].publicKey.slice(0,16),to:other.publicKey.slice(0,16),videoId:roomId,probe:roomId},`probe-${i}`);}catch(e){finish(false,e.message);}}}
  try{for(let i=0;i<2;i++){
   const identity=identities[i],other=identities[1-i];
   const bus=busFactory({roomId,identity,relays:[url],WebSocket,
    onAnnouncement:p=>{if(p.s7pPublicKey===other.publicKey&&p.probe===roomId){announced.add(i);send();}},
    onSignal:p=>{if(p.s7pPublicKey===other.publicKey&&p.control?.probe===roomId&&p.control.to===identity.publicKey.slice(0,16)){received.add(i);check();}},
    onStatus:s=>{if(s.type==='open'){opened.add(i);send();}if(s.type==='write_blocked')finish(false,'publication_rejected: '+s.reason);if(s.type==='publish'&&s.packetType==='SIGNAL'){if(s.accepted){accepted.add(i);check();}else finish(false,'publication_rejected: '+s.reason);}if(s.type==='subscription_closed')finish(false,'subscription_closed: '+s.reason);if(s.type==='exhausted')finish(false,'connection_exhausted');},
    onError:e=>{if(diagnostics.length<5)diagnostics.push(e.message);}
   });buses.push(bus);bus.announce({probe:roomId,row:{hKey:identity.publicKey.slice(0,16),hex:identity.publicKey,bootId:roomId,roomJoinedAt:start}});
  }}catch(e){finish(false,e.message);}
 });
}
export async function probeRelays(urls,{concurrency=4,onResult=()=>{},...options}={}) {
 const results=new Array(urls.length);let next=0;
 await Promise.all(Array.from({length:Math.min(concurrency,urls.length)},async()=>{while(next<urls.length){const i=next++;results[i]=await probeRelay(urls[i],options);onResult(results[i]);}}));
 return results;
}
