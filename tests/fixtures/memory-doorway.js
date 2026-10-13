import {createDoorway} from '../../packages/phantom-doorways/src/interface.js';
// A developer adapter with no Nostr envelope or CF socket protocol. Discovery
// cannot prove absence. Delivery deliberately allows ICE ahead of offers.
export function memoryDoorway({copies=1}={}) {
 const sessions=new Map(),identities=new Map();let generation=0,paused=false;
 function roster(){
  const rows=[...sessions.values()].filter(s=>s.row).map(s=>s.row);
  for(const s of sessions.values())s.c.onDiscovery({kind:'concierge_route',roomId:s.c.roomId,
   epoch:rows.length,leader:rows[0]?.hKey,mirror:rows[1]?.hKey??null,peers:rows});
 }
 const adapter=createDoorway({discovery:true,open(c){
  const s={c,row:null,generation:String(++generation)};sessions.set(c.peerId,s);
  queueMicrotask(c.onOpen);
  return {
   announce(value){s.row={...value,hKey:c.peerId,from:c.peerId,videoId:c.roomId,hex:identities.get(c.peerId),roomJoinedAt:value.t};roster();},
   reconcile(){roster();},
   sendSignal(packet){const target=sessions.get(packet.to);if(paused||!target)return;
    if(packet.type==='rtc_offer')setImmediate(()=>{if(!paused)for(let i=0;i<copies;i++)target.c.onSignal(packet);});
    else queueMicrotask(()=>{if(!paused)target.c.onSignal(packet);});},
   close(){sessions.delete(c.peerId);}
  };
 }});
 return {register(identity){identities.set(identity.publicKey.slice(0,16),identity.publicKey);return adapter;},pause(){paused=true;},resume(){paused=false;
  for(const remote of sessions.values()){remote.generation=String(++generation);
   for(const local of sessions.values())if(local!==remote)local.c.onStatus({type:'peer_available',verified:true,
    roomId:local.c.roomId,peerId:remote.c.peerId,bootId:remote.row.bootId,availabilityId:remote.generation});}
 }};
}
