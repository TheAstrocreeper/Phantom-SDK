import {createDoorway} from './interface.js';

// A doorway's local second socket is not proof of the network's mirror.
// Enter as a candidate; a native leader offer can establish the pair direction.
export function withNativeEntry(doorway,{onStatus=()=>{}}={}) {
 const active=new Set();
 const adapter=createDoorway({discovery:true,presence:doorway.presence,open(context){
  let closed=false,admitted=false,lastHint,pairLeader;
  function discovery(hint){
   if(closed)return;
   lastHint=hint;
   const candidate=!admitted&&hint.leader&&hint.leader!==context.peerId&&hint.mirror===context.peerId&&pairLeader!==hint.leader;
   context.onDiscovery(candidate?{...hint,extensions:{...hint.extensions,bootstrapAdmission:true}}:hint);
  }
  const session=doorway.open({...context,onStatus:detail=>{onStatus(detail);context.onStatus?.(detail);},onDiscovery:discovery,onSignal:packet=>{
   if(closed)return;
   if(!admitted&&lastHint?.mirror===context.peerId&&packet.videoId===context.roomId&&packet.to===context.peerId&&packet.from===lastHint.leader&&packet.type==='rtc_offer'&&(packet.pipe==='sys'||packet.sysCarrier===true)){
    pairLeader=packet.from;context.onDiscovery(lastHint);
   }
   context.onSignal(packet);
  }});
  const result={
   observeNetworkState(state){if(state.state==='admitted')admitted=true;session.observeNetworkState?.(state);},
   announce:value=>session.announce(value),reconcile:value=>session.reconcile(value),sendSignal:value=>session.sendSignal(value),
   publishAuthority:value=>session.publishAuthority?.(value),
   close(){closed=true;active.delete(result);session.close();}
  };active.add(result);return result;
 }});
 return {...adapter,observeNetworkState:state=>{for(const s of active)s.observeNetworkState(state);}};
}
