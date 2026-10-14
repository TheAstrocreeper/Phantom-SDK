export function transportConfig({nostr=false,relays,cf}={}){
 if(!nostr&&relays!==undefined)throw Error('--relays require --nostr');
 const secure=value=>{const url=new URL(value);if(url.protocol!=='wss:'||url.username||url.password||url.hash)throw Error('Live relay URLs must be credential-free wss');return url.href;};
 if(!nostr)return cf?{mode:'cloudflare',doorways:[{type:'websocket',url:secure(cf)}]}:{mode:'local',doorways:[{type:'harness'}]};
 let selected;if(relays!==undefined){selected=[...new Set(relays.split(',').map(secure))];if(!selected.length||selected.length>8)throw Error('Supply 1–8 Nostr relay URLs');}
 return {mode:cf?'nostr-with-cf-fallback':'nostr',doorways:[{type:'nostr',...(selected?{relays:selected}:{discover:true})},...(cf?[{type:'websocket',url:secure(cf)}]:[])]};
}
/** Test observation/fault injection wraps native sockets; the SDK owns adapters. */
export function observeSignaling(NativeWebSocket,log){
 let paused=false;const counts={sent:0,received:0,dropped:0};
 const describe=data=>{let packet;try{packet=JSON.parse(data);}catch{}return {bytes:typeof data==='string'?data.length:0,type:Array.isArray(packet)?packet[0]:packet?.type??'unknown',...(packet?.[0]==='OK'?{eventId:packet[1],accepted:packet[2]===true,reason:packet[3]}:{}),...(packet?.[0]==='EVENT'?{eventId:packet.at(-1)?.id,kind:packet.at(-1)?.kind}:{})};};
 class Observed extends NativeWebSocket {
  constructor(...args){super(...args);const surface=String(args[0]);this.addEventListener('open',()=>void log('signaling.open',{surface}));this.addEventListener('close',event=>void log('signaling.close',{surface,code:event.code,reason:event.reason,wasClean:event.wasClean}));this.addEventListener('error',()=>void log('signaling.error',{surface}));this.addEventListener('message',event=>{if(paused){counts.dropped++;event.stopImmediatePropagation();void log('signaling.dropped',{surface,direction:'rx',...describe(event.data)});return;}counts.received++;void log('signaling.rx',{surface,...describe(event.data)});});}
  send(data){if(paused){counts.dropped++;void log('signaling.dropped',{surface:this.url,direction:'tx',...describe(data)});return;}counts.sent++;void log('signaling.tx',{surface:this.url,...describe(data)});return super.send(data);}
 }
 return {WebSocket:Observed,counts,setPaused(value){paused=!!value;void log('fault.signaling',{paused,scope:'SDK socket frames; native sockets remain open'});}};
}
