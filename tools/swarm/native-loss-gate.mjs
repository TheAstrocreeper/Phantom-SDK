/** Harness-only asymmetric observation delay. SDP, ICE and RTC payloads stay native. */
export function createNativeLossGate(NativeRTC,{systemChannel,log=()=>{}}){
 const records=new Map();
 function getter(object,key){let proto=object;while(proto){const d=Object.getOwnPropertyDescriptor(proto,key);if(d?.get)return ()=>d.get.call(object);proto=Object.getPrototypeOf(proto);}throw Error('Missing native accessor: '+key);}
 function mask(object,key,value,held){const read=getter(object,key);Object.defineProperty(object,key,{configurable:true,get:()=>held()?value:read()});return read;}
 function channel(rec,dc){if(dc.label!==systemChannel||rec.channels.has(dc))return;rec.channels.add(dc);
  const read=mask(dc,'readyState','open',()=>rec.held),send=dc.send.bind(dc);
  dc.send=data=>{if(rec.held&&read()!=='open'){void log('fault.sys_send_held',{});return;}return send(data);};
  dc.addEventListener('close',event=>{if(rec.held){event.stopImmediatePropagation();void log('fault.sys_close_held',{});}},true);
 }
 class GatedRTC extends NativeRTC{
  constructor(...args){super(...args);const rec={pc:this,held:false,channels:new Set()};records.set(this,rec);
   mask(this,'connectionState','connected',()=>rec.held);mask(this,'iceConnectionState','connected',()=>rec.held);
   for(const type of ['connectionstatechange','iceconnectionstatechange'])this.addEventListener(type,event=>{if(rec.held){event.stopImmediatePropagation();void log('fault.sys_state_held',{type});}},true);
   this.addEventListener('datachannel',event=>channel(rec,event.channel));
  }
  createDataChannel(...args){const dc=super.createDataChannel(...args);channel(records.get(this),dc);return dc;}
  close(){const rec=records.get(this);if(rec)rec.held=false;records.delete(this);return super.close();}
 }
 return {RTCPeerConnection:GatedRTC,
  hold(){const found=[...records.values()].filter(r=>r.pc.connectionState==='connected'&&[...r.channels].some(c=>c.readyState==='open'));if(found.length!==1)throw Error('Expected exactly one live SYS edge, found '+found.length);found[0].held=true;void log('fault.sys_loss_gate',{action:'held'});return {held:1};},
  release(){let released=0;for(const rec of [...records.values()])if(rec.held){rec.held=false;released++;for(const dc of rec.channels)if(dc.readyState==='closed')dc.dispatchEvent(new Event('close'));rec.pc.dispatchEvent(new Event('connectionstatechange'));rec.pc.dispatchEvent(new Event('iceconnectionstatechange'));}void log('fault.sys_loss_gate',{action:'released',released});return {released};}
 };
}
