/** Deterministic paired transport for engine lifecycle tests, not a browser/NAT test. */
export function memoryRTC({emitCandidates=false,onSend=()=>true,maxMessageSize=262144}={}){
 const offers=new Map();let serial=0;
 class Channel {
  constructor(label){this.label=label;this.readyState='connecting';this.bufferedAmount=0;}
  send(data){if(this.readyState!=='open')throw Error('channel closed');const bytes=typeof data==='string'?new TextEncoder().encode(data).byteLength:data.byteLength;if(bytes>maxMessageSize)throw Error('message too large');if(onSend(data,this)===false)return;queueMicrotask(()=>{if(this.other?.readyState==='open')this.other.onmessage?.({data});});}
  close(){if(this.readyState==='closed')return;this.readyState='closed';queueMicrotask(()=>this.onclose?.());if(this.other?.readyState!=='closed')this.other?.close();}
 }
 return class RTC {
  static appliedCandidates=[];
  constructor(){this.id=++serial;this.channels=[];this.listeners=new Map();this.connectionState='new';this.iceConnectionState='new';this.iceGatheringState='new';this.signalingState='stable';}
  addEventListener(type,fn){if(!this.listeners.has(type))this.listeners.set(type,new Set());this.listeners.get(type).add(fn);}
  removeEventListener(type,fn){this.listeners.get(type)?.delete(fn);}
  fire(type,event={}){this['on'+type]?.(event);for(const fn of this.listeners.get(type)??[])fn(event);}
  createDataChannel(label){const ch=new Channel(label);this.channels.push(ch);return ch;}
  async createOffer(){offers.set(this.id,this);return {type:'offer',sdp:String(this.id)};}
  async createAnswer(){return {type:'answer',sdp:this.remoteDescription.sdp};}
  async setLocalDescription(value){this.localDescription=value;this.signalingState=value.type==='offer'?'have-local-offer':'stable';this.fire('signalingstatechange');if(emitCandidates)setTimeout(()=>{if(this.connectionState!=='closed')this.fire('icecandidate',{candidate:{candidate:`candidate:${this.id} 1 udp 1 192.0.2.1 ${10000+this.id} typ host`,sdpMid:'0',sdpMLineIndex:0}});},0);}
  async setRemoteDescription(value){this.remoteDescription=value;this.signalingState=value.type==='offer'?'have-remote-offer':'stable';this.fire('signalingstatechange');if(value.type==='offer')this.other=offers.get(Number(value.sdp));else{
    const peer=this.other??[...offers.values()].find(pc=>pc.id===Number(value.sdp))?.answerer;
    if(!peer)return;
    queueMicrotask(()=>this.connect(peer));
   }
   if(value.type==='offer'&&this.other)this.other.answerer=this;
  }
  connect(peer){if(this.connectionState==='closed'||peer.connectionState==='closed')return;this.other=peer;peer.other=this;
   for(const pc of [this,peer]){pc.connectionState='connected';pc.iceConnectionState='connected';pc.sctp={state:'connected',maxMessageSize,transport:{state:'connected'}};pc.fire('connectionstatechange');pc.fire('iceconnectionstatechange');}
   for(const ch of this.channels){if(ch.other)continue;const remote=new Channel(ch.label);ch.other=remote;remote.other=ch;peer.channels.push(remote);peer.fire('datachannel',{channel:remote});for(const channel of [ch,remote]){channel.readyState='open';queueMicrotask(()=>channel.onopen?.());}}
  }
  async addIceCandidate(candidate){this.constructor.appliedCandidates.push(candidate.candidate);}
  async getStats(){return new Map();}
  close(){if(this.connectionState==='closed')return;this.connectionState='closed';this.iceConnectionState='closed';this.signalingState='closed';for(const ch of this.channels)ch.close();this.fire('connectionstatechange');if(this.other&&this.other.connectionState!=='closed'){this.other.connectionState='failed';this.other.iceConnectionState='failed';this.other.fire('connectionstatechange');}}
 };
}
