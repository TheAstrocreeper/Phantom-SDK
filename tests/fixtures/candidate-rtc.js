import {RTC,rtc} from './native-network.js';
// Unlike the original fixture, SDP alone cannot open a channel. Each side
// must receive a candidate for the current remote PC. Candidates leave before
// the offer/answer, as native setLocalDescription can start gathering early.
export class CandidateRTC extends RTC {
 async setLocalDescription(d){
  await super.setLocalDescription(d);
  this.onicecandidate?.({candidate:{candidate:this.id}});
  this.maybeConnect();
 }
 async setRemoteDescription(d){
  if(d.type==='offer')await super.setRemoteDescription(d);
  else{this.remoteDescription=d;this.signalingState='stable';this.remote=rtc.get(d.sdp);}
  this.maybeConnect();
 }
 async addIceCandidate(c){
  if(!this.remoteDescription)throw Error('remote description missing');
  if(c.candidate!==this.remoteDescription.sdp)throw Error('wrong ICE generation');
  this.hasCandidate=true;this.maybeConnect();
 }
 maybeConnect(){
  const peer=this.remote;
  if(!peer||!this.hasCandidate||!peer.hasCandidate||!this.localDescription||!peer.localDescription||
     !this.remoteDescription||!peer.remoteDescription||this.signalingState==='closed'||peer.signalingState==='closed')return;
  for(const pc of [this,peer]){
   if(pc.connectionState==='connected')continue;
   pc.connectionState='connected';pc.iceConnectionState='connected';
   for(const c of pc.channels){c.readyState='open';queueMicrotask(()=>c.onopen?.());}
   pc.onconnectionstatechange?.();
  }
 }
}
