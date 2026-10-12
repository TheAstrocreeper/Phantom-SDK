/** Harness-only transport loss. Freezes one engine without its graceful dispose path. */
export function createFaultRuntime(base) {
 let stopped=false;const pcs=new Set(),channels=new Set(),timeouts=new Set(),intervals=new Set();
 class RTC extends base.RTCPeerConnection {
  constructor(...args){super(...args);pcs.add(this);this.addEventListener?.("datachannel",event=>channels.add(event.channel));}
  createDataChannel(...args){const channel=super.createDataChannel(...args);channels.add(channel);return channel;}
 }
 const runtime={crypto:base.crypto,RTCPeerConnection:RTC,
  setTimeout(fn,ms,...args){const id=base.setTimeout(()=>{timeouts.delete(id);if(!stopped)fn(...args);},ms);timeouts.add(id);return id;},
  clearTimeout(id){base.clearTimeout(id);timeouts.delete(id);},
  setInterval(fn,ms,...args){const id=base.setInterval(()=>{if(!stopped)fn(...args);},ms);intervals.add(id);return id;},
  clearInterval(id){base.clearInterval(id);intervals.delete(id);}
 };
 function stop(){if(stopped)return;stopped=true;for(const id of timeouts)base.clearTimeout(id);for(const id of intervals)base.clearInterval(id);timeouts.clear();intervals.clear();
  for(const dc of channels){for(const name of ['onopen','onclose','onerror','onmessage','onbufferedamountlow'])dc[name]=null;}channels.clear();
  for(const pc of pcs){for(const name of ['onconnectionstatechange','oniceconnectionstatechange','onicecandidate','onsignalingstatechange','ondatachannel','onnegotiationneeded'])pc[name]=null;pc.close();}pcs.clear();}
 return {runtime,stop};
}
