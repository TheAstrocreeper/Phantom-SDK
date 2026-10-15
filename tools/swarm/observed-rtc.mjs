/** Keep only live native objects; cumulative diagnostics retain scalar counts. */
export function observeRTC(NativeRTC,log){
 const active=new Set(),counts={created:0,closed:0,constructionFailed:0};
 function release(pc){if(active.delete(pc))counts.closed++;}
 class ObservedRTC extends NativeRTC {
  constructor(...args){try{super(...args);}catch(error){counts.constructionFailed++;void log('rtc.constructionFailed',{error:error.message,...counts,live:active.size});throw error;}counts.created++;active.add(this);this.addEventListener('connectionstatechange',()=>{if(this.connectionState==='closed')release(this);void log('rtc.state',{state:this.connectionState,...counts,live:active.size});});}
  close(){try{return super.close();}finally{release(this);}}
 }
 return {RTCPeerConnection:ObservedRTC,active,counts};
}
