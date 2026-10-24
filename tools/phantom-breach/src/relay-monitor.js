/** Observe real browser relay sockets; cutting prevents new socket constructors. */
export function createRelayMonitor(Base){
 const sockets=new Set();let cut=false;
 class MonitoredSocket extends Base{
  constructor(...args){if(cut)throw Error('Signaling deliberately disconnected');super(...args);sockets.add(this);this.addEventListener('close',()=>sockets.delete(this),{once:true});}
 }
 return {WebSocket:MonitoredSocket,get disconnected(){return cut;},count:()=>[...sockets].filter(s=>s.readyState!==Base.CLOSED).length,
  async disconnect(){cut=true;for(const s of sockets)try{s.close(1000,'application_signaling_cut');}catch{}
   const deadline=Date.now()+5000;while(this.count()&&Date.now()<deadline)await new Promise(r=>setTimeout(r,25));
   if(this.count())throw Error('Relay sockets have not confirmed closed');return 0;
  }};
}
