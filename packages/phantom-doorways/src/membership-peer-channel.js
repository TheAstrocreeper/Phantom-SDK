/** Membership RPC over authenticated SDK messaging; pairing remains bootstrap/fallback. */
export function openMembershipPeerChannel({network,remotePublicKey,onRequest,onEvent=()=>{},timeoutMs=2000}) {
 const app='phantom-membership/1',pending=new Map();let closed=false,working=0;
 const peer=remotePublicKey?.slice(0,16);
 const ready=id=>!closed&&network.getPeerConnections().some(p=>p.peerId===id&&p.state==='ready');
 function send(to,body){const text=JSON.stringify({app,...body});if(text.length>30000)throw Error('Membership message too large');if(!ready(to))throw Error('Membership peer unavailable');network.send(to,text);}
 const off=network.on('message',({from,text})=>{if(closed||typeof text!=='string'||text.length>30000)return;
  let m;try{m=JSON.parse(text);}catch{return;}if(m.app!==app||!ready(from))return;
  if(remotePublicKey){
   if(from!==peer)return;
   if(m.kind==='event'){onEvent(m.data);return;}
   const row=pending.get(m.id);if(m.kind!=='response'||!row)return;
   row.finish(m.error?new Error(String(m.error).slice(0,200)):null,m.data);return;
  }
  if(m.kind!=='request'||typeof m.id!=='string'||m.id.length>64||working>=16||typeof m.action!=='string')return;
  const key=m.data?.request?.peerPublicKey;
  if(typeof key!=='string'||!/^[0-9a-f]{64}$/.test(key)||key.slice(0,16)!==from)return;
  working++;
  void Promise.resolve().then(()=>onRequest({action:m.action,data:m.data,peerPublicKey:key})).then(data=>send(from,{kind:'response',id:m.id,data}),error=>send(from,{kind:'response',id:m.id,error:error.message})).catch(()=>{}).finally(()=>working--);
 });
 return {
  ready:()=>ready(peer),
  request(action,data,{signal}={}){return new Promise((resolve,reject)=>{
   if(closed||signal?.aborted||pending.size>=16){reject(Error('Membership request unavailable'));return;}
   const id=crypto.randomUUID();let timer;
   const finish=(error,value)=>{if(!pending.delete(id))return;clearTimeout(timer);signal?.removeEventListener('abort',abort);error?reject(error):resolve(value);};
   const abort=()=>finish(Error('Membership request aborted'));
   pending.set(id,{finish});timer=setTimeout(()=>finish(Error('Membership peer request timed out')),timeoutMs);signal?.addEventListener('abort',abort,{once:true});
   try{send(peer,{kind:'request',id,action,data});}catch(error){finish(error);}
  });},
  notify(key,data){send(key.slice(0,16),{kind:'event',data});},
  close(){if(closed)return;closed=true;off();for(const row of [...pending.values()])row.finish(Error('Membership channel closed'));}
 };
}
