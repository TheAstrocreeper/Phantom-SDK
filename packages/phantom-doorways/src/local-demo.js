// Local-only test doorway. Peer payloads travel over WebRTC.
export function createLocalDemoDoorway() {
 let blocked = false;
 const sockets = new Set();
 class Socket {
  static CONNECTING=0;static OPEN=1;static CLOSING=2;static CLOSED=3;
  constructor(url){if(blocked)throw new Error('Local test doorway deliberately offline');sockets.add(this);const u=new URL(url);this.room=u.searchParams.get('v');this.self=u.searchParams.get('to');this.readyState=0;this.members=new Map();this.epoch=0;this.topology="";this.channel=new BroadcastChannel(`phantom-demo:${this.room}`);this.channel.onmessage=({data})=>this.receive(data);queueMicrotask(()=>{if(this.readyState===0){this.readyState=1;this.onopen?.();}});}
  deliver(msg){if(this.readyState===1)this.onmessage?.({data:JSON.stringify(msg)});}
  announce(){this.channel.postMessage({kind:'presence',row:this.members.get(this.self)});}
  roster(){const list=[...this.members.values()].sort((a,b)=>a.roomJoinedAt-b.roomJoinedAt||a.hKey.localeCompare(b.hKey));const topology=list.map(row=>row.hKey).join(',');if(topology!==this.topology){this.topology=topology;this.epoch++;}this.deliver({type:'concierge_route',videoId:this.room,epoch:this.epoch,syscon1:list[0]?.hKey,syscon2:list[1]?.hKey||null,list});this.deliver({type:'roster',videoId:this.room,list});}
  receive(data){if(this.readyState!==1)return;if(data.kind==='presence'&&data.row?.hKey){const fresh=!this.members.has(data.row.hKey);this.members.set(data.row.hKey,data.row);if(fresh){this.announce();this.roster();}}else if(data.kind==='signal'){const msg=data.packet;if(msg.to===this.self||!msg.to)this.deliver(msg);}else if(data.kind==='leave'){this.members.delete(data.hKey);this.roster();}}
  send(raw){if(this.readyState!==1)throw new Error('closed');const packet=JSON.parse(raw);if(packet.type==='concierge_connect'){this.members.set(this.self,{hKey:this.self,handle:packet.handle,bootId:packet.bootId,roomJoinedAt:Date.now()});this.announce();this.roster();}else if(packet.type==='doorway_reconcile'){this.announce();this.roster();}else this.channel.postMessage({kind:'signal',packet});}
  close(){if(this.readyState===3)return;this.readyState=3;sockets.delete(this);if(!blocked)this.channel.postMessage({kind:'leave',hKey:this.self});this.channel.close();}
  cut(){if(this.readyState===3)return;this.close();this.onclose?.({code:1006,reason:'demo_doorway_cut',wasClean:false});}
 }
 return {protocol:'six7-concierge-v1',presence:'discovery-hints',url:'ws://local-demo.invalid/',WebSocket:Socket,
  cut(){blocked=true;for(const socket of [...sockets])socket.cut();},
  get offline(){return blocked;}
 };
}
