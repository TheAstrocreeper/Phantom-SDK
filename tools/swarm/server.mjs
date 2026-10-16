import http from 'node:http';
import {WebSocketServer} from 'ws';
/** Loopback-only test discovery/signaling. No file payloads pass through this server. */
export async function startServer(bundle,log,{host='127.0.0.1',port=0}={}){
 const server=http.createServer((req,res)=>{res.setHeader('Cache-Control','no-store');if(req.url==='/peer.js'){res.setHeader('Content-Type','text/javascript');res.end(bundle);}else if(req.url==='/'){res.setHeader('Content-Type','text/html');res.end('<!doctype html><title>Phantom swarm peer</title><script type="module" src="/peer.js"></script>');}else{res.writeHead(404);res.end();}});
 const ws=new WebSocketServer({server,maxPayload:1024*1024}),rooms=new Map();let paused=false;const broadcasts=new Set();
 const send=(socket,packet)=>{if(!paused&&socket.readyState===1)socket.send(JSON.stringify(packet));};
 ws.on('connection',(socket,req)=>{
  const u=new URL(req.url,'http://localhost'),id=u.searchParams.get('to'),roomId=u.searchParams.get('v');
  if(!/^[0-9a-f]{16}$/.test(id??'')||!roomId){socket.close(1008);return;}
  let room=rooms.get(roomId);if(!room){room={members:new Map(),epoch:0,leader:null,mirror:null};rooms.set(roomId,room);}
  function discovery(type){const keys=[...room.members.keys()].sort();const leader=keys.includes(room.leader)?room.leader:keys.includes(room.mirror)?room.mirror:keys[0]??null;const mirror=keys.includes(room.mirror)&&room.mirror!==leader?room.mirror:keys.find(k=>k!==leader)??null;if(leader!==room.leader||mirror!==room.mirror)room.epoch++;room.leader=leader;room.mirror=mirror;return {type,videoId:roomId,epoch:room.epoch,syscon1:leader,syscon2:mirror,list:[...room.members.values()].map(v=>v.row).sort((a,b)=>a.roomJoinedAt-b.roomJoinedAt||a.hKey.localeCompare(b.hKey)),syscon1EndpointId:room.members.get(leader)?.row.endpointId??null,syscon2EndpointId:room.members.get(mirror)?.row.endpointId??null,t:Date.now()};}
  function broadcast(type){const p=discovery(type);for(const member of room.members.values())send(member.socket,p);}
  broadcasts.add(broadcast);
  socket.on('message',raw=>{if(paused){log('relay.dropped',{peerId:id,bytes:raw.length});return;}try{const p=JSON.parse(raw);if(p.videoId&&p.videoId!==roomId)return;
   if(p.type==='concierge_connect'){if(p.from!==id)return;const first=!room.leader;const previous=room.members.get(id);room.members.set(id,{socket,row:{hKey:id,from:id,handle:p.handle,bootId:p.bootId,endpointId:crypto.randomUUID(),roomJoinedAt:p.t,lastSeen:Date.now()}});if(previous&&previous.socket!==socket)previous.socket.close(4000);send(socket,discovery(first?'concierge_bootstrap_needed':'concierge_route'));broadcast('concierge_update');log('relay.join',{peerId:id,members:room.members.size});}
   else if(room.members.get(id)?.socket!==socket)return;
   else if(p.type==='doorway_reconcile'||p.type==='presence')send(socket,discovery(p.type==='presence'?'roster':'concierge_route'));
   else if(p.type==='doorway_authority')return;
   else if(p.to){if(p.from!==id)return;const target=room.members.get(p.to);log('relay.signal',{from:id,to:p.to,type:p.type,delivered:!!target});if(target)send(target.socket,p);else if(p.type==='rtc_offer'&&p.sysCarrier)send(socket,{type:'rtc_undeliverable',signalType:'rtc_offer',target:p.to,sysCarrier:true,epoch:p.epoch,at:Date.now()});}
  }catch(e){log('relay.error',{error:e.message});}});
  socket.on('close',()=>{broadcasts.delete(broadcast);if(room.members.get(id)?.socket===socket){room.members.delete(id);broadcast('concierge_update');log('relay.leave',{peerId:id,members:room.members.size});}});
  socket.on('error',e=>log('relay.error',{error:e.message}));
 });
 await new Promise(resolve=>server.listen(port,host,resolve));
 return {setSignalingPaused(value){paused=!!value;log('fault.signaling',{paused});if(!paused)for(const broadcast of broadcasts)broadcast('concierge_update');},url:`http://127.0.0.1:${server.address().port}`,async close(){for(const s of ws.clients)s.terminate();await new Promise(resolve=>ws.close(resolve));await new Promise(resolve=>server.close(resolve));}};
}
