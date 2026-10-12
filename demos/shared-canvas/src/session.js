import {createBrowserPhantom} from '../../../packages/phantom-doorways/src/browser-entry.js';
import {apply,history} from './model.js';

/** App wiring: all discovery, membership and peer data use the real SDK. */
export async function openCanvas({invite,name,board,onChange=()=>{},onStatus=()=>{},onTrace=()=>{},signal,sdkFactory=createBrowserPhantom,sdkOptions={}}){
 let network,closed=false;const ready=new Set(),invited=new Map(),pending=[];
 const stats={sent:0,received:0};
 function trace(type,detail){onTrace({at:Date.now(),type,detail});}
 function send(to,ops){if(closed||!ready.has(to))return;network.send(to,JSON.stringify({app:'phantom-canvas',version:1,type:'ops',ops}));stats.sent++;}
 function sync(to){const ops=history(board);for(let i=0;i<ops.length;i+=24)send(to,ops.slice(i,i+24));trace('canvas.sync',to);}
 function connect(){
  if(!network||closed)return;const self=network.getState().peerId;
  const peers=new Set(network.getPeers().map(p=>p.hKey));
  for(const key of [...ready])if(!peers.has(key))ready.delete(key);
  for(const key of peers)if(key!==self&&self<key&&!ready.has(key)&&Date.now()-(invited.get(key)??0)>4000){invited.set(key,Date.now());network.invitePeer(key);trace('peer.invite',key);}
 }
 function event(e){
  if(!network){if(pending.length<200)pending.push(e);return;}
  if(e.type==='VEIL_INVITE_RECEIVED'){network.acceptPeer(e.payload.from);trace('peer.accept',e.payload.from);}
  if(e.type==='P2P_CHAT_READY'){const key=e.payload.from;if(!ready.has(key)){ready.add(key);trace('peer.ready',key);sync(key);}}
  if(e.type==='PRESENCE_ROSTER'){connect();onStatus(network.getState());}
 }
 const sdk=sdkFactory({...sdkOptions,onDoorwayStatus:s=>trace('doorway',s.type??'status')});
 const options={name,signal,onEvent:event,onState:s=>{onStatus(s);if(network)connect();}};
 network=await (invite?sdk.join(invite,options):sdk.create(options));
 const off=network.on('message',({from,text})=>{
  if(text.length>64000||!ready.has(from))return;
  let p;try{p=JSON.parse(text);}catch{return;}
  if(p?.app!=='phantom-canvas'||p.version!==1||p.type!=='ops'||!Array.isArray(p.ops)||p.ops.length>24)return;
  stats.received++;let changed=false;for(const op of p.ops)changed=apply(board,op)||changed;
  if(changed)onChange();onStatus(network.getState());
 });
 for(const e of pending)event(e);connect();onStatus(network.getState());trace('network.open',network.networkId);
 const timer=setInterval(()=>{connect();onStatus(network.getState());},1000);
 return {network,stats,ready,
  broadcast(op){for(const key of ready)send(key,[op]);},
  close(){if(closed)return;closed=true;clearInterval(timer);off();ready.clear();network.close();trace('network.closed','You left the room.');}
 };
}
