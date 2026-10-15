import {createBrowserPhantom} from '../../../packages/phantom-doorways/src/browser-entry.js';
import {apply,history} from './model.js';

/** App wiring: all discovery, membership and peer data use the real SDK. */
export async function openCanvas({invite,name,board,onChange=()=>{},onStatus=()=>{},onTrace=()=>{},signal,sdkFactory=createBrowserPhantom,sdkOptions={}}){
 let network,closed=false;
 const stats={sent:0,received:0};
 const ready=()=>new Set(network.getPeerConnections().filter(p=>p.state==='ready').map(p=>p.peerId));
 function trace(type,detail){onTrace({at:Date.now(),type,detail});}
 function send(to,ops){if(closed||!ready().has(to))return;network.send(to,JSON.stringify({app:'phantom-canvas',version:1,type:'ops',ops}));stats.sent++;}
 function sync(to){const ops=history(board);for(let i=0;i<ops.length;i+=24)send(to,ops.slice(i,i+24));trace('canvas.sync',to);}
 const sdk=sdkFactory({...sdkOptions,messaging:'automatic',onDoorwayStatus:s=>trace('doorway',s.type??'status')});
 const options={name,signal,onState:onStatus};
 network=await (invite?sdk.join(invite,options):sdk.create(options));
 const offConnection=network.on('peerConnection',p=>{trace('peer.connection',p);if(p.state==='ready')sync(p.peerId);onStatus(network.getState());});
 const off=network.on('message',({from,text})=>{
  if(text.length>64000||!ready().has(from))return;
  let p;try{p=JSON.parse(text);}catch{return;}
  if(p?.app!=='phantom-canvas'||p.version!==1||p.type!=='ops'||!Array.isArray(p.ops)||p.ops.length>24)return;
  stats.received++;let changed=false;for(const op of p.ops)changed=apply(board,op)||changed;
  if(changed)onChange();onStatus(network.getState());
 });
 for(const peer of ready())sync(peer);onStatus(network.getState());trace('network.open',network.networkId);
 return {network,stats,get ready(){return ready();},
  broadcast(op){for(const key of ready())send(key,[op]);},
  close(){if(closed)return;closed=true;off();offConnection();network.close();trace('network.closed','You left the room.');}
 };
}
