import assert from 'node:assert/strict';
import {createBrowserPhantom} from '../../packages/phantom-doorways/src/browser-entry.js';
import {createPhantom} from '../../packages/phantom-core/src/index.js';
import {createDoorway} from '../../packages/phantom-doorways/src/interface.js';
import {memoryRTC} from './memory-rtc.js';
async function until(check){const end=Date.now()+4000;while(Date.now()<end){if(check())return;await new Promise(r=>setTimeout(r,5));}assert.fail('messaging did not settle');}
export async function bulkNetworkFixture(messaging='automatic',{dropFirstOffer=false,dropEveryOffer=false}={}){
 let dropped=false;const offers=[],packets=[],denied=new Set();
 const members=new Map(),networks=[],cores=[],instances=[],events=[],Base=memoryRTC({onSend:(data,channel)=>{let m;try{m=JSON.parse(data);packets.push({frame:m,channel});}catch{return true;}if(m.msg?.type==='rtc_offer'&&m.msg.pipe==='chat'){offers.push(m.msg.chatAttemptId);if(dropEveryOffer||(dropFirstOffer&&!dropped)){dropped=true;return false;}}return true;}});
 class RTC extends Base {constructor(config){super(config);instances.push(this);}}
 const adapter=()=>createDoorway({discovery:true,presence:'live-sockets',open(c){queueMicrotask(c.onOpen);
  const publish=()=>{const keys=[...members.keys()].sort(),value={kind:'concierge_update',epoch:1,leader:keys[0],mirror:keys[1],peers:[...members.values()].map(m=>m.row)};for(const m of members.values())m.context.onDiscovery(value);};
  return {announce(v){members.set(c.peerId,{context:c,row:{hKey:c.peerId,from:c.peerId,bootId:v.bootId,handle:v.handle,roomJoinedAt:v.t}});publish();},reconcile:publish,sendSignal(p){queueMicrotask(()=>members.get(p.to)?.context.onSignal(p));},close(){members.delete(c.peerId);publish();}};
 }});
 async function open(index,invite){const values=new Map(),storage={getItem:k=>values.get(k)??null,setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)};
  const sdk=createBrowserPhantom({messaging,doorways:[{type:'memory'}],adapters:{memory:adapter},runtime:{crypto,RTCPeerConnection:RTC,setTimeout:(fn,ms,...args)=>setTimeout(fn,(dropFirstOffer||dropEveryOffer)&&ms===20000?100:ms,...args),clearTimeout,setInterval,clearInterval,localStorage:storage},
   coreFactory:config=>{const core=createPhantom({...config,canUsePeer:peer=>!denied.has(peer)});cores.push(core);return {...core,join:options=>core.join({...options,identity:{version:1,id:String(index+1).repeat(64)}})};}});
  const options={name:`Peer ${index}`,timeoutMs:3000,onEvent:e=>events.push(e)};
  const n=await(invite?sdk.join(invite,options):sdk.create(options));networks.push(n);return n;
 }
 try{const a=await open(0),b=await open(1,a.invite);await until(()=>networks.every(n=>n.getState().state==='admitted'));return {a,b,cores,instances,events,networks,offers,packets,denied,close(){for(const n of networks)n.close();}};}catch(e){for(const n of networks)n.close();throw e;}
}
