import {runMixedTest} from './mixed-doorways-scenario.js';
import {createBrowserPhantom} from '../../packages/phantom-doorways/src/browser-entry.js';
import {writeFile} from 'node:fs/promises';
const sockets=new Set(),rtc=new Map();let next=0;
const trace=[],cfEpochs=new Map();
function cfDiscovery(network){
 const clients=[...sockets].filter(s=>s.cf&&s.row&&s.url.searchParams.get('v')===network);
 const epoch=(cfEpochs.get(network)??0)+1;cfEpochs.set(network,epoch);
 for(const s of clients)s.receive({type:process.env.PHANTOM_CF_ROSTER==='full'?'concierge_update':'concierge_route',videoId:network,epoch,syscon1:clients[0]?.row.hKey,syscon2:clients[1]?.row.hKey,syscon1EndpointId:clients[0]?.row.endpointId,syscon2EndpointId:clients[1]?.row.endpointId,...(process.env.PHANTOM_CF_ROSTER==='full'?{list:clients.map(c=>c.row)}:{})});
}
class Socket {
 static OPEN=1;readyState=0;listeners={};
 constructor(url){this.url=new URL(url);this.cf=this.url.hostname==='cf.fixture';sockets.add(this);queueMicrotask(()=>{this.readyState=1;this.onopen?.();});}
 addEventListener(type,fn){(this.listeners[type]??=[]).push(fn);}
 receive(value){const deliver=()=>{if(this.readyState===1)this.onmessage?.({data:JSON.stringify(value)});};if(this.cf&&value.type?.startsWith('rtc_')&&process.env.PHANTOM_SIGNAL_DELAY)setTimeout(deliver,Number(process.env.PHANTOM_SIGNAL_DELAY));else queueMicrotask(deliver);}
 send(raw){const m=JSON.parse(raw);
  if(!this.cf){if(m[0]==='REQ'){this.sub=m[1];this.receive(['EOSE',this.sub]);}if(m[0]==='EVENT'){this.receive(['OK',m[1].id,true,'']);for(const s of sockets)if(!s.cf&&s.sub)s.receive(['EVENT',s.sub,m[1]]);}return;}
  const network=this.url.searchParams.get('v'),from=this.url.searchParams.get('to');
  trace.push({type:m.type,from,to:m.to});
  if(m.type==='concierge_connect'){
   this.row={...m,hKey:from,endpointId:'endpoint-'+from};cfDiscovery(network);return;
  }
  if(!['rtc_offer','rtc_answer','rtc_ice'].includes(m.type))return;
  for(const s of sockets)if(s.cf&&s.url.searchParams.get('v')===network&&s.url.searchParams.get('to')===m.to)s.receive(m);
 }
 close(){if(this.readyState===3)return;this.readyState=3;sockets.delete(this);if(this.cf&&this.row)cfDiscovery(this.url.searchParams.get('v'));for(const fn of this.listeners.close??[])fn();this.onclose?.({code:1000,reason:'closed'});}
}
function channel(label){return {label,readyState:'connecting',bufferedAmount:0,send(data){if(this.readyState!=='open')throw Error('not open');queueMicrotask(()=>this.other?.onmessage?.({data}));},close(){if(this.readyState==='closed')return;this.readyState='closed';queueMicrotask(()=>this.onclose?.());if(this.other&&this.other.readyState!=='closed'){this.other.readyState='closed';queueMicrotask(()=>this.other.onclose?.());}}};}
class RTC {
 signalingState='stable';connectionState='new';iceConnectionState='new';id=String(++next);channels=[];
 constructor(){rtc.set(this.id,this);}
 createDataChannel(label){const c=channel(label);this.channels.push(c);return c;}
 async createOffer(){return {type:'offer',sdp:this.id};}
 async createAnswer(){return {type:'answer',sdp:this.id};}
 async setLocalDescription(d){this.localDescription=d;this.signalingState=d.type==='offer'?'have-local-offer':'stable';}
 async setRemoteDescription(d){this.remoteDescription=d;if(d.type==='offer'){
  this.signalingState='have-remote-offer';this.remote=rtc.get(d.sdp);
  for(const c of this.remote.channels){const other=channel(c.label);c.other=other;other.other=c;this.channels.push(other);this.ondatachannel?.({channel:other});}
 }else{this.signalingState='stable';const peer=rtc.get(d.sdp);for(const pc of [this,peer]){pc.connectionState='connected';pc.iceConnectionState='connected';for(const c of pc.channels){c.readyState='open';queueMicrotask(()=>c.onopen?.());}pc.onconnectionstatechange?.();}}}
 async addIceCandidate(){}
 close(){this.signalingState='closed';this.channels.forEach(c=>c.close());}
}
const sdkFactory=options=>{
 if(process.env.PHANTOM_TEST_LAYOUT!=='cf-only')return createBrowserPhantom(options);
 const sdk=createBrowserPhantom({...options,doorways:options.doorways.filter(d=>d.type==='websocket')});
 return {...sdk,join:(invite,joinOptions)=>sdk.join(invite,{...joinOptions,doorwayTypes:['websocket']})};
};
const report=await runMixedTest({transportRecoveryGraceMs:10000,lifecycle:process.env.PHANTOM_LIFECYCLE==='1',abrupt:process.env.PHANTOM_ABRUPT==='1',sdkFactory,endpoint:'wss://cf.fixture/',relays:['wss://nostr.fixture/'],WebSocket:Socket,runtime:{crypto:globalThis.crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval},deadlineMs:process.env.PHANTOM_ABRUPT==='1'?30000:5000,onUpdate:r=>console.log(r.steps.at(-1)?.label,r.steps.at(-1)?.status)});
const customPackets=trace.filter(p=>p.type.startsWith('phantom_entry'));
console.log(report.passed,report.error);process.exitCode=report.passed&&!customPackets.length?0:1;console.log(trace.filter(p=>p.type.startsWith('phantom_entry')));console.log(report.events.filter(e=>e.type==='DOORWAY_STATUS'&&e.payload.type.startsWith('entry_')));
if(process.env.PHANTOM_TRACE_OUTPUT)await writeFile(process.env.PHANTOM_TRACE_OUTPUT,JSON.stringify({report,trace},null,2));
