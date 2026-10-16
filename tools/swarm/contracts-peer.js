import {createBrowserPhantom,createBrowserIdentity} from '../../packages/phantom-doorways/src/browser-entry.js';
import {createAdminIdentity} from '../../packages/phantom-core/src/authorization.js';
import {createWebSocketDoorway,adaptWebSocketDoorway} from '../../packages/phantom-doorways/src/index.js';
import {COMPATIBILITY_WIRE} from '../../packages/phantom-core/src/compatibility.js';

// Test-only durable storage; reloads use the browser's real IndexedDB backend.
const db=await new Promise((resolve,reject)=>{const r=indexedDB.open('phantom-contracts',1);r.onupgradeneeded=()=>{r.result.createObjectStore('chunks');r.result.createObjectStore('records');};r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});
function stored(table,mode,action){return new Promise((resolve,reject)=>{const tx=db.transaction(table,mode),r=action(tx.objectStore(table));tx.oncomplete=()=>resolve(r.result);tx.onerror=()=>reject(tx.error);tx.onabort=()=>reject(tx.error??Error('Storage transaction aborted'));});}
let network,lastState,lastSnapshot,interrupted=false,interrupt=false,turnOnly=false;
const trace=[],messages=[],connections=[],errors=[],native=new Set();
const log=(type,data)=>{void globalThis.harnessLog({type,data});};
class RTC extends RTCPeerConnection{
 constructor(config){
  super(config);native.add(this);log('rtc.created',{policy:this.getConfiguration().iceTransportPolicy});
  if(turnOnly&&this.getConfiguration().iceTransportPolicy!=='relay')throw Error('SDK connection did not retain relay-only policy');
  this.addEventListener('connectionstatechange',()=>{if(this.connectionState==='connected')void this.capturePair();});
  this.addEventListener('datachannel',e=>this.observeChannel(e.channel));
 }
 observeChannel(dc){
  if(dc.label===COMPATIBILITY_WIRE.chatChannel)this.chat=dc;
  this.kind=({[COMPATIBILITY_WIRE.systemChannel]:'system',[COMPATIBILITY_WIRE.chatChannel]:'messaging','phantom-distribution':'distribution'})[dc.label];
  dc.addEventListener('open',()=>void this.capturePair());
 }
 createDataChannel(...args){const dc=super.createDataChannel(...args);this.observeChannel(dc);return dc;}
 async capturePair(){
  if(!this.kind)return;
  try{
   const stats=await this.getStats();
   for(const transport of stats.values())if(transport.type==='transport'&&transport.selectedCandidatePairId){
    const pair=stats.get(transport.selectedCandidatePairId),local=stats.get(pair?.localCandidateId),remote=stats.get(pair?.remoteCandidateId);
    if(pair&&local&&remote)log('rtc.selectedPair',{kind:this.kind,state:pair.state,localType:local.candidateType,remoteType:remote.candidateType,localAddress:local.address,remoteAddress:remote.address,localPort:local.port,remotePort:remote.port,protocol:local.protocol,relayProtocol:local.relayProtocol,policy:this.getConfiguration().iceTransportPolicy});
   }
  }catch(error){log('rtc.statsError',{kind:this.kind,error:error.message});}
 }
 close(){native.delete(this);super.close();}
}
const chunkStore={
 get:(id,index)=>stored('chunks','readonly',s=>s.get(`${id}:${index}`)),
 async put(id,index,bytes){await stored('chunks','readwrite',s=>s.put(bytes,`${id}:${index}`));if(interrupt){interrupt=false;interrupted=true;network.close();log('fault.closedAfterStoredChunk',{id,index});}},
 load:async networkId=>(await stored('records','readonly',s=>s.getAll())).filter(r=>r.proof.networkId===networkId),
 save:(networkId,record)=>{if(record.proof.networkId!==networkId)throw Error('Wrong storage scope');return stored('records','readwrite',s=>s.put(record,record.manifest.id));},
};
const sha=async bytes=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
globalThis.contracts={
 async open({invite,label,rtcConfiguration={iceServers:[]},timeoutMs=60000}){
  turnOnly=rtcConfiguration.iceTransportPolicy==='relay';
  let identity=JSON.parse(localStorage.getItem('identity')??'null');if(!identity){identity=createBrowserIdentity();localStorage.setItem('identity',JSON.stringify(identity));}
  const sdk=createBrowserPhantom({identity,messaging:'automatic',rtcConfiguration,...(!invite?{adminIdentity:await createAdminIdentity()}:{}),
   distribution:{chunkStore,batchSize:1,maxConnections:1,intervalMs:50,traceChunks:true},
   runtime:{crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval,localStorage},
   doorways:[{type:'harness'}],adapters:{harness:()=>adaptWebSocketDoorway(createWebSocketDoorway({url:location.origin.replace('http:','ws:')+'/' }))}});
  const options={name:label,timeoutMs,onState:state=>{lastState=state;log('sdk.state',state);},onEvent:e=>{if(e.type==='SNAPSHOT')lastSnapshot=e.payload;log('sdk.event',e);}};
  network=await(invite?sdk.join(invite,options):sdk.create(options));
  for(const type of ['distributionChunkTrace','distributionJob','distributionJobFailed','distributionRestored','distributionRestoreRejected','distributionComplete','distributionError','peerConnection'])network.on(type,data=>{
   if(type==='distributionChunkTrace')trace.push(data);
   if(type==='peerConnection')connections.push(data);
   if(type==='distributionError'||type==='distributionRestoreRejected')errors.push({type,...data});
   log(type,type==='distributionComplete'?{id:data.id,size:data.blob.size}:data);
  });
  network.on('message',data=>messages.push(data));
  return {peerId:network.getState().peerId,invite:network.invite};
 },
 inspect(){return {state:network?.getState()??lastState,snapshot:network?.snapshot()??lastSnapshot,connections:network?.getPeerConnections()??[],connectionEvents:connections,nativeConnections:[...native].map(pc=>({connectionState:pc.connectionState,iceConnectionState:pc.iceConnectionState})),distribution:network?.getDistributionState(),trace,messages,errors,interrupted};},
 send({to,text}){network.send(to,text);},
 breakMessaging(){let closed=0;for(const pc of native)if(pc.chat?.readyState==='open'){pc.chat.close();closed++;}if(!closed)throw Error('No native messaging channel found');return closed;},
 armPartial(){interrupt=true;},
 async publish({recipients,size=262144}){const bytes=Uint8Array.from({length:size},(_,i)=>(i*31+(i>>>8)+17)%256),manifest=await network.distributeFile(new Blob([bytes]),{recipients,name:'contracts.bin'});return {...manifest,sha256:await sha(bytes)};},
 async localFile(id){try{const blob=await network.getDistributedFile(id);return {size:blob.size,sha256:await sha(await blob.arrayBuffer())};}catch(error){return {error:error.message};}},
 chunkKeys:()=>stored('chunks','readonly',s=>s.getAllKeys()),
 close(){network.close();},
};
