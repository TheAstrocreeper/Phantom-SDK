import {createNetworkDistribution} from '../src/network-distribution.js';
import {createMemoryChunkStore} from '../src/files.js';
import {createReubenTransfer} from './reuben.js';

export const reubenComparisonVersion='reuben-browser-v1';
export const reubenComparisonCases=Object.freeze(['current','reuben','reuben','current']);
export const reubenAblationEngines=Object.freeze(['current','reuben','reuben-split','reuben-batches','reuben-trusted']);
const pattern=i=>(i*17+29)%251;
const scope=address=>!address?'unknown':address.endsWith('.local')?'mdns':address==='::1'||/^127\./.test(address)?'loopback':/^(169\.254\.|fe[89ab])/i.test(address)?'link-local':/^(10\.|192\.168\.|172\.(1[6-9]|2\d|3[01])\.|f[cd])/i.test(address)?'private':'public';
/** One bounded diagnostic endpoint. Caller authenticates the peer and transports control messages.
 * Uses the actual distribution coordinator for current, the actual adapter for Reuben.
 * Generates test data only. No user files. No default application transfer path changes.
 */
export function createReubenComparisonEndpoint({engine,role,selfId,peerId,networkId,adminPublicKey,admin,send,store=createMemoryChunkStore(),runtime=globalThis,rtcConfiguration={iceServers:[]},bytes=32*1024*1024+17,chunkSize=65536,currentSettings={},onComplete=()=>{},onError=()=>{},onProgress=()=>{},isAllowed=()=>true}){
 if(!reubenAblationEngines.includes(engine)||!['sender','receiver'].includes(role)||![selfId,peerId].every(id=>/^[a-f0-9]{16}$/.test(id))||selfId===peerId||!Number.isSafeInteger(bytes)||bytes<1||bytes>128*1024*1024+17||!Number.isInteger(chunkSize)||chunkSize<1024||chunkSize>65536)throw Error('Invalid comparison settings');
 const batchSize=currentSettings.batchSize??32,requestWindow=currentSettings.requestWindow??32;
 if(!Number.isInteger(batchSize)||batchSize<1||batchSize>64||!Number.isInteger(requestWindow)||requestWindow<1||requestWindow>32)throw Error('Invalid current-engine diagnostic settings');
 const timings=[];let requestBoundaryCount=0,lastWriteAt=null,assemblyMs=0,completedAt=null;
 const suppliedStore=store;
 if(engine==='reuben-trusted'){
  // Same copies and input hashing; expose committed indexes so completion avoids rereading/rehashing.
  const inventory=new Map();
  store={async get(id,index){return suppliedStore.get(id,index);},async put(id,index,data){await suppliedStore.put(id,index,data);if(!inventory.has(id))inventory.set(id,new Set());inventory.get(id).add(index);},async getVerifiedIndexes(id){return [...(inventory.get(id)??[])];},async getFile(id,m){const at=performance.now(),parts=[];for(let i=0;i<m.total;i++)parts.push(await suppliedStore.get(id,i));const blob=new Blob(parts,{type:m.type});assemblyMs+=performance.now()-at;return blob;}};
 }
 const sender=role==='sender',sourceId=sender?selfId:peerId,receiverId=sender?peerId:selfId;
 let closed=false,startedAt=null,completed=false,pc,dc,adapter,distribution,coreHandler,payload,preparationMs=0,activeWrites=0,peakWrites=0,writeMs=0,committedBytes=0,sequence=0;
 const connections=new Set(),channels=new Map(),pendingIce=[],samples=[];let sampleBusy=false;
 const controller=new AbortController();
 const alive=()=>!closed&&isAllowed();
 const fail=error=>{if(closed)return;onError(error instanceof Error?error:Error(String(error)));close();};
 const transmit=message=>{if(alive())send(message);};
 const measuredStore=new Proxy(store,{get(target,key){if(key==='put')return async(...args)=>{activeWrites++;peakWrites=Math.max(peakWrites,activeWrites);const at=performance.now();try{await target.put(...args);}finally{writeMs+=performance.now()-at;lastWriteAt=performance.now();activeWrites--;}};const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}});
 function observe(channel,connection){
  channel.binaryType='arraybuffer';const id=++sequence,row={id,connection,sentBytes:0,receivedBytes:0,sentCount:0,receivedCount:0,peakBufferedAmount:0};channels.set(channel,row);
  const original=channel.send.bind(channel);channel.send=data=>{const result=original(data);if(data instanceof ArrayBuffer||ArrayBuffer.isView(data)){row.sentBytes+=data.byteLength;row.sentCount++;row.peakBufferedAmount=Math.max(row.peakBufferedAmount,channel.bufferedAmount);}return result;};
  channel.addEventListener('message',event=>{if(event.data instanceof ArrayBuffer){row.receivedBytes+=event.data.byteLength;row.receivedCount++;}});
 }
 const Native=runtime.RTCPeerConnection;
 class ObservedRTC extends Native {
  constructor(...args){super(...args);connections.add(this);this.addEventListener('datachannel',e=>observe(e.channel,this));}
  createDataChannel(...args){const channel=super.createDataChannel(...args);observe(channel,this);return channel;}
 }
 async function sample(){
  if(sampleBusy||closed)return;sampleBusy=true;
  try{for(const [channel,row]of channels){
   const at=performance.now();let pair,transport,local,remote,statsStatus='available';
   try{const stats=await row.connection.getStats();stats.forEach(s=>{if(s.type==='transport'&&s.selectedCandidatePairId){transport=s;pair=stats.get(s.selectedCandidatePairId);}});local=stats.get(pair?.localCandidateId);remote=stats.get(pair?.remoteCandidateId);}catch{statsStatus='unavailable';}
   if(closed)return;
   const point={channel:row.id,atMs:at-(startedAt??at),statsStatus,bufferedAmount:channel.bufferedAmount,peakBufferedAmount:row.peakBufferedAmount,sentBytes:row.sentBytes,receivedBytes:row.receivedBytes,sentCount:row.sentCount,receivedCount:row.receivedCount,transportBytesSent:transport?.bytesSent??null,transportBytesReceived:transport?.bytesReceived??null,rttMs:Number.isFinite(pair?.currentRoundTripTime)?pair.currentRoundTripTime*1000:null,localCandidateType:local?.candidateType??'unknown',remoteCandidateType:remote?.candidateType??'unknown',protocol:local?.protocol??'unknown',localScope:scope(local?.address),remoteScope:scope(remote?.address),routeChanges:transport?.selectedCandidatePairChanges??null,visibility:globalThis.document?.visibilityState??'unknown',heapBytes:globalThis.performance?.memory?.usedJSHeapSize??null};
   const previous=row.previous;
   point.sampleIntervalMs=previous?at-previous.at:0;
   point.transportSendBytesPerSecond=previous&&point.sampleIntervalMs>0&&Number.isFinite(point.transportBytesSent)&&Number.isFinite(previous.bytesSent)?Math.max(0,point.transportBytesSent-previous.bytesSent)*1000/point.sampleIntervalMs:null;
   point.transportReceiveBytesPerSecond=previous&&point.sampleIntervalMs>0&&Number.isFinite(point.transportBytesReceived)&&Number.isFinite(previous.bytesReceived)?Math.max(0,point.transportBytesReceived-previous.bytesReceived)*1000/point.sampleIntervalMs:null;
   row.previous={at,bytesSent:point.transportBytesSent,bytesReceived:point.transportBytesReceived};samples.push(point);if(samples.length>48)samples.splice(1,1);
  }}finally{sampleBusy=false;}
 }
 const timer=setInterval(()=>{void sample().catch(fail);},1000);timer?.unref?.();
 const startClock=()=>{startedAt??=performance.now();};
 const progress=data=>{if(!alive())return;committedBytes=engine==='current'?Math.min(bytes,(data.received??0)*chunkSize):(data.verifiedBytes??0);onProgress({engine,bytes:committedBytes,total:bytes});};
 async function complete(blob){
  if(completed||!alive())return;completed=true;completedAt=performance.now();committedBytes=bytes;const elapsedMs=performance.now()-startedAt,verificationAt=performance.now();
  if(blob.size!==bytes)throw Error('Comparison size mismatch');
  for(let offset=0;offset<bytes;offset+=1024*1024){const block=new Uint8Array(await blob.slice(offset,offset+1024*1024).arrayBuffer());for(let i=0;i<block.length;i++)if(block[i]!==pattern(offset+i))throw Error('Comparison content mismatch');if(!alive())return;}
  const verificationMs=performance.now()-verificationAt;await sample();
  if(alive())onComplete({...report(),elapsedMs,verificationMs,verified:true});
 }
 function report(){return {schema:1,version:reubenComparisonVersion,engine,role,bytes,chunkSize,requestWindow:engine==='current'?requestWindow:32,batchSize:engine==='current'?batchSize:null,wireFormat:engine==='current'?'sdk-split':engine==='reuben-split'?'split':'reuben',requestMode:engine==='current'?`coordinator-batches-of-${batchSize}`:engine==='reuben-batches'?'batches-of-32':'rolling',storage:engine==='reuben-trusted'?'verified-inventory-memory':'caller-provided',requestBoundaryCount,lastWriteToCompleteMs:completedAt!==null&&lastWriteAt!==null?completedAt-lastWriteAt:null,assemblyMs,timings:structuredClone(timings),committedBytes,preparationMs,elapsedMs:startedAt===null?null:performance.now()-startedAt,peakWrites,writeMs,samples:structuredClone(samples),channels:[...channels.values()].map(({id,sentBytes,receivedBytes,sentCount,receivedCount,peakBufferedAmount})=>({id,sentBytes,receivedBytes,sentCount,receivedCount,peakBufferedAmount})),memoryScope:'heapBytes is optional whole-page heap; buffered bytes and writes are queue measurements, not total process memory',sctpCounters:'unavailable'};}
 if(engine==='current'){
  distribution=createNetworkDistribution({networkId,adminPublicKey,runtime:{crypto:runtime.crypto??crypto,RTCPeerConnection:ObservedRTC},canUsePeer:id=>alive()&&[selfId,peerId].includes(id),allowed:()=>alive(),
   core:{getState:()=>({peerId:selfId,leader:sourceId,mirror:receiverId,role:sender?'syscon1':'syscon2',state:'admitted',admitted:true}),onEvent(fn){coreHandler=fn;return()=>{coreHandler=null;};},command(type,{frame}){if(type!=='PHANTOM_DIST_SEND')return;if(frame.to===selfId)queueMicrotask(()=>coreHandler?.({type:'PHANTOM_DIST',payload:{from:selfId,frame}}));else transmit({type:'distribution',frame});}},
   lifecycle:{issue:(action,target,fields)=>{if(!sender||!admin)throw Error('Comparison sender authority unavailable');return admin.sign({networkId,action,target,...fields,operationId:crypto.randomUUID()});}},
   options:{rtcConfiguration,batchSize,requestWindow,chunkStore:measuredStore,jobTimeoutMs:30000},
   emit:(type,data)=>{if(type==='distributionBatchStarted'&&!sender)startClock();if(type==='distributionProgress'&&!sender)progress(data);if(type==='distributionComplete'&&!sender)void complete(data.blob).catch(fail);if(type==='distributionError')fail(Error(data.error));}
  });
  coreHandler?.({type:'PRESENCE_ROSTER',payload:{list:[selfId,peerId].map(hKey=>({hKey}))}});
 }
 function attach(channel){
  dc=channel;
  if(channel.label!=='phantom-reuben-comparison')throw Error('Unexpected comparison channel');
  const ready=()=>{
   if(!alive()||adapter)return;
   adapter=createReubenTransfer({channel,peerId,crypto:runtime.crypto??crypto,chunkStore:measuredStore,maxMessageSize:pc.sctp?.maxMessageSize||65536,wireFormat:engine==='reuben-split'?'split':'reuben',canUsePeer:id=>id===peerId&&alive(),allowed:()=>alive(),signal:controller.signal,requestTimeoutMs:30000,maxFileBytes:bytes,
    emit:(type,data)=>{if(type==='fileTransferTiming')timings.push(data);if(type==='fileOffer'&&!sender){startClock();void (async()=>{if(engine==='reuben-batches'){for(let offset=0;offset<data.manifest.total;offset+=32){requestBoundaryCount++;await adapter.request(data.manifest.id,{peers:[peerId],indexes:Array.from({length:Math.min(32,data.manifest.total-offset)},(_,i)=>offset+i)});}}await complete(await adapter.request(data.manifest.id,{peers:[peerId]}));})().catch(fail);}if(type==='fileProgress'&&!sender)progress(data);if(['fileError','reubenProtocolError'].includes(type))fail(Error(data.error||type));}
   });
  };
  channel.addEventListener('open',ready,{once:true});if(channel.readyState==='open')ready();
 }
 if(engine!=='current'){
  pc=new ObservedRTC(rtcConfiguration);pc.ondatachannel=e=>{try{attach(e.channel);}catch(error){fail(error);}};
  pc.onicecandidate=e=>{if(e.candidate)transmit({type:'rtc',candidate:e.candidate.toJSON?.()??e.candidate});};
  pc.onconnectionstatechange=()=>{if(alive()&&!completed&&['failed','closed','disconnected'].includes(pc.connectionState))fail(Error('Comparison transport closed'));};
 }
 async function handle(message){
  if(!alive())return;
  if(engine==='current'){
   if(message?.type!=='distribution'||message.frame?.networkId!==networkId||message.frame.from!==peerId||message.frame.to!==selfId)throw Error('Invalid comparison distribution frame');
   coreHandler?.({type:'PHANTOM_DIST',payload:{from:peerId,frame:message.frame}});return;
  }
  if(message?.type!=='rtc')throw Error('Invalid comparison signal');
  if(message.description){if(!['offer','answer'].includes(message.description.type)||typeof message.description.sdp!=='string'||message.description.sdp.length>60000)throw Error('Invalid comparison description');await pc.setRemoteDescription(message.description);for(const c of pendingIce.splice(0))await pc.addIceCandidate(c);if(message.description.type==='offer'){await pc.setLocalDescription(await pc.createAnswer());transmit({type:'rtc',description:pc.localDescription});}}
  else if(message.candidate){if(typeof message.candidate.candidate!=='string'||message.candidate.candidate.length>4096)throw Error('Invalid comparison candidate');if(pc.remoteDescription)await pc.addIceCandidate(message.candidate);else if(pendingIce.length<64)pendingIce.push(message.candidate);else throw Error('Too many comparison candidates');}
 }
 async function start(){
  if(!sender||closed)throw Error('Only the active sender can start');
  startClock();const at=performance.now();payload??=new Blob([Uint8Array.from({length:bytes},(_,i)=>pattern(i))]);
  if(engine==='current'){const prepared=await distribution.prepare(payload,{chunkSize,sourceReadBlockBytes:4*1024*1024});preparationMs=performance.now()-at;if(alive())await distribution.publish(payload,{prepared,recipients:[peerId]});return;}
  attach(pc.createDataChannel('phantom-reuben-comparison'));
  await pc.setLocalDescription(await pc.createOffer());transmit({type:'rtc',description:pc.localDescription});
  if(!alive())throw Error('Comparison aborted');
  await new Promise((resolve,reject)=>{const check=()=>{if(adapter)resolve();};const abort=()=>reject(Error('Comparison aborted'));controller.signal.addEventListener('abort',abort,{once:true});dc.addEventListener('open',()=>{controller.signal.removeEventListener('abort',abort);check();},{once:true});check();});
  if(!alive())return;
  if((pc.sctp?.maxMessageSize||65536)<chunkSize+(engine==='reuben-split'?0:44))throw Error('Negotiated message size cannot fit matched chunk settings');
  const manifest=await adapter.publish(payload,{chunkSize,sourceReadBlockBytes:4*1024*1024});preparationMs=performance.now()-at;if(alive())await adapter.share(peerId,manifest.id);
 }
 function close(){if(closed)return;closed=true;clearInterval(timer);controller.abort();distribution?.close();adapter?.close();for(const connection of connections)connection.close();payload=null;pendingIce.length=0;}
 return {start,handle,report,close,sample};
}
