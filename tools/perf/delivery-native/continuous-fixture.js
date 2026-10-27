import {makeManifest, sendStream, receiveStream} from '/tools/perf/continuous-prototype/stream.js';
import {createNetworkDistribution} from '/packages/phantom-core/src/network-distribution.js';
import {createAdminIdentity} from '/packages/phantom-core/src/authorization.js';
import {createMemoryChunkStore} from '/packages/phantom-core/src/files.js';
const totalBytes=32*1024*1024;
window.makeAdmin=async()=>{window.admin=await createAdminIdentity();return admin.publicKey;};
window.configure=({selfId,peerId,publicKey,mode,messageBytes,parallel=1})=>{
 const source=selfId==='1111111111111111';let callback;
 window.record={bytes:0,firstDataMs:null,lastDataMs:null,marks:[],offers:0,retries:0,closes:[],samples:[],errors:[],complete:false};
 window.peers=[];window.shutdown=false;
 const received=bytes=>{record.bytes+=bytes;record.firstDataMs??=performance.now();record.lastDataMs=performance.now();if(record.bytes%(2*1024*1024)===0)record.marks.push({bytes:record.bytes,atMs:performance.now()});};
 window.deliver=frame=>callback?.({type:'PHANTOM_DIST',payload:{from:peerId,frame}});
 class RTC extends RTCPeerConnection{
  constructor(...args){super(...args);peers.push(this);}
  createDataChannel(...args){const dc=super.createDataChannel(...args),send=dc.send.bind(dc);dc.send=data=>{const result=send(data);if(typeof data==='string')try{if(JSON.parse(data).sys==='FILE_OFFER')record.offers++;}catch{}return result;};this.channel=dc;return dc;}
  close(){record.closes.push({queuedBytes:this.channel?.bufferedAmount??0,harnessShutdown:shutdown});return super.close();}
 }
 window.sampler=setInterval(async()=>{for(const pc of peers){try{const stats=await pc.getStats();let transport,pair;for(const s of stats.values())if(s.type==='transport')transport=s;if(transport?.selectedCandidatePairId)pair=stats.get(transport.selectedCandidatePairId);record.samples.push({atMs:performance.now(),bytes:record.bytes,queuedBytes:pc.channel?.bufferedAmount??0,sent:transport?.bytesSent,received:transport?.bytesReceived,rttMs:pair?.currentRoundTripTime*1000});}catch{}}},500);
 window.store=createMemoryChunkStore();
 if(mode==='sdk'){
  window.d=createNetworkDistribution({networkId:'delivery-native',adminPublicKey:publicKey,runtime:{crypto,RTCPeerConnection:RTC},allowed:()=>true,canUsePeer:()=>true,lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'delivery-native',action,target,...fields,operationId:crypto.randomUUID()})},core:{getState:()=>({peerId:selfId,leader:'1111111111111111',mirror:'2222222222222222',role:source?'syscon1':'syscon2',state:'admitted',admitted:true}),onEvent(fn){callback=fn;return()=>callback=null;},command(type,{frame}){if(type!=='PHANTOM_DIST_SEND')return;if(frame.to===selfId)queueMicrotask(()=>callback?.({type:'PHANTOM_DIST',payload:{from:selfId,frame}}));else void bridge(frame);}},options:{chunkStore:store,transferLanes:parallel,batchSize:32,requestWindow:32,rtcConfiguration:{iceServers:[]}},emit:(type,data)=>{if(type==='distributionProgress')received(65536);if(type==='distributionComplete')record.complete=true;if(type==='distributionFileEvent'&&data.fileEvent==='fileRetry')record.retries++;if(type==='distributionError')record.errors.push(data.error);}});
  callback({type:'PRESENCE_ROSTER',payload:{list:[selfId,peerId].map(hKey=>({hKey}))}});
  window.publish=async()=>{const file=new File([new Uint8Array(totalBytes)],'fixture');const prepared=await d.prepare(file,{chunkSize:65536,sourceReadBlockBytes:4194304});window.assetId=prepared.id;await d.publish(file,{prepared,recipients:[peerId]});};
  window.verify=async id=>{let bytes=0;for(let i=0;i<totalBytes/65536;i++){const b=new Uint8Array(await store.get(id,i));if(b.length!==65536||b.some(v=>v!==0))throw Error('SDK stored payload mismatch');bytes+=b.length;}return bytes;};
 }else{
  window.pc=new RTC({iceServers:[]});pc.onicecandidate=({candidate})=>{if(candidate)void ice(candidate.toJSON());};
  window.pendingIce=[];window.addIce=async candidate=>{if(pc.remoteDescription)await pc.addIceCandidate(candidate);else pendingIce.push(candidate);};
  window.setRemote=async desc=>{await pc.setRemoteDescription(desc);for(const c of pendingIce.splice(0))await pc.addIceCandidate(c);};
  window.rawInvalid=false;
  pc.ondatachannel=({channel:dc})=>{pc.channel=dc;dc.binaryType='arraybuffer';window.streamReceiver=receiveStream(dc,store,{onCommit:bytes=>received(bytes)});streamReceiver.done.then(()=>{record.complete=true;}).catch(e=>record.errors.push(e.message));};
  window.offer=async()=>{window.dc=pc.createDataChannel('raw',{ordered:true});const offer=await pc.createOffer();await pc.setLocalDescription(offer);return offer;};
  window.answer=async offer=>{await setRemote(offer);const answer=await pc.createAnswer();await pc.setLocalDescription(answer);return answer;};
  window.publish=async()=>{const file=new File([new Uint8Array(totalBytes)],'fixture');const manifest=await makeManifest(file);await sendStream(dc,file,manifest);};
  window.verify=async()=>{let bytes=0;for(let i=0;i<totalBytes/65536;i++){const b=new Uint8Array(await store.get('stream',i));if(b.length!==65536||b.some(v=>v!==0))throw Error('Stream stored payload mismatch');bytes+=b.length;}return bytes;};
 }
 window.stop=()=>{window.shutdown=true;clearInterval(sampler);window.d?.close();for(const pc of peers)if(pc.signalingState!=='closed')pc.close();};
};
window.ready=true;

