import {createNetworkDistribution} from '/packages/phantom-core/src/network-distribution.js';
import {createAdminIdentity} from '/packages/phantom-core/src/authorization.js';
import {createMemoryChunkStore} from '/packages/phantom-core/src/files.js';
const totalBytes=32*1024*1024;
window.makeAdmin=async()=>{window.admin=await createAdminIdentity();return admin.publicKey;};
window.configure=({selfId,peerId,publicKey,mode,messageBytes,parallel=1,streams=1})=>{
 const source=selfId==='1111111111111111';let callback;
 window.record={bytes:0,firstDataMs:null,lastDataMs:null,marks:[],offers:0,retries:0,closes:[],samples:[],errors:[],complete:false};
 window.peers=[];window.shutdown=false;
 const received=bytes=>{record.bytes+=bytes;record.firstDataMs??=performance.now();record.lastDataMs=performance.now();if(record.bytes%(2*1024*1024)===0)record.marks.push({bytes:record.bytes,atMs:performance.now()});};
 window.deliver=frame=>callback?.({type:'PHANTOM_DIST',payload:{from:peerId,frame}});
 class RTC extends RTCPeerConnection{
  constructor(...args){super(...args);peers.push(this);}
  createDataChannel(...args){const dc=super.createDataChannel(...args),send=dc.send.bind(dc);dc.send=data=>{const result=send(data);if(typeof data==='string')try{if(JSON.parse(data).sys==='FILE_OFFER')record.offers++;}catch{}return result;};(this.channels??=[]).push(dc);this.channel=dc;return dc;}
  close(){record.closes.push({queuedBytes:this.channel?.bufferedAmount??0,harnessShutdown:shutdown});return super.close();}
 }
 window.sampler=setInterval(async()=>{for(const pc of peers){try{const stats=await pc.getStats();let transport,pair;for(const s of stats.values())if(s.type==='transport')transport=s;if(transport?.selectedCandidatePairId)pair=stats.get(transport.selectedCandidatePairId);record.samples.push({atMs:performance.now(),bytes:record.bytes,peerIndex:peers.indexOf(pc),queuedBytes:(pc.channels??[]).reduce((n,dc)=>n+dc.bufferedAmount,0),sent:transport?.bytesSent,received:transport?.bytesReceived,rttMs:pair?.currentRoundTripTime*1000});}catch{}}},500);
 window.store=createMemoryChunkStore();
 if(mode==='sdk'){
  window.d=createNetworkDistribution({networkId:'delivery-native',adminPublicKey:publicKey,runtime:{crypto,RTCPeerConnection:RTC},allowed:()=>true,canUsePeer:()=>true,lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'delivery-native',action,target,...fields,operationId:crypto.randomUUID()})},core:{getState:()=>({peerId:selfId,leader:'1111111111111111',mirror:'2222222222222222',role:source?'syscon1':'syscon2',state:'admitted',admitted:true}),onEvent(fn){callback=fn;return()=>callback=null;},command(type,{frame}){if(type!=='PHANTOM_DIST_SEND')return;if(frame.to===selfId)queueMicrotask(()=>callback?.({type:'PHANTOM_DIST',payload:{from:selfId,frame}}));else void bridge(frame);}},options:{chunkStore:store,batchSize:32,requestWindow:32,rtcConfiguration:{iceServers:[]}},emit:(type,data)=>{if(type==='distributionProgress')received(65536);if(type==='distributionComplete')record.complete=true;if(type==='distributionFileEvent'&&data.fileEvent==='fileRetry')record.retries++;if(type==='distributionError')record.errors.push(data.error);}});
  callback({type:'PRESENCE_ROSTER',payload:{list:[selfId,peerId].map(hKey=>({hKey}))}});
  window.publish=async()=>{const file=new File([new Uint8Array(totalBytes)],'fixture');const prepared=await d.prepare(file,{chunkSize:65536,sourceReadBlockBytes:4194304});window.assetId=prepared.id;await d.publish(file,{prepared,recipients:[peerId]});};
  window.verify=async id=>{let bytes=0;for(let i=0;i<totalBytes/65536;i++){const b=new Uint8Array(await store.get(id,i));if(b.length!==65536||b.some(v=>v!==0))throw Error('SDK stored payload mismatch');bytes+=b.length;}return bytes;};
 }else{
  window.rawPeers=Array.from({length:parallel},()=>new RTC({iceServers:[]}));window.rawChannels=[];
  for(const [index,pc] of rawPeers.entries()){
   pc.pendingIce=[];pc.onicecandidate=({candidate})=>{if(candidate)void ice(candidate.toJSON(),index);};
   pc.ondatachannel=({channel:dc})=>{pc.channel=dc;(pc.channels??=[]).push(dc);dc.binaryType='arraybuffer';dc.onmessage=({data})=>{const bytes=new Uint8Array(data);if(bytes.length!==messageBytes||bytes.some(v=>v!==0))window.rawInvalid=true;received(bytes.length);if(record.bytes===totalBytes)record.complete=true;if(record.bytes>totalBytes)window.rawInvalid=true;};};
  }
  window.addIce=async(candidate,index=0)=>{const pc=rawPeers[index];if(pc.remoteDescription)await pc.addIceCandidate(candidate);else pc.pendingIce.push(candidate);};
  window.setRemote=async(desc,index=0)=>{const pc=rawPeers[index];await pc.setRemoteDescription(desc);for(const c of pc.pendingIce.splice(0))await pc.addIceCandidate(c);};
  window.rawInvalid=false;
  window.offer=async(index=0)=>{const pc=rawPeers[index];for(let j=0;j<streams;j++)rawChannels.push(pc.createDataChannel('raw-'+j,{ordered:true}));const offer=await pc.createOffer();await pc.setLocalDescription(offer);return offer;};
  window.answer=async(offer,index=0)=>{const pc=rawPeers[index];await setRemote(offer,index);const answer=await pc.createAnswer();await pc.setLocalDescription(answer);return answer;};
  window.publish=async()=>{const payload=new Uint8Array(messageBytes);let sent=0;const budget=Math.floor((2*1024*1024)/rawChannels.length);for(const dc of rawChannels){dc.bufferedAmountLowThreshold=Math.floor(budget/2);const pump=()=>{try{while(sent<totalBytes&&dc.bufferedAmount+payload.byteLength<=budget){dc.send(payload);sent+=payload.byteLength;}}catch(e){record.errors.push(e.message);}};dc.onbufferedamountlow=pump;pump();}};
  window.verify=async()=>{if(rawInvalid||record.bytes!==totalBytes)throw Error('Raw received payload mismatch');return record.bytes;};
 }
 window.stop=()=>{window.shutdown=true;clearInterval(sampler);window.d?.close();for(const pc of peers)if(pc.signalingState!=='closed')pc.close();};
};
window.ready=true;
