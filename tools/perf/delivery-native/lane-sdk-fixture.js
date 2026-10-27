import {createAssignedRangeReceiver} from '/packages/phantom-core/src/assigned-range-receiver.js';
import {createFileDistribution} from '/packages/phantom-core/src/files.js';
import {createAdminAuthorization} from '/packages/phantom-core/src/authorization.js';
import {createLaneScheduler} from '/tools/perf/lane-priority/scheduler.js';
import {makeManifest, sendStream, receiveStream} from '/tools/perf/continuous-prototype/stream.js';
import {createNetworkDistribution} from '/packages/phantom-core/src/network-distribution.js';
import {createAdminIdentity} from '/packages/phantom-core/src/authorization.js';
import {createMemoryChunkStore} from '/packages/phantom-core/src/files.js';
const totalBytes=64*1024*1024;
function testFile(){const data=new Uint8Array(totalBytes);for(let i=0;i<totalBytes/65536;i++)data.fill(i%251,i*65536,(i+1)*65536);return new File([data],'fixture');}
window.makeAdmin=async()=>{window.admin=await createAdminIdentity();return admin.publicKey;};
window.configure=({selfId,peerId,publicKey,mode,messageBytes,parallel=1})=>{
 const source=selfId==='1111111111111111';let callback;
 window.record={bytes:0,firstDataMs:null,lastDataMs:null,marks:[],offers:0,retries:0,closes:[],samples:[],errors:[],controls:[],laneSeen:[0,0,0],reassignments:0,complete:false};
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
  window.publish=async()=>{const file=testFile();const prepared=await d.prepare(file,{chunkSize:65536,sourceReadBlockBytes:4194304});window.assetId=prepared.id;await d.publish(file,{prepared,recipients:[peerId]});};
  window.verify=async id=>{let bytes=0;for(let i=0;i<totalBytes/65536;i++){const b=new Uint8Array(await store.get(id,i));if(b.length!==65536||b.some(v=>v!==i%251))throw Error('SDK stored payload mismatch');bytes+=b.length;}return bytes;};
 }else if(mode==='lanes'||mode==='lanes_fail'){
  const chunkSize=65536, chunksPerRange=32, chunkCount=totalBytes/chunkSize, rangeCount=Math.ceil(chunkCount/chunksPerRange);
  window.rawPeers=Array.from({length:3},()=>new RTC({iceServers:[]}));window.rawChannels=[];
  const seen=new Set(),latestGeneration=new Int32Array(rangeCount),liveRanges=new Map();window.allDone=false;window.intentionalFailure=false;window.assigned=null;
  window.deliver=async frame=>{if(frame.type!=='bootstrap'||source)throw Error('Unexpected bootstrap');const {manifest,proof}=frame,policy=createAdminAuthorization({networkId:'delivery-native',adminPublicKey:publicKey,crypto});window.authorizedManifest=manifest;window.assigned=await createAssignedRangeReceiver({manifest,chunkStore:store,verifyManifest:async m=>{const v=await policy.verify(proof);return !!v&&v.action==='content.publish'&&v.target===m.id&&(!v.recipients||v.recipients.includes(selfId));}});};
  for(const [lane,pc] of rawPeers.entries()){
   pc.pendingIce=[];pc.onicecandidate=({candidate})=>{if(candidate)void ice(candidate.toJSON(),lane);};
   pc.ondatachannel=({channel})=>{pc.channel=channel;channel.binaryType='arraybuffer';let current=null;
    channel.onmessage=event=>{
     if(typeof event.data==='string'){
      let msg;try{msg=JSON.parse(event.data);}catch{record.errors.push('Invalid manifest');return;}
      if(msg.type==='control'){record.controls.push({id:msg.id,receivedAtBytes:record.bytes});channel.send(JSON.stringify({type:'control_ack',id:msg.id}));return;}
      const m=msg.manifest;
      if(msg.type!=='manifest'||current||!Number.isInteger(m?.range)||m.range<0||m.range>=rangeCount||!Number.isInteger(m.generation)||m.generation<=latestGeneration[m.range]||m.chunkSize!==chunkSize||!Array.isArray(m.hashes)||m.hashes.length!==chunksPerRange||m.size!==chunkSize*chunksPerRange){record.errors.push('Invalid range manifest');return;}
      if(!assigned||m.hashes.some((h,i)=>h!==authorizedManifest.hashes[m.range*chunksPerRange+i])){record.errors.push('Unauthorized range manifest');return;}
      latestGeneration[m.range]=m.generation;const previous=liveRanges.get(m.range),range=m.range,generation=m.generation;
      const ready=(async()=>{if(previous){const old=await previous.ready;if(old)await assigned.retire(old);}if(latestGeneration[range]!==generation)return null;const have=new Set(assigned.snapshot().have),indexes=Array.from({length:chunksPerRange},(_,i)=>range*chunksPerRange+i).filter(i=>!have.has(i));return indexes.length?assigned.claim(indexes):null;})();
      current={range,generation,hashes:m.hashes,accepted:0,committed:0,queue:Promise.resolve(),ready};liveRanges.set(range,current);return;
     }
     if(!(event.data instanceof ArrayBuffer)||!current||event.data.byteLength!==chunkSize+4){record.errors.push('Invalid data frame');return;}
     const range=current,index=new DataView(event.data).getUint32(0),bytes=event.data.slice(4);
     if(index!==range.accepted||index>=chunksPerRange){record.errors.push('Out-of-order range');return;}range.accepted++;
     range.queue=range.queue.then(async()=>{const lease=await range.ready;if(latestGeneration[range.range]!==range.generation)return;const global=range.range*chunksPerRange+index;
      if(!seen.has(global)){if(!lease)throw Error('Missing assignment');await assigned.receive(lease,global,bytes);}
      if(latestGeneration[range.range]!==range.generation)return;
      if(!seen.has(global)){seen.add(global);record.laneSeen[lane]++;received(bytes.byteLength);}range.committed++;
      if(range.committed===chunksPerRange&&lease)await assigned.retire(lease);
      if(range.committed%16===0||range.committed===chunksPerRange)channel.readyState==='open'&&channel.send(JSON.stringify({type:'ack',count:range.committed}));
      if(range.committed===chunksPerRange)current=null;if(seen.size===chunkCount)record.complete=true;
     }).catch(e=>{if(latestGeneration[range.range]===range.generation&&channel.readyState==='open')record.errors.push(e.message);});
    };
   };
  }
  window.addIce=async(candidate,index)=>{const pc=rawPeers[index];if(pc.remoteDescription)await pc.addIceCandidate(candidate);else pc.pendingIce.push(candidate);};
  window.setRemote=async(desc,index)=>{const pc=rawPeers[index];await pc.setRemoteDescription(desc);for(const c of pc.pendingIce.splice(0))await pc.addIceCandidate(c);};
  window.offer=async(index)=>{const pc=rawPeers[index];rawChannels[index]=pc.createDataChannel('lane-'+index,{ordered:true});const o=await pc.createOffer();await pc.setLocalDescription(o);return o;};
  window.answer=async({offer,index})=>{const pc=rawPeers[index];await setRemote(offer,index);const a=await pc.createAnswer();await pc.setLocalDescription(a);return a;};
  window.publish=async()=>{const file=testFile(),service=createFileDistribution({core:{onEvent(){return()=>{};},command(){}},crypto}),manifest=await service.publish(file,{name:'fixture',chunkSize});service.close();const proof=await admin.sign({networkId:'delivery-native',action:'content.publish',target:manifest.id,recipients:[peerId],operationId:crypto.randomUUID()});await bridge({type:'bootstrap',manifest,proof});const scheduler=createLaneScheduler({lanes:3,rangeCount});
   let waiters=[];const wake=()=>{const ready=waiters;waiters=[];for(const resolve of ready)resolve();};
   window.queueControl=()=>{const id=scheduler.control('test-control');wake();return id;};
   window.failLane=lane=>{window.intentionalFailure=true;rawChannels[lane].close();};
   const sendControl=(channel,work)=>new Promise((resolve,reject)=>{const onMessage=e=>{if(typeof e.data!=='string')return;let m;try{m=JSON.parse(e.data);}catch{return;}if(m.type==='control_ack'&&m.id===work.id){cleanup();resolve();}};const onClose=()=>{cleanup();reject(Error('Channel closed'));};const cleanup=()=>{channel.removeEventListener('message',onMessage);channel.removeEventListener('close',onClose);};channel.addEventListener('message',onMessage);channel.addEventListener('close',onClose);channel.send(JSON.stringify({type:'control',id:work.id}));});
   await Promise.all(rawChannels.map(async(channel,lane)=>{while(channel.readyState==='open'){const work=scheduler.next(lane);if(!work){if(scheduler.snapshot().complete===rangeCount)break;await new Promise(resolve=>waiters.push(resolve));continue;}if(work.kind==='control'){try{await sendControl(channel,work);scheduler.finish(lane,work);wake();}catch(e){scheduler.close(lane);if(!window.intentionalFailure)record.errors.push(e.message);wake();break;}continue;}const start=work.index*chunksPerRange*chunkSize,part=file.slice(start,start+chunksPerRange*chunkSize),hashes=manifest.hashes.slice(work.index*chunksPerRange,(work.index+1)*chunksPerRange);try{await sendStream(channel,part,{size:part.size,chunkSize,hashes,range:work.index,generation:work.generation},{windowChunks:32});scheduler.finish(lane,work);wake();}catch(e){scheduler.close(lane);if(window.intentionalFailure&&lane===0)record.reassignments++;else record.errors.push(e.message);wake();break;}}}));
   if(scheduler.snapshot().complete!==rangeCount)throw Error('Incomplete range assignments');
  };
  window.verify=async()=>{let bytes=0;for(let i=0;i<chunkCount;i++){const b=new Uint8Array(await store.get('lanes',i));if(b.length!==chunkSize||b.some(v=>v!==i%251))throw Error('Lane stored payload mismatch');bytes+=b.length;}return bytes;};
 }else{
  window.pc=new RTC({iceServers:[]});pc.onicecandidate=({candidate})=>{if(candidate)void ice(candidate.toJSON());};
  window.pendingIce=[];window.addIce=async candidate=>{if(pc.remoteDescription)await pc.addIceCandidate(candidate);else pendingIce.push(candidate);};
  window.setRemote=async desc=>{await pc.setRemoteDescription(desc);for(const c of pendingIce.splice(0))await pc.addIceCandidate(c);};
  window.rawInvalid=false;
  pc.ondatachannel=({channel:dc})=>{pc.channel=dc;dc.binaryType='arraybuffer';window.streamReceiver=receiveStream(dc,store,{onCommit:bytes=>received(bytes)});streamReceiver.done.then(()=>{record.complete=true;}).catch(e=>record.errors.push(e.message));};
  window.offer=async()=>{window.dc=pc.createDataChannel('raw',{ordered:true});const offer=await pc.createOffer();await pc.setLocalDescription(offer);return offer;};
  window.answer=async offer=>{await setRemote(offer);const answer=await pc.createAnswer();await pc.setLocalDescription(answer);return answer;};
  window.publish=async()=>{const file=testFile();const manifest=await makeManifest(file);await sendStream(dc,file,manifest);};
  window.verify=async()=>{let bytes=0;for(let i=0;i<totalBytes/65536;i++){const b=new Uint8Array(await store.get('stream',i));if(b.length!==65536||b.some(v=>v!==i%251))throw Error('Stream stored payload mismatch');bytes+=b.length;}return bytes;};
 }
 window.stop=()=>{window.shutdown=true;clearInterval(sampler);window.d?.close();for(const pc of peers)if(pc.signalingState!=='closed')pc.close();};
};
window.ready=true;

