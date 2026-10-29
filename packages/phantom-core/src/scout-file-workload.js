import {createFileDistribution} from './files.js';
import {sendFileChannelPayload,coalesceQueuedFileChunk} from './file-channel-payload.js';
/** Isolated synthetic file, using the production request/read/hash/write service.
 * The memory sink deliberately does not claim native Android storage coverage.
 */
export function createScoutFileWorkload({peer,lanes,channels,pcs,bytes,chunkSize,batchSize,sourceReadBlockBytes=0,requestWindow,requestTimeoutMs=10000,maxAttemptsPerPeer=2,queuePolicy,messageBytes=null,crypto=globalThis.crypto,signal,onChunk=()=>{},onSend=()=>{},onComplete=()=>{},onError=()=>{}}){
 if(!Number.isSafeInteger(bytes)||bytes<1||bytes>144*1024*1024||!Number.isInteger(chunkSize)||chunkSize<1024||chunkSize>262144||bytes%(lanes.length*chunkSize)||!Number.isInteger(batchSize)||batchSize<1||batchSize>64)throw TypeError('Invalid scout file workload');
 const listeners=new Set(),pending=new Map(),stored=new Map(),received=new Set(),submitted=new Map(lanes.map(l=>[l,0])),queued=new Map(lanes.map(l=>[l,new Map()]));let closed=false,manifest=null,requested=false,wireId=null;
 const chunksPerLane=bytes/chunkSize/lanes.length;
 const laneFor=index=>lanes[Math.floor(index/chunksPerLane)];
 const fire=(type,payload)=>{for(const fn of listeners)fn({type,payload});};
 const send=(lane,data)=>{if(closed||signal?.aborted)throw Error('Scout file workload stopped');channels[lane].send(data);submitted.set(lane,submitted.get(lane)+(typeof data==='string'?new TextEncoder().encode(data).byteLength:data.byteLength));};
 const core={onEvent(fn){listeners.add(fn);return()=>listeners.delete(fn);},getFileSendScope(to,file){return channels[laneFor(file.index)]??null;},command(type,payload){
  if(type==='SEND_DM'){send(Number.isInteger(payload.file?.index)?laneFor(payload.file.index):lanes[0],JSON.stringify({kind:'message',sys:payload.sys,file:payload.file}));return;}
  if(type==='SEND_FILE_BIN_CHUNK'){
   const lane=laneFor(payload.file.index);
   if(coalesceQueuedFileChunk({dc:channels[lane],submittedBytes:submitted.get(lane),queuedChunks:queued.get(lane),index:payload.file.index})){onSend(lane,{coalesced:true});fire('FILE_BIN_CHUNK_SENT',{...payload,ok:true,bufferWaitMs:0});return;}
   void sendFileChannelPayload({dc:channels[lane],pcs:[pcs[lane]],bytes:payload.bytes,file:payload.file,send:data=>send(lane,data),fragments:true,queuePolicy,messageBytes,timeoutMs:requestTimeoutMs,signal,assertLive(){if(closed||signal?.aborted)throw Error('Scout file workload stopped');}})
    .then(({bufferWaitMs})=>{queued.get(lane).set(payload.file.index,submitted.get(lane));onSend(lane,{bytes:payload.bytes.byteLength,bufferedBytes:channels[lane].bufferedAmount,bufferWaitMs});fire('FILE_BIN_CHUNK_SENT',{...payload,ok:true,bufferWaitMs});},error=>{fire('FILE_BIN_CHUNK_SENT',{...payload,ok:false,error:error.message});onError(error);});
  }
 }};
 const service=createFileDistribution({core,crypto,requestWindow,requestTimeoutMs,maxAttemptsPerPeer,maxChunkBytes:262144,
  chunkStore:{async put(id,index,data){stored.set(index,data.byteLength);},async get(){return undefined;},async getVerifiedIndexes(){return [...stored.keys()];},async getFile(id,m){return new Blob([new Uint8Array(m.size)]);}},
  emit(type,payload){if(type==='fileOffer'&&!requested){requested=true;manifest=payload.manifest;if(manifest.size!==bytes||manifest.chunkSize!==chunkSize){onError(Error('Scout workload manifest mismatch'));return;}
    void Promise.all(lanes.map(async lane=>{const start=lanes.indexOf(lane)*chunksPerLane;for(let base=start;base<start+chunksPerLane;base+=batchSize){
      const indexes=Array.from({length:Math.min(batchSize,start+chunksPerLane-base)},(_,i)=>base+i);
      await service.request(manifest.id,{peers:[peer],indexes,signal,concurrentRanges:true});
    }})).then(()=>onComplete(),onError);
   }else if(type==='fileError')onError(Error(payload.error??'Scout file service failed'));}
 });
 const abort=()=>api.close();signal?.addEventListener('abort',abort,{once:true});
 const api={
  async prepare(){manifest=await service.publish(new Blob([new Uint8Array(bytes)]),{chunkSize,sourceReadBlockBytes,name:'phantom-scout',distributionId:'scout'});wireId=manifest.id;return manifest;},
  async start(){if(!manifest)throw Error('Scout file not prepared');await service.share(peer,manifest.id);},
  receive(lane,data){
   if(closed)return;
   if(typeof data==='string'){
    if(data.length>16384)throw Error('Scout file frame too large');const m=JSON.parse(data);
    if(m.kind==='message'){
     const f=m.file;if(!f||typeof f.id!=='string'||!['FILE_OFFER','PHANTOM_FILE_MANIFEST','PHANTOM_FILE_REQUEST','PHANTOM_FILE_MISSING'].includes(m.sys))throw Error('Invalid scout file message');
     if(m.sys==='FILE_OFFER'){
      if(wireId!==null||f.phantom?.size!==bytes||f.phantom?.chunkSize!==chunkSize||f.phantom?.total!==bytes/chunkSize||f.phantom?.id!==f.id)throw Error('Invalid scout file offer');wireId=f.id;
     }else if(f.id!==wireId)throw Error('Foreign scout file');
     if(['PHANTOM_FILE_REQUEST','PHANTOM_FILE_MISSING'].includes(m.sys)&&(!Number.isInteger(f.index)||f.index<0||f.index>=bytes/chunkSize||laneFor(f.index)!==lane))throw Error('Invalid scout chunk request');
     fire('INCOMING_MSG',{from:peer,sys:m.sys,file:f});return;
    }
    if(!['binary','binary-fragment'].includes(m.kind)||pending.get(lane)?.header||!Number.isInteger(m.file?.index)||laneFor(m.file.index)!==lane||m.file.id!==wireId||m.file.bytes!==chunkSize)throw Error('Invalid scout file header');
    let row=pending.get(lane)??{};
    if(m.kind==='binary-fragment'){
     if(!Number.isInteger(m.offset)||!Number.isInteger(m.length)||m.length<1||m.offset<0||m.offset+m.length>m.file.bytes)throw Error('Invalid scout fragment');
     if(m.offset===0){if(row.buffer)throw Error('Overlapping scout fragment');row={buffer:new Uint8Array(m.file.bytes),offset:0,file:m.file};}
     if(!row.buffer||row.offset!==m.offset||row.file.id!==m.file.id||row.file.index!==m.file.index)throw Error('Invalid scout fragment order');
    }else if(row.buffer)throw Error('Interrupted scout fragment');
    row.header=m;pending.set(lane,row);return;
   }
   const row=pending.get(lane),m=row?.header;if(!(data instanceof ArrayBuffer)||!m)throw Error('Unexpected scout file payload');row.header=null;
   let complete=data;
   if(m.kind==='binary-fragment'){if(data.byteLength!==m.length)throw Error('Invalid scout fragment length');row.buffer.set(new Uint8Array(data),row.offset);row.offset+=data.byteLength;if(row.offset<row.buffer.byteLength)return;complete=row.buffer.buffer;}
   if(complete.byteLength!==m.file.bytes)throw Error('Invalid scout chunk length');pending.delete(lane);if(!received.has(m.file.index)){received.add(m.file.index);onChunk(lane,complete);}fire('FILE_BIN_CHUNK',{from:peer,file:m.file,bytes:complete});
  },
  snapshot(){return lanes.map(lane=>({lane,submittedBytes:submitted.get(lane),bufferedBytes:channels[lane].bufferedAmount,drainedBytes:Math.max(0,submitted.get(lane)-channels[lane].bufferedAmount)}));},
  close(){if(closed)return;closed=true;signal?.removeEventListener('abort',abort);service.close();pending.clear();stored.clear();listeners.clear();}
 };
 return api;
}
