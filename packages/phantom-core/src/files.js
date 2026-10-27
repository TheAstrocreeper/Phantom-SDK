import {createPreparedSendQueue,createFileTimings} from './file-pipeline.js';
import {createSourceBlockReader} from './source-block-reader.js';
const hex=b=>Array.from(new Uint8Array(b),x=>x.toString(16).padStart(2,'0')).join('');
const digest=async(b,crypto)=>hex(await crypto.subtle.digest('SHA-256',b));
const descriptor=m=>[m.name,m.type,m.size,m.chunkSize,m.total,m.hashRoot,m.distributionId];
export function createMemoryChunkStore(){const data=new Map();return {async get(id,index){return data.get(id+':'+index);},async put(id,index,bytes){data.set(id+':'+index,bytes.slice(0));}};}
/** Chunked file exchange over the peer data channel with backpressure. */
export function createFileDistribution({core,crypto=globalThis.crypto,emit=()=>{},allowed=()=>true,canUsePeer=()=>true,chunkStore=createMemoryChunkStore(),requestTimeoutMs=10000,requestWindow=8,maxAttemptsPerPeer=2,maxFileBytes=Number.MAX_SAFE_INTEGER}) {
 const manifests=new Map(),sources=new Map(),offers=new Map(),downloads=new Map(),startingDownloads=new Map(),sendTimings=new Map(),activeSends=new Set(),peerGenerations=new Map();let closed=false;
 const sendQueue=createPreparedSendQueue({maxPerPeer:32}),sourceReader=createSourceBlockReader({onRead:p=>emit('fileSourceRead',p)}),writeQueue=[];let activeWrites=0,queuedWrites=0;
 const maxReceiveSlots=32,writeConcurrency=4;
 const now=()=>performance.now();
 function senderMetrics(to,id){const key=to+':'+id;let row=sendTimings.get(key);if(!row){row={to,id,total:0,timings:createFileTimings()};sendTimings.set(key,row);}return row;}
 function flushTimings(to,id){for(const [key,row]of sendTimings)if((!to||row.to===to)&&(!id||row.id===id)){if(row.total)emit('fileTransferTiming',{id:row.id,to:row.to,phase:'send',total:row.total,metrics:row.timings.summary()});sendTimings.delete(key);}}

 if(!Number.isFinite(requestTimeoutMs)||requestTimeoutMs<=0||!Number.isSafeInteger(maxFileBytes)||maxFileBytes<0)throw new TypeError('Invalid file service limits');
 if(!Number.isInteger(requestWindow)||requestWindow<1||requestWindow>32||!Number.isInteger(maxAttemptsPerPeer)||maxAttemptsPerPeer<1||maxAttemptsPerPeer>3)throw new TypeError('Invalid file request window or retry limit');
 const check=id=>{if(closed)throw new Error('File service closed');const m=manifests.get(id);if(!m)throw new Error('Unknown file');if(!allowed(id,m.distributionId))throw new Error('Distribution revoked');return m;};
 const peerAllowed=peer=>{if(!canUsePeer(peer))throw new Error('Peer membership inactive');};
 const packet=(to,sys,file)=>{peerAllowed(to);core.command('SEND_DM',{to,sys,file});};
 function validManifest(m){return !!m&&!(m.version!==1||typeof m.id!=='string'||!/^sha256:[0-9a-f]{64}$/.test(m.id)||!Number.isSafeInteger(m.size)||m.size<0||m.size>maxFileBytes||!Number.isInteger(m.chunkSize)||m.chunkSize<1024||m.chunkSize>65536||m.total!==Math.ceil(m.size/m.chunkSize)||!Number.isSafeInteger(m.total)||typeof m.name!=='string'||m.name.length>256||typeof m.type!=='string'||m.type.length>128||typeof m.distributionId!=='string'||m.distributionId.length>256||! /^[0-9a-f]{64}$/.test(m.hashRoot));}
 async function readChunk(m,index,includeSource=false,timings,prefix=''){
  const readAt=now();
  const record=includeSource&&sources.get(m.id),source=record?.file;
  const bytes=source?(record.blockBytes?await sourceReader.read(record,index,m.chunkSize):await source.slice(index*m.chunkSize,(index+1)*m.chunkSize).arrayBuffer()):await chunkStore.get(m.id,index);
  timings?.add(prefix+'readMs',now()-readAt);
  if(!(bytes instanceof ArrayBuffer)||bytes.byteLength!==Math.min(m.chunkSize,m.size-index*m.chunkSize))return null;
  const hashAt=now(),valid=await digest(bytes,crypto)===m.hashes[index];timings?.add(prefix+'hashMs',now()-hashAt);return valid?bytes:null;
 }
 async function restore(manifest){
  if(closed)throw new Error('File service closed');
  const m=structuredClone(manifest);
  if(!validManifest(m)||!Array.isArray(m.hashes)||m.hashes.length!==m.total||m.hashes.some(h=>! /^[0-9a-f]{64}$/.test(h)))throw new Error('Invalid stored manifest');
  await finishManifest(null,{m,hashes:new Map(m.hashes.map((h,i)=>[i,h]))});
  const indexes=[];for(let i=0;i<m.total;i++){check(m.id);if(await readChunk(m,i))indexes.push(i);}
  check(m.id);return {manifest:publicManifest(check(m.id)),indexes};
 }
 async function publish(file,{name=file?.name||'file',type=file?.type||'application/octet-stream',chunkSize=16384,distributionId='',sourceReadBlockBytes=0}={}){
  if(closed)throw new Error('File service closed');if(!file?.slice||!Number.isSafeInteger(file.size)||file.size<0||file.size>maxFileBytes)throw new TypeError('Supply a Blob within maxFileBytes');
  if(!Number.isInteger(chunkSize)||chunkSize<1024||chunkSize>65536)throw new TypeError('Chunk size must be 1024–65536 bytes');
  if(![0,4*1024*1024].includes(sourceReadBlockBytes))throw new TypeError('Source read block must be 0 or 4 MiB');
  if(typeof name!=='string'||name.length>256||typeof type!=='string'||type.length>128||typeof distributionId!=='string'||distributionId.length>256)throw new TypeError('Invalid file metadata');
  const total=Math.ceil(file.size/chunkSize),hashes=new Array(total),startedAt=performance.now();let readMs=0,hashMs=0;
  // Bound temporary file data to 4 MiB and outstanding WebCrypto jobs to eight.
  const batchChunks=Math.max(1,Math.floor(4*1024*1024/chunkSize));
  for(let base=0;base<total;base+=batchChunks){
   if(closed||!allowed('',distributionId))throw new Error('Distribution unavailable');
   const end=Math.min(total,base+batchChunks),readAt=performance.now();
   const bytes=await file.slice(base*chunkSize,Math.min(file.size,end*chunkSize)).arrayBuffer();readMs+=performance.now()-readAt;
   const hashAt=performance.now();
   for(let offset=base;offset<end;offset+=8){
    if(closed||!allowed('',distributionId))throw new Error('Distribution unavailable');
    await Promise.all(Array.from({length:Math.min(8,end-offset)},async(_,j)=>{const index=offset+j,start=(index-base)*chunkSize;hashes[index]=await digest(new Uint8Array(bytes,start,Math.min(chunkSize,bytes.byteLength-start)),crypto);}));
   }
   hashMs+=performance.now()-hashAt;
   if(closed||!allowed('',distributionId))throw new Error('Distribution unavailable');
   emit('filePreparationProgress',{name,processed:end,total,readMs,hashMs,elapsedMs:performance.now()-startedAt});
  }
  const m={version:1,name,type,size:file.size,chunkSize,total:hashes.length,hashRoot:await digest(new TextEncoder().encode(hashes.join('')),crypto),distributionId};
  m.id='sha256:'+await digest(new TextEncoder().encode(JSON.stringify(descriptor(m))),crypto);m.hashes=hashes;checkAllowed(m);if(typeof chunkStore.save==='function')for(let i=0;i<m.total;i++){checkAllowed(m);if(closed)throw new Error('File service closed');await chunkStore.put(m.id,i,await file.slice(i*m.chunkSize,(i+1)*m.chunkSize).arrayBuffer());}checkAllowed(m);if(closed)throw new Error('File service closed');manifests.set(m.id,m);sources.set(m.id,{file,blockBytes:sourceReadBlockBytes,size:file.size,slice:(start,end)=>file.slice(start,end)});emit('filePublished',publicManifest(m));return publicManifest(m);
 }
 function checkAllowed(m){if(!allowed(m.id,m.distributionId))throw new Error('Distribution revoked');}
 function publicManifest(m){return structuredClone(m);}
 async function share(to,id){peerAllowed(to);const m=check(id);const {hashes,...header}=m;packet(to,'FILE_OFFER',{id,name:m.name,size:m.size,type:m.type,phantom:header});for(let offset=0;offset<hashes.length;offset+=64){check(id);packet(to,'PHANTOM_FILE_MANIFEST',{id,offset,hashes:hashes.slice(offset,offset+64)});}return publicManifest(m);}
 async function finishManifest(from,row){const m=row.m;if(row.hashes.size!==m.total)return;const hashes=Array.from({length:m.total},(_,i)=>row.hashes.get(i));if(await digest(new TextEncoder().encode(hashes.join('')),crypto)!==m.hashRoot)throw new Error('Manifest hash mismatch');if('sha256:'+await digest(new TextEncoder().encode(JSON.stringify(descriptor(m))),crypto)!==m.id)throw new Error('Manifest identity mismatch');if(closed)throw new Error('File service closed');checkAllowed(m);const verified={...m,hashes};if(typeof chunkStore.begin==='function')await chunkStore.begin(m.id,publicManifest(verified));if(closed)throw new Error('File service closed');checkAllowed(m);manifests.set(m.id,verified);if(from!==null){offers.delete(from+':'+m.id);emit('fileOffer',{from,manifest:publicManifest(manifests.get(m.id))});}}
 async function sendChunk(to,id,index,generation,prepared){peerAllowed(to);if((peerGenerations.get(to)||0)!==generation)return;const m=check(id);
  const {bytes,timings}=prepared;
  if(!bytes){packet(to,'PHANTOM_FILE_MISSING',{id,index});return;}
  const row=senderMetrics(to,id);for(const [stage,ms]of Object.entries(timings))row.timings.add(stage,ms);
  const submitAt=now();
  await new Promise((resolve,reject)=>{const cancel=()=>finish(new Error('File service closed'));const finish=error=>{clearTimeout(timer);off();activeSends.delete(cancel);error?reject(error):resolve();};const timer=setTimeout(()=>finish(new Error('Chunk send timed out')),requestTimeoutMs);const off=core.onEvent(e=>{const f=e.payload?.file;if(e.type!=='FILE_BIN_CHUNK_SENT'||e.payload.to!==to||f?.id!==id||f.index!==index)return;if(e.payload.ok&&Number.isFinite(e.payload.bufferWaitMs))row.timings.add('bufferWaitMs',e.payload.bufferWaitMs);finish(e.payload.ok?null:new Error(e.payload.error));});cancel.peer=to;activeSends.add(cancel);core.command('SEND_FILE_BIN_CHUNK',{to,file:{...m,hashes:undefined,index,bytes:bytes.byteLength},bytes});});
  row.timings.add('submitMs',now()-submitAt);row.total++;if(row.total>=32)flushTimings(to,id);
 }
 function queueChunk(to,id,index){
  const m=check(id);if(!Number.isInteger(index)||index<0||index>=m.total)return;
  const generation=peerGenerations.get(to)||0,queuedAt=now();
  const next=sendQueue.enqueue(to,async()=>{
   check(id);peerAllowed(to);if((peerGenerations.get(to)||0)!==generation)throw Error('Peer membership inactive');
   const timings={},queueMs=now()-queuedAt;
   const recorder={add:(key,ms)=>timings[key]=ms};const bytes=await readChunk(m,index,true,recorder);
   check(id);peerAllowed(to);if((peerGenerations.get(to)||0)!==generation)throw Error('Peer membership inactive');
   return {bytes,timings:{...timings,queueMs}};
  },prepared=>sendChunk(to,id,index,generation,prepared));
  next?.catch(error=>{if(!closed)emit('fileError',{id,from:to,error:error.message});});
 }
 function receiveSlots(){return queuedWrites+[...downloads.values()].reduce((n,d)=>n+d.pending.size,0);}
 function pumpAll(){for(const d of downloads.values())pump(d.m.id,d.key);}
 function drainWrites(){while(activeWrites<writeConcurrency&&writeQueue.length){const task=writeQueue.shift();activeWrites++;
  Promise.resolve().then(async()=>{
   const {id,index,bytes,d,from}=task;
   if(downloads.get(d.key)!==d)return;
   check(id);peerAllowed(from);d.timings.add('writeQueueMs',now()-task.at);const started=now();
   await chunkStore.put(id,index,bytes);d.timings.add('writeMs',now()-started);
   if(downloads.get(d.key)!==d)return;check(id);peerAllowed(from);
   d.writes.delete(index);d.have.add(index);d.verifiedBytes+=bytes.byteLength;d.verificationMs+=now()-task.verificationStartedAt;
   emit('fileProgress',{id,index,bytes:bytes.byteLength,verifiedBytes:d.verifiedBytes,elapsedMs:now()-d.startedAt,verificationMs:d.verificationMs,inFlight:d.pending.size,received:d.have.size,total:d.m.total,from});
  }).catch(error=>{if(downloads.get(task.d.key)===task.d){fail(task.id,error,task.d.key);emit('fileError',{id:task.id,error:error.message});}}).finally(()=>{
   clearTimeout(task.timer);activeWrites--;queuedWrites--;task.d.writes.delete(task.index);drainWrites();pumpAll();
  });
 }}
 async function verifiedIndexes(m,wanted){const {hashes,...metadata}=m;const indexes=await chunkStore.getVerifiedIndexes(m.id,metadata,wanted);if(!Array.isArray(indexes)||indexes.some(i=>!Number.isInteger(i)||i<0||i>=m.total))throw new Error('Invalid verified storage inventory');return new Set(indexes);}
 async function request(id,{peers,signal,indexes,requestWindow:window=requestWindow,concurrentRanges=false}={}){
  const m=check(id);if(!Array.isArray(peers)||!peers.length||peers.some(p=>! /^[0-9a-f]{16}$/.test(p)))throw new TypeError('Supply connected peer IDs');if(signal?.aborted)throw new Error('File download aborted');
  if(indexes!==undefined&&(!Array.isArray(indexes)||indexes.some(i=>!Number.isInteger(i)||i<0||i>=m.total)))throw new TypeError('Invalid chunk indexes');
  if(!Number.isInteger(window)||window<1||window>32)throw new TypeError('Invalid file request window');
  if(typeof concurrentRanges!=='boolean'||(concurrentRanges&&indexes===undefined))throw new TypeError('Concurrent ranges require explicit indexes');
  const wanted=new Set(indexes??Array.from({length:m.total},(_,i)=>i));
  const other=[...downloads.values()].filter(d=>d.m.id===id).concat([...(startingDownloads.get(id)??[])]);
  if(other.some(d=>!concurrentRanges||!d.concurrentRanges||[...wanted].some(i=>d.wanted.has(i))))throw new Error('File download already active');
  const reservation={wanted,concurrentRanges},key=concurrentRanges?Symbol(id):id;
  if(!startingDownloads.has(id))startingDownloads.set(id,new Set());startingDownloads.get(id).add(reservation);
  try{
  const trustedWrites=typeof chunkStore.getVerifiedIndexes==='function';
  const committed=trustedWrites?await verifiedIndexes(m,[...wanted]):null;
  if(signal?.aborted)throw new Error('File download aborted');
  const have=new Set();for(const i of wanted){if(signal?.aborted)throw new Error('File download aborted');if(trustedWrites?committed.has(i):await readChunk(m,i))have.add(i);}
  if(signal?.aborted)throw new Error('File download aborted');
  check(id);
  const order=[...wanted].filter(i=>!have.has(i)).sort((a,b)=>a-b);
  return new Promise((resolve,reject)=>{const d={key,concurrentRanges,m,wanted,order,available:new Set(order),selections:new Map(),trustedWrites,requestWindow:window,partial:indexes!==undefined,peers:[...new Set(peers)].filter(canUsePeer),have,startedAt:now(),peerProgress:new Map(),verifiedBytes:0,verificationMs:0,verified:new Set(have),writes:new Map(),timings:createFileTimings(),pending:new Map(),attempts:new Map(),resolve,reject,signal,abort:()=>fail(id,new Error('File download aborted'),key)};downloads.set(key,d);signal?.addEventListener('abort',d.abort,{once:true});if(signal?.aborted){d.abort();return;}pump(id,key);});
  }finally{const starts=startingDownloads.get(id);starts?.delete(reservation);if(starts?.size===0)startingDownloads.delete(id);}
 }
 function fail(id,error,key=id){const d=downloads.get(key);if(!d)return;downloads.delete(key);emit('fileTransferTiming',{id,phase:'receive',status:'failed',total:d.have.size,metrics:d.timings.summary()});for(const p of d.pending.values())clearTimeout(p.timer);for(const task of d.writes.values())clearTimeout(task.timer);for(let i=writeQueue.length-1;i>=0;i--)if(writeQueue[i].d===d){writeQueue.splice(i,1);queuedWrites--;}d.writes.clear();d.signal?.removeEventListener('abort',d.abort);d.reject(error);queueMicrotask(pumpAll);}
 async function complete(id,key=id){const d=downloads.get(key);if(!d)return;try{check(id);const parts=[];if(!d.trustedWrites)for(const i of [...d.wanted].sort((a,b)=>a-b)){const bytes=await readChunk(d.m,i,false,d.timings,'cache_');if(!bytes)throw new Error('Stored file chunk failed verification');if(!d.partial&&typeof chunkStore.getFile!=='function')parts.push(bytes);}check(id);if(downloads.get(d.key)!==d)return;emit('fileTransferTiming',{id,phase:'receive',total:d.have.size,metrics:d.timings.summary()});if(d.partial){const indexes=[...d.wanted].sort((a,b)=>a-b);downloads.delete(d.key);d.signal?.removeEventListener('abort',d.abort);emit('fileChunksComplete',{id,indexes});d.resolve({id,indexes});return;}const blob=typeof chunkStore.getFile==='function'?await chunkStore.getFile(id,publicManifest(d.m)):new Blob(parts,{type:d.m.type});check(id);if(closed)throw new Error('File service closed');if(!blob||typeof blob.slice!=='function'||blob.size!==d.m.size)throw new Error('Stored file size mismatch');if(downloads.get(d.key)!==d)return;downloads.delete(d.key);d.signal?.removeEventListener('abort',d.abort);emit('fileComplete',{id,size:blob.size});d.resolve(blob);}catch(error){if(downloads.get(d.key)===d)fail(id,error,d.key);}}
 // Each peer walks the missing order once; only failed earlier indexes re-enter its queue.
 function returnIndex(d,index){
  d.available.add(index);
  for(const [peer,selection]of d.selections)if(selection.cursor&&index<=d.order[selection.cursor-1]&&(d.attempts.get(index)?.get(peer)||0)<maxAttemptsPerPeer)selection.retries.add(index);
 }
 function nextIndex(d,peer){
  let selection=d.selections.get(peer);
  if(!selection){selection={cursor:0,retries:new Set()};d.selections.set(peer,selection);}
  const take=index=>{if(!d.available.has(index)||(d.attempts.get(index)?.get(peer)||0)>=maxAttemptsPerPeer)return false;d.available.delete(index);return true;};
  for(const index of selection.retries){selection.retries.delete(index);if(take(index))return index;}
  while(selection.cursor<d.order.length){const index=d.order[selection.cursor++];if(take(index))return index;}
  return -1;
 }
 function pump(id,key=id){const d=downloads.get(key);if(!d)return;try{check(id);}catch(e){fail(id,e,key);return;}if(d.have.size===d.wanted.size){if(!d.completing){d.completing=true;void complete(id,key);}return;}
  const activeRanges=d.concurrentRanges?[...downloads.values()].filter(other=>other.m.id===id&&other.concurrentRanges).length:1;
  const window=Math.min(d.requestWindow,Math.max(1,Math.floor(maxReceiveSlots/activeRanges)));
  for(const peer of d.peers){let outstanding=[...d.pending.values()].filter(p=>p.peer===peer).length;
   while(outstanding<window&&receiveSlots()<maxReceiveSlots){const index=nextIndex(d,peer);if(index<0)break;
    if(!d.attempts.has(index))d.attempts.set(index,new Map());const attempts=d.attempts.get(index);attempts.set(peer,(attempts.get(peer)||0)+1);
    const pending={peer,processing:false,requestedAt:now(),requestedWall:Date.now()};
    const expire=()=>{if(downloads.get(d.key)!==d||d.pending.get(index)!==pending)return;if(pending.processing){fail(id,new Error('Chunk verification timed out'),key);return;}
     // Other verified chunks prove the same source is still delivering its window.
     const remaining=requestTimeoutMs-(Date.now()-Math.max(pending.requestedWall,d.peerProgress.get(peer)??0));
     if(remaining>0){pending.timer=setTimeout(expire,remaining);return;}
     d.pending.delete(index);returnIndex(d,index);emit('fileRetry',{id,index,from:peer,attempt:attempts.get(peer)});pump(id,key);
    };pending.timer=setTimeout(expire,requestTimeoutMs);
    d.pending.set(index,pending);outstanding++;packet(peer,'PHANTOM_FILE_REQUEST',{id,index});
   }
  }
  if(!d.pending.size&&!d.writes.size&&receiveSlots()<maxReceiveSlots&&d.have.size<d.wanted.size)fail(id,new Error('No source supplied the remaining chunks; retry to resume'),key);
 }
 async function handle(event){if(closed)return;const p=event.payload||{},f=p.file,from=String(p.from||'').slice(0,16);if(from&&!canUsePeer(from))return;
  if(event.type==='INCOMING_MSG'&&p.sys==='FILE_OFFER'&&f?.phantom){const m=f.phantom;
   if(!validManifest(m))return;
   checkAllowed(m);if(offers.size>=128)return;const row={m:Object.fromEntries(['version','id','name','type','size','chunkSize','total','hashRoot','distributionId'].map(k=>[k,m[k]])),hashes:new Map()};offers.set(from+':'+m.id,row);if(!m.total)await finishManifest(from,row);return;
  }
  if(event.type==='INCOMING_MSG'&&p.sys==='PHANTOM_FILE_MANIFEST'){const row=offers.get(from+':'+f?.id);if(!row||!Array.isArray(f.hashes)||f.hashes.length>64||!Number.isInteger(f.offset)||f.offset<0||f.offset+f.hashes.length>row.m.total||f.hashes.some(h=>! /^[0-9a-f]{64}$/.test(h)))return;for(let i=0;i<f.hashes.length;i++)row.hashes.set(f.offset+i,f.hashes[i]);await finishManifest(from,row);return;}
  if(event.type==='INCOMING_MSG'&&p.sys==='PHANTOM_FILE_REQUEST'){if(manifests.has(f?.id)&&allowed(f.id,manifests.get(f.id).distributionId))queueChunk(from,f.id,f.index);return;}
  if(event.type==='INCOMING_MSG'&&p.sys==='PHANTOM_FILE_MISSING'){const d=[...downloads.values()].find(d=>d.m.id===f?.id&&d.pending.get(f.index)?.peer===from),pending=d?.pending.get(f.index);if(pending?.peer===from){d.attempts.get(f.index)?.set(from,maxAttemptsPerPeer);clearTimeout(pending.timer);d.pending.delete(f.index);returnIndex(d,f.index);pump(f.id,d.key);}return;}
  if(event.type==='FILE_BIN_CHUNK'){const d=[...downloads.values()].find(d=>d.m.id===f?.id&&d.pending.get(f.index)?.peer===from),pending=d?.pending.get(f.index);if(!d||pending?.peer!==from||pending.processing)return;pending.processing=true;check(f.id);d.timings.add('requestToReceiveMs',now()-pending.requestedAt);const hashAt=now();
   if(!(p.bytes instanceof ArrayBuffer)||p.bytes.byteLength!==Math.min(d.m.chunkSize,d.m.size-f.index*d.m.chunkSize)||await digest(p.bytes,crypto)!==d.m.hashes[f.index]){if(downloads.get(d.key)!==d||d.pending.get(f.index)!==pending)return;d.attempts.get(f.index)?.set(from,maxAttemptsPerPeer);clearTimeout(pending.timer);d.pending.delete(f.index);returnIndex(d,f.index);emit('fileError',{id:f.id,from,error:'Chunk failed verification'});pump(f.id,d.key);return;}
   d.timings.add('hashMs',now()-hashAt);check(f.id);if(!canUsePeer(from)||downloads.get(d.key)!==d||d.pending.get(f.index)!==pending)return;
   clearTimeout(pending.timer);d.pending.delete(f.index);d.verified.add(f.index);d.peerProgress.set(from,Date.now());
   const task={id:f.id,index:f.index,bytes:p.bytes,d,from,at:now(),verificationStartedAt:hashAt};task.timer=setTimeout(()=>{if(downloads.get(d.key)===d&&d.writes.get(f.index)===task)fail(f.id,new Error('Chunk storage timed out'),d.key);},requestTimeoutMs);d.writes.set(f.index,task);queuedWrites++;writeQueue.push(task);drainWrites();if(d.concurrentRanges)pumpAll();else pump(f.id,d.key);
  }
 }
 const off=core.onEvent(e=>{const id=e.payload?.file?.id,index=e.payload?.file?.index,from=String(e.payload?.from||'').slice(0,16),owner=e.type==='FILE_BIN_CHUNK'?[...downloads.values()].find(d=>d.m.id===id&&d.pending.get(index)?.peer===from):null;void handle(e).catch(error=>{if(e.type==='FILE_BIN_CHUNK'&&(!owner||downloads.get(owner.key)!==owner))return;if(owner)fail(id,error,owner.key);else if(id)fail(id,error);emit('fileError',{error:error.message});});});
 return {publish,share,request,restore,flushTimings,dropPeer(peer){
  flushTimings(peer);peerGenerations.set(peer,(peerGenerations.get(peer)||0)+1);
  for(const cancel of [...activeSends])if(cancel.peer===peer)cancel();
  for(const key of offers.keys())if(key.startsWith(peer+':'))offers.delete(key);
  for(const [key,d]of downloads){const id=d.m.id;d.peers=d.peers.filter(p=>p!==peer);d.selections.delete(peer);for(const [index,p]of d.pending)if(p.peer===peer){clearTimeout(p.timer);d.pending.delete(index);returnIndex(d,index);}pump(id,key);}
 },async getFile(id){const m=check(id),parts=[];if(!sources.has(id)&&typeof chunkStore.getVerifiedIndexes==='function'&&typeof chunkStore.getFile==='function'){const known=await verifiedIndexes(m);check(id);if(known.size!==m.total)throw new Error('No complete local file copy');const file=await chunkStore.getFile(id,publicManifest(m));check(id);if(!file||file.size!==m.size||typeof file.slice!=='function')throw new Error('Stored file size mismatch');return file;}for(let i=0;i<m.total;i++){const bytes=await readChunk(m,i,true);if(!bytes)throw new Error('No complete local file copy');if(typeof chunkStore.getFile!=='function'||sources.has(id))parts.push(bytes);}check(id);return typeof chunkStore.getFile==='function'&&!sources.has(id)?chunkStore.getFile(id,publicManifest(m)):new Blob(parts,{type:m.type});},cancel:id=>{for(const d of [...downloads.values()])if(d.m.id===id)fail(id,new Error('File download cancelled'),d.key);},getManifest:id=>publicManifest(check(id)),stop(target){sourceReader.clear();for(const d of [...downloads.values()])if(d.m.id===target||d.m.distributionId===target)fail(d.m.id,new Error('Distribution revoked'),d.key);for(const [key,row]of offers)if(row.m.id===target||row.m.distributionId===target)offers.delete(key);},close(){if(closed)return;closed=true;sourceReader.close();sendQueue.close();flushTimings();off();for(const cancel of [...activeSends])cancel();for(const d of [...downloads.values()])fail(d.m.id,new Error('File service closed'),d.key);offers.clear();sources.clear();manifests.clear();}};
}
