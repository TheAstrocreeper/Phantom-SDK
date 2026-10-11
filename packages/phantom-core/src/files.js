const hex=b=>Array.from(new Uint8Array(b),x=>x.toString(16).padStart(2,'0')).join('');
const digest=async(b,crypto)=>hex(await crypto.subtle.digest('SHA-256',b));
const descriptor=m=>[m.name,m.type,m.size,m.chunkSize,m.total,m.hashRoot,m.distributionId];
export function createMemoryChunkStore(){const data=new Map();return {async get(id,index){return data.get(id+':'+index);},async put(id,index,bytes){data.set(id+':'+index,bytes.slice(0));}};}
/** Reuses SIX7 FILE_OFFER, binary metadata/data and its channel backpressure. */
export function createFileDistribution({core,crypto=globalThis.crypto,emit=()=>{},allowed=()=>true,chunkStore=createMemoryChunkStore(),requestTimeoutMs=10000,maxFileBytes=1024*1024*1024}) {
 const manifests=new Map(),sources=new Map(),offers=new Map(),downloads=new Map(),sendQueues=new Map(),queueCounts=new Map(),activeSends=new Set();let closed=false;
 if(!Number.isFinite(requestTimeoutMs)||requestTimeoutMs<=0||!Number.isSafeInteger(maxFileBytes)||maxFileBytes<0)throw new TypeError('Invalid file service limits');
 const check=id=>{if(closed)throw new Error('File service closed');const m=manifests.get(id);if(!m)throw new Error('Unknown file');if(!allowed(id,m.distributionId))throw new Error('Distribution revoked');return m;};
 const packet=(to,sys,file)=>core.command('SEND_DM',{to,sys,file});
 async function publish(file,{name=file?.name||'file',type=file?.type||'application/octet-stream',chunkSize=16384,distributionId=''}={}){
  if(closed)throw new Error('File service closed');if(!file?.slice||!Number.isSafeInteger(file.size)||file.size<0||file.size>maxFileBytes)throw new TypeError('Supply a Blob within maxFileBytes');
  if(!Number.isInteger(chunkSize)||chunkSize<1024||chunkSize>65536)throw new TypeError('Chunk size must be 1024–65536 bytes');
  if(typeof name!=='string'||name.length>256||typeof type!=='string'||type.length>128||typeof distributionId!=='string'||distributionId.length>256)throw new TypeError('Invalid file metadata');
  if(Math.ceil(file.size/chunkSize)>65536)throw new TypeError('At most 65536 chunks per file');
  const hashes=[];for(let i=0;i<Math.ceil(file.size/chunkSize);i++){if(closed||!allowed('',distributionId))throw new Error('Distribution unavailable');hashes.push(await digest(await file.slice(i*chunkSize,(i+1)*chunkSize).arrayBuffer(),crypto));}
  const m={version:1,name,type,size:file.size,chunkSize,total:hashes.length,hashRoot:await digest(new TextEncoder().encode(hashes.join('')),crypto),distributionId};
  m.id='sha256:'+await digest(new TextEncoder().encode(JSON.stringify(descriptor(m))),crypto);m.hashes=hashes;checkAllowed(m);manifests.set(m.id,m);sources.set(m.id,file);emit('filePublished',publicManifest(m));return publicManifest(m);
 }
 function checkAllowed(m){if(!allowed(m.id,m.distributionId))throw new Error('Distribution revoked');}
 function publicManifest(m){return structuredClone(m);}
 async function share(to,id){const m=check(id);const {hashes,...header}=m;packet(to,'FILE_OFFER',{id,name:m.name,size:m.size,type:m.type,phantom:header});for(let offset=0;offset<hashes.length;offset+=64){check(id);packet(to,'PHANTOM_FILE_MANIFEST',{id,offset,hashes:hashes.slice(offset,offset+64)});}return publicManifest(m);}
 async function finishManifest(from,row){const m=row.m;if(row.hashes.size!==m.total)return;const hashes=Array.from({length:m.total},(_,i)=>row.hashes.get(i));if(await digest(new TextEncoder().encode(hashes.join('')),crypto)!==m.hashRoot)throw new Error('Manifest hash mismatch');if('sha256:'+await digest(new TextEncoder().encode(JSON.stringify(descriptor(m))),crypto)!==m.id)throw new Error('Manifest identity mismatch');checkAllowed(m);manifests.set(m.id,{...m,hashes});offers.delete(from+':'+m.id);emit('fileOffer',{from,manifest:publicManifest(manifests.get(m.id))});}
 async function sendChunk(to,id,index){const m=check(id);if(!Number.isInteger(index)||index<0||index>=m.total)return;
  const source=sources.get(id);const bytes=source?await source.slice(index*m.chunkSize,(index+1)*m.chunkSize).arrayBuffer():await chunkStore.get(id,index);
  check(id);if(bytes&&await digest(bytes,crypto)!==m.hashes[index])throw new Error('Source chunk failed verification');
  check(id);if(!bytes){packet(to,'PHANTOM_FILE_MISSING',{id,index});return;}
  await new Promise((resolve,reject)=>{const cancel=()=>finish(new Error('File service closed'));const finish=error=>{clearTimeout(timer);off();activeSends.delete(cancel);error?reject(error):resolve();};const timer=setTimeout(()=>finish(new Error('Chunk send timed out')),requestTimeoutMs);const off=core.onEvent(e=>{const f=e.payload?.file;if(e.type!=='FILE_BIN_CHUNK_SENT'||e.payload.to!==to||f?.id!==id||f.index!==index)return;finish(e.payload.ok?null:new Error(e.payload.error));});activeSends.add(cancel);core.command('SEND_FILE_BIN_CHUNK',{to,file:{...m,hashes:undefined,index,bytes:bytes.byteLength},bytes});});
 }
 function queueChunk(to,id,index){if((queueCounts.get(to)||0)>=16)return;queueCounts.set(to,(queueCounts.get(to)||0)+1);const previous=sendQueues.get(to)||Promise.resolve();const next=previous.catch(()=>{}).then(()=>sendChunk(to,id,index));sendQueues.set(to,next);next.catch(error=>emit('fileError',{id,from:to,error:error.message})).finally(()=>{queueCounts.set(to,Math.max(0,(queueCounts.get(to)||1)-1));if(sendQueues.get(to)===next)sendQueues.delete(to);});}
 async function request(id,{peers,signal,indexes}={}){
  const m=check(id);if(!Array.isArray(peers)||!peers.length||peers.some(p=>! /^[0-9a-f]{16}$/.test(p)))throw new TypeError('Supply connected peer IDs');if(downloads.has(id))throw new Error('File download already active');if(signal?.aborted)throw new Error('File download aborted');
  if(indexes!==undefined&&(!Array.isArray(indexes)||indexes.some(i=>!Number.isInteger(i)||i<0||i>=m.total)))throw new TypeError('Invalid chunk indexes');
  const wanted=new Set(indexes??Array.from({length:m.total},(_,i)=>i));
  const have=new Set();for(const i of wanted){const bytes=await chunkStore.get(id,i);if(bytes&&bytes.byteLength===Math.min(m.chunkSize,m.size-i*m.chunkSize)&&await digest(bytes,crypto)===m.hashes[i])have.add(i);}
  check(id);
  return new Promise((resolve,reject)=>{const d={m,wanted,partial:indexes!==undefined,peers:[...new Set(peers)],have,pending:new Map(),attempts:new Map(),resolve,reject,signal,abort:()=>fail(id,new Error('File download aborted'))};downloads.set(id,d);signal?.addEventListener('abort',d.abort,{once:true});if(signal?.aborted){d.abort();return;}pump(id);});
 }
 function fail(id,error){const d=downloads.get(id);if(!d)return;downloads.delete(id);for(const p of d.pending.values())clearTimeout(p.timer);d.signal?.removeEventListener('abort',d.abort);d.reject(error);}
 async function complete(id){const d=downloads.get(id);if(!d)return;try{check(id);const parts=[];for(const i of [...d.wanted].sort((a,b)=>a-b)){const bytes=await chunkStore.get(id,i);if(!bytes||await digest(bytes,crypto)!==d.m.hashes[i])throw new Error('Stored file chunk failed verification');parts.push(bytes);}check(id);if(downloads.get(id)!==d)return;downloads.delete(id);d.signal?.removeEventListener('abort',d.abort);if(d.partial){const indexes=[...d.wanted].sort((a,b)=>a-b);emit('fileChunksComplete',{id,indexes});d.resolve({id,indexes});return;}const blob=new Blob(parts,{type:d.m.type});emit('fileComplete',{id,size:blob.size});d.resolve(blob);}catch(error){fail(id,error);}}
 function pump(id){const d=downloads.get(id);if(!d)return;try{check(id);}catch(e){fail(id,e);return;}if(d.have.size===d.wanted.size){if(!d.completing){d.completing=true;void complete(id);}return;}
  for(const peer of d.peers){if([...d.pending.values()].some(p=>p.peer===peer))continue;let index=-1;for(let i=0;i<d.m.total;i++)if(d.wanted.has(i)&&!d.have.has(i)&&!d.pending.has(i)&&!d.attempts.get(i)?.has(peer)){index=i;break;}if(index<0)continue;
   if(!d.attempts.has(index))d.attempts.set(index,new Set());d.attempts.get(index).add(peer);
   const timer=setTimeout(()=>{if(downloads.get(id)!==d)return;d.pending.delete(index);pump(id);},requestTimeoutMs);d.pending.set(index,{peer,timer});packet(peer,'PHANTOM_FILE_REQUEST',{id,index});
  }
  if(!d.pending.size&&d.have.size<d.wanted.size)fail(id,new Error('No source supplied the remaining chunks; retry to resume'));
 }
 async function handle(event){if(closed)return;const p=event.payload||{},f=p.file,from=String(p.from||'').slice(0,16);
  if(event.type==='INCOMING_MSG'&&p.sys==='FILE_OFFER'&&f?.phantom){const m=f.phantom;
   if(m.version!==1||typeof m.id!=='string'||!/^sha256:[0-9a-f]{64}$/.test(m.id)||!Number.isSafeInteger(m.size)||m.size<0||m.size>maxFileBytes||!Number.isInteger(m.chunkSize)||m.chunkSize<1024||m.chunkSize>65536||m.total!==Math.ceil(m.size/m.chunkSize)||m.total>65536||typeof m.name!=='string'||m.name.length>256||typeof m.type!=='string'||m.type.length>128||typeof m.distributionId!=='string'||m.distributionId.length>256||! /^[0-9a-f]{64}$/.test(m.hashRoot))return;
   checkAllowed(m);if(offers.size>=128)return;const row={m:Object.fromEntries(['version','id','name','type','size','chunkSize','total','hashRoot','distributionId'].map(k=>[k,m[k]])),hashes:new Map()};offers.set(from+':'+m.id,row);if(!m.total)await finishManifest(from,row);return;
  }
  if(event.type==='INCOMING_MSG'&&p.sys==='PHANTOM_FILE_MANIFEST'){const row=offers.get(from+':'+f?.id);if(!row||!Array.isArray(f.hashes)||f.hashes.length>64||!Number.isInteger(f.offset)||f.offset<0||f.offset+f.hashes.length>row.m.total||f.hashes.some(h=>! /^[0-9a-f]{64}$/.test(h)))return;for(let i=0;i<f.hashes.length;i++)row.hashes.set(f.offset+i,f.hashes[i]);await finishManifest(from,row);return;}
  if(event.type==='INCOMING_MSG'&&p.sys==='PHANTOM_FILE_REQUEST'){if(manifests.has(f?.id)&&allowed(f.id,manifests.get(f.id).distributionId))queueChunk(from,f.id,f.index);return;}
  if(event.type==='INCOMING_MSG'&&p.sys==='PHANTOM_FILE_MISSING'){const d=downloads.get(f?.id),pending=d?.pending.get(f.index);if(pending?.peer===from){clearTimeout(pending.timer);d.pending.delete(f.index);pump(f.id);}return;}
  if(event.type==='FILE_BIN_CHUNK'){const d=downloads.get(f?.id),pending=d?.pending.get(f.index);if(!d||pending?.peer!==from)return;check(f.id);
   if(!(p.bytes instanceof ArrayBuffer)||p.bytes.byteLength!==Math.min(d.m.chunkSize,d.m.size-f.index*d.m.chunkSize)||await digest(p.bytes,crypto)!==d.m.hashes[f.index]){clearTimeout(pending.timer);d.pending.delete(f.index);emit('fileError',{id:f.id,from,error:'Chunk failed verification'});pump(f.id);return;}
   check(f.id);if(downloads.get(f.id)!==d||d.pending.get(f.index)!==pending)return;await chunkStore.put(f.id,f.index,p.bytes);check(f.id);if(downloads.get(f.id)!==d||d.pending.get(f.index)!==pending)return;clearTimeout(pending.timer);d.pending.delete(f.index);d.have.add(f.index);emit('fileProgress',{id:f.id,received:d.have.size,total:d.m.total,from});pump(f.id);
  }
 }
 const off=core.onEvent(e=>{void handle(e).catch(error=>{if(e.payload?.file?.id)fail(e.payload.file.id,error);emit('fileError',{error:error.message});});});
 return {publish,share,request,cancel:id=>fail(id,new Error('File download cancelled')),getManifest:id=>publicManifest(check(id)),stop(target){for(const [id,d]of downloads)if(id===target||d.m.distributionId===target)fail(id,new Error('Distribution revoked'));for(const [key,row]of offers)if(row.m.id===target||row.m.distributionId===target)offers.delete(key);},close(){if(closed)return;closed=true;off();for(const cancel of [...activeSends])cancel();for(const id of downloads.keys())fail(id,new Error('File service closed'));offers.clear();sources.clear();manifests.clear();}};
}
