/** One retained/in-flight block per service; callers receive independent chunk buffers. */
export function createSourceBlockReader({onRead=()=>{}}={}) {
 const blockBytes=4*1024*1024,waiting=[];
 let cached=null,active=null,closed=false,scheduled=false,generation=0;
 function schedule(){if(closed||active||scheduled)return;scheduled=true;queueMicrotask(()=>{scheduled=false;pump();});}
 const covers=(row,task)=>row?.source===task.source&&task.start>=row.start&&task.end<=row.end;
 function pump(){
  if(closed||active||!waiting.length)return;
  const first=waiting.shift();
  if(covers(cached,first)){first.resolve(cached.bytes.slice(first.start-cached.start,first.end-cached.start));pump();return;}
  cached=null;
  const chunks=Math.floor(blockBytes/first.chunkSize),start=Math.floor(first.index/chunks)*chunks*first.chunkSize;
  const block={source:first.source,start,end:Math.min(first.source.size,start+chunks*first.chunkSize)};
  // Interleaved ranges do not justify reading a full block for a few chunks.
  // Batch the queued demand inside the first block; retain full read-ahead for
  // a single local range. Both paths keep one read/cache within the same 4 MiB.
  const mixed=waiting.some(task=>!covers(block,task));
  const local=mixed?[first,...waiting.filter(task=>covers(block,task))]:null;
  const row={source:first.source,start:mixed?Math.min(...local.map(t=>t.start)):start,
   end:mixed?Math.max(...local.map(t=>t.end)):block.end,task:first,generation};active=row;
  const at=performance.now();
  void Promise.resolve().then(()=>row.source.slice(row.start,row.end).arrayBuffer()).then(bytes=>{
   if(closed)return;
   if(!(bytes instanceof ArrayBuffer)||bytes.byteLength!==row.end-row.start)throw Error('Source block read returned invalid bytes');
   try{onRead({bytes:bytes.byteLength,readMs:performance.now()-at});}catch{}
   if(closed)return;
   const result={source:row.source,start:row.start,end:row.end,bytes};if(row.generation===generation)cached=result;
   first.resolve(bytes.slice(first.start-row.start,first.end-row.start));
   for(let i=0;i<waiting.length;)if(covers(result,waiting[i])){const task=waiting.splice(i,1)[0];task.resolve(bytes.slice(task.start-row.start,task.end-row.start));}else i++;
  }).catch(error=>{first.reject(error);
   for(let i=0;i<waiting.length;)if(covers(row,waiting[i]))waiting.splice(i,1)[0].reject(error);else i++;
  }).finally(()=>{active=null;pump();});
 }
 return {
  read(source,index,chunkSize){
   if(closed)return Promise.reject(Error('Source reader closed'));
   if(!Number.isInteger(index)||index<0||!Number.isInteger(chunkSize)||chunkSize<1024||chunkSize>262144||index*chunkSize>=source.size)return Promise.reject(Error('Invalid source chunk'));
   return new Promise((resolve,reject)=>{waiting.push({source,index,chunkSize,start:index*chunkSize,end:Math.min(source.size,(index+1)*chunkSize),resolve,reject});schedule();});
  },
  clear(){generation++;cached=null;},
  close(){closed=true;generation++;cached=null;const error=Error('Source reader closed');active?.task.reject(error);for(const task of waiting.splice(0))task.reject(error);}
 };
}
