/** One retained/in-flight block per service; callers receive independent chunk buffers. */
export function createSourceBlockReader({onRead=()=>{}}={}) {
 const blockBytes=4*1024*1024,waiting=[];
 let cached=null,active=null,closed=false,generation=0;
 const covers=(row,task)=>row?.source===task.source&&task.start>=row.start&&task.end<=row.end;
 function pump(){
  if(closed||active||!waiting.length)return;
  const first=waiting.shift();
  if(covers(cached,first)){first.resolve(cached.bytes.slice(first.start-cached.start,first.end-cached.start));pump();return;}
  cached=null;
  const chunks=Math.floor(blockBytes/first.chunkSize),start=Math.floor(first.index/chunks)*chunks*first.chunkSize;
  const row={source:first.source,start,end:Math.min(first.source.size,start+chunks*first.chunkSize),task:first,generation};active=row;
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
   if(!Number.isInteger(index)||index<0||!Number.isInteger(chunkSize)||chunkSize<1024||chunkSize>65536||index*chunkSize>=source.size)return Promise.reject(Error('Invalid source chunk'));
   return new Promise((resolve,reject)=>{waiting.push({source,index,chunkSize,start:index*chunkSize,end:Math.min(source.size,(index+1)*chunkSize),resolve,reject});pump();});
  },
  clear(){generation++;cached=null;},
  close(){closed=true;generation++;cached=null;const error=Error('Source reader closed');active?.task.reject(error);for(const task of waiting.splice(0))task.reject(error);}
 };
}
