const fields = new Set('state reason nextAction role admitted type id from to peer peerId jobId index received total processed size bytes elapsedMs readMs hashMs error message code visibility online connected ready outgoing status generation batch fileEvent connectionState iceConnectionState iceGatheringState signalingState source receiver retryAfterMs reportedBy phase metrics readMs hashMs queueMs bufferWaitMs submitMs requestToReceiveMs writeMs writeQueueMs cache_readMs cache_hashMs count sum max p50 p95 sampleCount'.split(' '));
const text = v => String(v).replace(/sha256:([0-9a-f]{64})/gi,(_,hash)=>'asset:'+hash.slice(0,12)).replace(/https?:\/\/\S+|wss?:\/\/\S+|P1(?:[-\s]?[0-9A-Z]){28,}|[0-9a-f]{64,}/gi,'[redacted]').slice(0,300);
function clean(v,depth=0){
 if(v==null||typeof v==='boolean'||typeof v==='number')return v;
 if(typeof v==='string')return text(v);
 if(depth>=3||typeof v!=='object')return undefined;
 const out={};for(const k of fields)if(Object.hasOwn(v,k))out[k]=clean(v[k],depth+1);return out;
}
export function createDiagnostics({storage,now=Date.now,limit=1200}={}){
 const key='phantom-share-diagnostics-v1';let rows=[],dropped=0,storageError=false;const sampled=new Map();
 try{const saved=JSON.parse(storage?.getItem(key)||'null');if(Array.isArray(saved?.rows)){rows=saved.rows.slice(-limit).map(r=>({at:r.at,type:text(r.type),data:clean(r.data)}));dropped=saved.dropped||0;}}catch{storageError=true;}
 const record=(type,data={})=>{
 const at=now();
 if(['fileProgress','distributionProgress','filePreparationProgress','distributionFileEvent','transfer-state'].includes(type)){
  const sampleKey=type+':'+(data.id||'')+':'+(data.status||data.fileEvent||'');
  if(at-(sampled.get(sampleKey)??-Infinity)<1000)return;
  if(sampled.size>=128)sampled.clear();sampled.set(sampleKey,at);
 }
 rows.push({at:now(),type:text(type),data:clean(data)});if(rows.length>limit){dropped+=rows.length-limit;rows.splice(0,rows.length-limit);}};
 const flush=()=>{try{storage?.setItem(key,JSON.stringify({rows,dropped}));}catch{storageError=true;}};
 const report=()=>({schema:1,build:'share-pipeline-v6',exportedAt:now(),dropped,storageError,rows:structuredClone(rows)});
 record('page-start');return {record,flush,report};
}
