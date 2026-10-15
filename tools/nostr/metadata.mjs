/** NIP-11 declarations, not independently verified limits. No linked URLs fetched. */
export async function relayMetadata(relay,{fetchImpl=fetch,timeoutMs=5000,maxBytes=65536}={}) {
 const url=new URL(relay);url.protocol='https:';const start=Date.now();
 try {
  const response=await fetchImpl(url,{headers:{Accept:'application/nostr+json'},redirect:'error',signal:AbortSignal.timeout(timeoutMs)});
  if(!response.ok){await response.body?.cancel();return {available:false,status:response.status,elapsedMs:Date.now()-start};}
  const reader=response.body.getReader();let bytes=0;const chunks=[];
  try{while(true){const {done,value}=await reader.read();if(done)break;bytes+=value.byteLength;if(bytes>maxBytes)throw Error('metadata_too_large');chunks.push(value);}}finally{await reader.cancel();}
  const document=JSON.parse(Buffer.concat(chunks).toString('utf8'));
  if(!document||typeof document!=='object'||Array.isArray(document))throw Error('invalid_metadata');
  return {available:true,elapsedMs:Date.now()-start,bytes,document};
 }catch(error){return {available:false,elapsedMs:Date.now()-start,error:error.message};}
}
