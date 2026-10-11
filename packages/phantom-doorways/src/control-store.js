/** Optional durable admin record anchor. Used on entry/actions, never for file bytes or elections. */
export function createHttpControlStore({url,fetch=globalThis.fetch,timeoutMs=10000}={}) {
 const base=new URL(url);if(base.protocol!=='https:'||base.username||base.password||base.hash)throw new TypeError('Control store requires credential-free HTTPS');
 if(!Number.isFinite(timeoutMs)||timeoutMs<=0)throw new TypeError('Positive control store timeout required');
 async function call(id,record){const endpoint=new URL(base);endpoint.searchParams.set('v',id);const response=await fetch(endpoint.href,{method:record?'POST':'GET',cache:'no-store',headers:record?{'Content-Type':'application/json'}:undefined,body:record?JSON.stringify(record):undefined,signal:AbortSignal.timeout(timeoutMs)});if(!response.ok)throw new Error('Control store unavailable: '+response.status);return response.json();}
 return {load:async id=>{const records=await call(id);if(!Array.isArray(records)||records.length>10000)throw new Error('Invalid control store response');return records;},append:async(id,record)=>{await call(id,record);}};
}
