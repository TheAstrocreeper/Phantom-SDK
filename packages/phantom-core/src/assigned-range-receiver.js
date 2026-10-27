// Internal building block for a future assigned-lane transport. The caller must
// authenticate the manifest and peer using the distribution authorization path.
// This module is not yet connected to network-distribution or exported as API.
const hex=bytes=>Array.from(new Uint8Array(bytes),b=>b.toString(16).padStart(2,'0')).join('');
const descriptor=m=>[m.name,m.type,m.size,m.chunkSize,m.total,m.hashRoot,m.distributionId];

export async function createAssignedRangeReceiver({manifest,chunkStore,verifyManifest,crypto=globalThis.crypto,allowed=()=>true,maxPending=96,maxWrites=4}={}){
 const m=structuredClone(manifest);
 if(!m||typeof m.id!=='string'||!/^sha256:[0-9a-f]{64}$/.test(m.id)||!Number.isSafeInteger(m.size)||m.size<0||!Number.isInteger(m.chunkSize)||m.chunkSize<1024||m.chunkSize>65536||m.total!==Math.ceil(m.size/m.chunkSize)||!Array.isArray(m.hashes)||m.hashes.length!==m.total||m.hashes.some(h=>! /^[0-9a-f]{64}$/.test(h))||typeof m.name!=='string'||typeof m.type!=='string'||typeof m.distributionId!=='string'||! /^[0-9a-f]{64}$/.test(m.hashRoot)||typeof verifyManifest!=='function'||!chunkStore||typeof chunkStore.get!=='function'||typeof chunkStore.put!=='function'||!Number.isInteger(maxWrites)||maxWrites<1||!Number.isInteger(maxPending)||maxPending<maxWrites)throw TypeError('Invalid assigned range receiver');
 const check=()=>{if(closed)throw Error('Assigned receiver closed');if(!allowed(m.id,m.distributionId))throw Error('Distribution unavailable');};
 const digest=async bytes=>hex(await crypto.subtle.digest('SHA-256',bytes));
 let closed=false;check();
 if(await digest(new TextEncoder().encode(m.hashes.join('')))!==m.hashRoot||'sha256:'+await digest(new TextEncoder().encode(JSON.stringify(descriptor(m))))!==m.id)throw Error('Manifest integrity failed');
 check();if(await verifyManifest(structuredClone(m))!==true)throw Error('Manifest authorization failed');check();await chunkStore.begin?.(m.id,structuredClone(m));check();
 const have=new Set(),all=Array.from({length:m.total},(_,i)=>i),metadata={...m};delete metadata.hashes;
 if(typeof chunkStore.getVerifiedIndexes==='function'){
  const indexes=await chunkStore.getVerifiedIndexes(m.id,metadata,all);check();
  if(!Array.isArray(indexes)||indexes.some(i=>!Number.isInteger(i)||i<0||i>=m.total))throw Error('Invalid verified storage inventory');
  for(const i of indexes)have.add(i);
 }else{
  for(const i of all){check();const bytes=await chunkStore.get(m.id,i);check();if(bytes instanceof ArrayBuffer&&bytes.byteLength===Math.min(m.chunkSize,m.size-i*m.chunkSize)&&await digest(bytes)===m.hashes[i])have.add(i);}
 }
 const owners=new Map(),leases=new Set(),queued=[];let activeWrites=0,pendingCount=0,generation=0;
 function finishRetirement(lease){if(lease.live||lease.pending.size)return;for(const i of lease.indexes)if(owners.get(i)===lease)owners.delete(i);leases.delete(lease);lease.retiredResolve?.();}
 function assertLease(lease,index){check();if(!leases.has(lease)||!lease.live||!lease.indexes.has(index)||owners.get(index)!==lease)throw Error('Range assignment retired');}
 function drain(){while(activeWrites<maxWrites&&queued.length){const task=queued.shift(),{lease,index,bytes}=task;activeWrites++;
  Promise.resolve().then(async()=>{assertLease(lease,index);if(await digest(bytes)!==m.hashes[index])throw Error('Chunk integrity failed');assertLease(lease,index);await chunkStore.put(m.id,index,bytes);assertLease(lease,index);have.add(index);return {index,received:have.size};})
   .then(task.resolve,task.reject).finally(()=>{activeWrites--;pendingCount--;lease.pending.delete(index);finishRetirement(lease);drain();});
 }}
 function claim(indexes){check();if(!Array.isArray(indexes)||!indexes.length||new Set(indexes).size!==indexes.length||indexes.some(i=>!Number.isInteger(i)||i<0||i>=m.total||owners.has(i)||have.has(i)))throw Error('Range unavailable');
  const lease={generation:++generation,live:true,indexes:new Set(indexes),pending:new Set(),retired:null,retiredResolve:null};leases.add(lease);for(const i of indexes)owners.set(i,lease);return lease;
 }
 function receive(lease,index,input){try{assertLease(lease,index);if(!(input instanceof ArrayBuffer)||input.byteLength!==Math.min(m.chunkSize,m.size-index*m.chunkSize))throw Error('Invalid chunk bounds');if(lease.pending.has(index)||have.has(index))throw Error('Chunk already received');if(pendingCount>=maxPending)throw Error('Receive capacity exhausted');
  const bytes=input.slice(0);lease.pending.add(index);pendingCount++;return new Promise((resolve,reject)=>{queued.push({lease,index,bytes,resolve,reject});drain();});
 }catch(error){return Promise.reject(error);}}
 function retire(lease){if(!leases.has(lease))return Promise.resolve();if(lease.retired)return lease.retired;lease.live=false;lease.retired=new Promise(resolve=>{lease.retiredResolve=resolve;});finishRetirement(lease);return lease.retired;}
 function close(){if(closed)return Promise.resolve();closed=true;return Promise.all([...leases].map(retire)).then(()=>{});}
 return {claim,receive,retire,close,snapshot:()=>({received:have.size,total:m.total,have:[...have].sort((a,b)=>a-b),activeWrites,pending:pendingCount})};
}
