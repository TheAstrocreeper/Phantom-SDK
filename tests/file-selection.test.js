import test from 'node:test';
import assert from 'node:assert/strict';
import {createFileDistribution,createMemoryChunkStore} from '../packages/phantom-core/src/files.js';
const peers=['1111111111111111','2222222222222222'];
const hash=async bytes=>Buffer.from(await crypto.subtle.digest('SHA-256',bytes)).toString('hex');
async function receiver(total,reply,{trusted=false}={}){
 const bytes=new ArrayBuffer(1024),chunkHash=await hash(bytes),hashes=Array(total).fill(chunkHash);
 const manifest={version:1,name:'selection',type:'application/octet-stream',size:total*1024,chunkSize:1024,total,hashRoot:await hash(new TextEncoder().encode(hashes.join(''))),distributionId:'selection-test'};
 manifest.id='sha256:'+await hash(new TextEncoder().encode(JSON.stringify([manifest.name,manifest.type,manifest.size,manifest.chunkSize,total,manifest.hashRoot,manifest.distributionId])));
 let event,offered;const ready=new Promise(resolve=>offered=resolve),requests=[];const memory=createMemoryChunkStore();
 const service=createFileDistribution({core:{onEvent(fn){event=fn;return()=>{};},command(type,payload){if(payload.sys==='PHANTOM_FILE_REQUEST'){requests.push(payload);reply(payload,packet=>queueMicrotask(()=>event(packet)),bytes);}}},chunkStore:trusted?{...memory,getVerifiedIndexes:()=>[]}:memory,emit(type){if(type==='fileOffer')offered();}});
 event({type:'INCOMING_MSG',payload:{from:peers[0],sys:'FILE_OFFER',file:{phantom:manifest}}});
 for(let offset=0;offset<total;offset+=64)event({type:'INCOMING_MSG',payload:{from:peers[0],sys:'PHANTOM_FILE_MANIFEST',file:{id:manifest.id,offset,hashes:hashes.slice(offset,offset+64)}}});
 await ready;return {service,manifest,requests};
}
const chunk=(request,bytes)=>({type:'FILE_BIN_CHUNK',payload:{from:request.to,file:request.file,bytes}});
test('an exhausted source does not repeatedly scan the other source remaining work',async t=>{
 const total=256,f=await receiver(total,(request,emit,bytes)=>{
  if(request.to===peers[0])emit({type:'INCOMING_MSG',payload:{from:request.to,sys:'PHANTOM_FILE_MISSING',file:request.file}});
  else emit(chunk(request,bytes));
 });
 const original=Set.prototype[Symbol.iterator],originalHas=Set.prototype.has;let visits=0;
 Set.prototype.has=function(value){if(Number.isInteger(value)&&value>=0&&value<total)visits++;return originalHas.call(this,value);};
 Set.prototype[Symbol.iterator]=function*(){for(const value of original.call(this)){if(Number.isInteger(value)&&value>=0&&value<total)visits++;yield value;}};
 try{
  const blob=await f.service.request(f.manifest.id,{peers});assert.equal(blob.size,total*1024);
  assert.ok(visits<5000,'fallback rescanned remaining indexes: '+visits);t.diagnostic('Numeric set operations across the full 256-piece fallback transfer: '+visits);
 }finally{Set.prototype[Symbol.iterator]=original;Set.prototype.has=originalHas;f.service.close();}
});
test('a late partial batch never scans the full manifest to fill request slots',async()=>{
 const total=8192,f=await receiver(total,(request,emit,bytes)=>emit(chunk(request,bytes)),{trusted:true});
 const original=Set.prototype.has;let probes=0;
 Set.prototype.has=function(value){if(this.size===32&&original.call(this,total-1))probes++;return original.call(this,value);};
 try{
  const wanted=Array.from({length:32},(_,i)=>total-32+i);
  const result=await f.service.request(f.manifest.id,{peers:[peers[0]],indexes:[...wanted].reverse(),requestWindow:32});
  assert.deepEqual(result.indexes,wanted);assert.deepEqual(f.requests.map(r=>r.file.index),wanted);
  assert.ok(probes<4096,'selection work grew with the full manifest: '+probes);
 }finally{Set.prototype.has=original;f.service.close();}
});
for(const mode of ['missing','corrupt','removed'])test('unfinished indexes return to admission after a '+mode+' source',async()=>{
 let release,first;const started=new Promise(resolve=>first=resolve);
 const f=await receiver(2,(request,emit,bytes)=>{
  if(request.to===peers[0]){
   if(mode==='missing')emit({type:'INCOMING_MSG',payload:{from:request.to,sys:'PHANTOM_FILE_MISSING',file:request.file}});
   else if(mode==='corrupt')emit(chunk(request,new Uint8Array(1024).fill(9).buffer));
   else{release=()=>f.service.dropPeer(peers[0]);first();}
  }else emit(chunk(request,bytes));
 });
 try{
  const result=f.service.request(f.manifest.id,{peers,requestWindow:1});
  if(mode==='removed'){await started;release();}
  const blob=await result;assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),new Uint8Array(2048));
  assert.ok(f.requests.some(r=>r.to===peers[1]&&r.file.index===0));
 }finally{f.service.close();}
});
