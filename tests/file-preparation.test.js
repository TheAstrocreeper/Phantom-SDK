import test from 'node:test';
import assert from 'node:assert/strict';
import {createFileDistribution} from '../packages/phantom-core/src/files.js';
const core={onEvent(){return()=>{};},command(){}};
test('block source cache is scoped to one publication and cannot warm a fresh transfer identity',async()=>{
 const file=new Blob([new Uint8Array(65536)]),reads=[];
 const service=createFileDistribution({core,emit:(type,p)=>{if(type==='fileSourceRead')reads.push(p);}});
 try{
  await assert.rejects(service.publish(file,{sourceReadBlockBytes:1}),/Source read block/);
  const a=await service.publish(file,{chunkSize:65536,distributionId:'one',sourceReadBlockBytes:4*1024*1024});
  await service.getFile(a.id);await service.getFile(a.id);assert.equal(reads.length,1);
  const b=await service.publish(file,{chunkSize:65536,distributionId:'two',sourceReadBlockBytes:4*1024*1024});
  await service.getFile(b.id);assert.notEqual(a.id,b.id);assert.equal(reads.length,2);assert.equal(reads[1].bytes,65536);
 }finally{service.close();}
});
test('batched preparation preserves every chunk hash and bounds reads and hashing concurrency',async()=>{
 const size=4*1024*1024+17003,bytes=Uint8Array.from({length:size},(_,i)=>i%251),blob=new Blob([bytes]);
 let reads=0,active=0,peak=0;const progress=[];
 const file={size:blob.size,slice(start,end){reads++;assert.ok(end-start<=4*1024*1024);return blob.slice(start,end);}};
 const cryptoWrapper={subtle:{async digest(...args){active++;peak=Math.max(peak,active);try{return await crypto.subtle.digest(...args);}finally{active--;}}}};
 const service=createFileDistribution({core,crypto:cryptoWrapper,emit:(type,p)=>{if(type==='filePreparationProgress')progress.push(p);}});
 try{const m=await service.publish(file,{chunkSize:65536});assert.equal(reads,2);assert.ok(peak<=8&&peak>1);assert.equal(progress.at(-1).processed,m.total);
 for(let i=0;i<m.total;i++){const expected=Buffer.from(await crypto.subtle.digest('SHA-256',bytes.subarray(i*65536,(i+1)*65536))).toString('hex');assert.equal(m.hashes[i],expected);}
 }finally{service.close();}
});
test('closing during a batch prevents publication',async()=>{
 let service;const blob=new Blob([new Uint8Array(65536)]);service=createFileDistribution({core,crypto:{subtle:{async digest(...args){service.close();return crypto.subtle.digest(...args);}}}});
 await assert.rejects(service.publish(blob,{chunkSize:65536}),/unavailable/);
});
test('revocation during concurrent preparation prevents publication',async()=>{
 let permitted=true,published=false;const blob=new Blob([new Uint8Array(65536)]);
 const service=createFileDistribution({core,allowed:()=>permitted,emit:type=>{if(type==='filePublished')published=true;},crypto:{subtle:{async digest(...args){permitted=false;return crypto.subtle.digest(...args);}}}});
 try{await assert.rejects(service.publish(blob,{chunkSize:1024}),/unavailable/);assert.equal(published,false);}finally{service.close();}
});
