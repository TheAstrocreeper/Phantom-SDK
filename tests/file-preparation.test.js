import test from 'node:test';
import assert from 'node:assert/strict';
import {createFileDistribution} from '../packages/phantom-core/src/files.js';
const core={onEvent(){return()=>{};},command(){}};
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
