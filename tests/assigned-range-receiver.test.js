import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createFileDistribution,createMemoryChunkStore} from '../packages/phantom-core/src/files.js';
import {createAssignedRangeReceiver} from '../packages/phantom-core/src/assigned-range-receiver.js';
const authorized=async()=>true;

async function fixture(){
 const bytes=new Uint8Array(3072);for(let i=0;i<3;i++)bytes.fill(i+1,i*1024,(i+1)*1024);
 const source=createFileDistribution({core:{onEvent(){return()=>{};},command(){}}});
 const manifest=await source.publish(new Blob([bytes]),{name:'ranges',chunkSize:1024});source.close();
 return {manifest,bytes,chunk:i=>bytes.slice(i*1024,(i+1)*1024).buffer};
}
test('independent assigned ranges verify and commit out of order',async()=>{
 const f=await fixture(),store=createMemoryChunkStore(),receiver=await createAssignedRangeReceiver({manifest:f.manifest,chunkStore:store,verifyManifest:authorized});
 const a=receiver.claim([0,1]),b=receiver.claim([2]);await receiver.receive(b,2,f.chunk(2));await receiver.receive(a,1,f.chunk(1));await receiver.receive(a,0,f.chunk(0));
 assert.deepEqual(receiver.snapshot().have,[0,1,2]);for(let i=0;i<3;i++)assert.deepEqual(new Uint8Array(await store.get(f.manifest.id,i)),new Uint8Array(f.chunk(i)));await receiver.close();
});
test('retirement fences a delayed write before reassignment',async()=>{
 const f=await fixture(),memory=createMemoryChunkStore();let entered,release;const inside=new Promise(r=>entered=r),gate=new Promise(r=>release=r);
 const store={...memory,async put(...args){entered();await gate;await memory.put(...args);}};
 const receiver=await createAssignedRangeReceiver({manifest:f.manifest,chunkStore:store,verifyManifest:authorized});const old=receiver.claim([0]);
 const write=receiver.receive(old,0,f.chunk(0)),rejected=assert.rejects(write,/retired/);await inside;
 let done=false;const retiring=receiver.retire(old).then(()=>done=true);await Promise.resolve();assert.equal(done,false);assert.throws(()=>receiver.claim([0]),/unavailable/);
 release();await rejected;await retiring;const next=receiver.claim([0]);await receiver.receive(next,0,f.chunk(0));assert.deepEqual(receiver.snapshot().have,[0]);await receiver.close();
});
test('rejects corrupt bytes and overlapping assignments',async()=>{
 const f=await fixture(),receiver=await createAssignedRangeReceiver({manifest:f.manifest,chunkStore:createMemoryChunkStore(),verifyManifest:authorized}),lease=receiver.claim([0]);
 assert.throws(()=>receiver.claim([0]),/unavailable/);await assert.rejects(receiver.receive(lease,0,f.chunk(1)),/integrity/);assert.equal(receiver.snapshot().received,0);await receiver.retire(lease);await receiver.close();
});
test('resume retains only previously verified storage chunks',async()=>{
 const f=await fixture(),store=createMemoryChunkStore();await store.put(f.manifest.id,0,f.chunk(0));await store.put(f.manifest.id,1,f.chunk(2));
 const receiver=await createAssignedRangeReceiver({manifest:f.manifest,chunkStore:store,verifyManifest:authorized});assert.deepEqual(receiver.snapshot().have,[0]);assert.throws(()=>receiver.claim([0]),/unavailable/);const lease=receiver.claim([1,2]);await receiver.receive(lease,1,f.chunk(1));await receiver.receive(lease,2,f.chunk(2));assert.equal(receiver.snapshot().received,3);await receiver.close();
});
test('revocation prevents a previously claimed chunk from being committed',async()=>{
 const f=await fixture();let active=true;const receiver=await createAssignedRangeReceiver({manifest:f.manifest,chunkStore:createMemoryChunkStore(),verifyManifest:authorized,allowed:()=>active});const lease=receiver.claim([0]);active=false;await assert.rejects(receiver.receive(lease,0,f.chunk(0)),/unavailable/);assert.equal(receiver.snapshot().received,0);await receiver.close();
});
test('rejects tampered manifests and bounds queued writes',async()=>{
 const f=await fixture(),bad={...f.manifest,hashes:[...f.manifest.hashes]};bad.hashes[0]='0'.repeat(64);
 await assert.rejects(createAssignedRangeReceiver({manifest:bad,chunkStore:createMemoryChunkStore(),verifyManifest:authorized}),/Manifest integrity/);
 await assert.rejects(createAssignedRangeReceiver({manifest:f.manifest,chunkStore:createMemoryChunkStore(),verifyManifest:async()=>false}),/authorization/);
 const memory=createMemoryChunkStore();let entered,release;const inside=new Promise(r=>entered=r),gate=new Promise(r=>release=r);
 const receiver=await createAssignedRangeReceiver({manifest:f.manifest,chunkStore:{...memory,async put(...args){entered();await gate;await memory.put(...args);}},verifyManifest:authorized,maxPending:1,maxWrites:1});
 const lease=receiver.claim([0,1]),first=receiver.receive(lease,0,f.chunk(0));await inside;await assert.rejects(receiver.receive(lease,1,f.chunk(1)),/capacity/);release();await first;await receiver.close();
});
