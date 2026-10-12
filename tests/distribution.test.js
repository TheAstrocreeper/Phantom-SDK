import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminIdentity,importAdminIdentity,createControlStore} from '../packages/phantom-core/src/index.js';
import {createAdminLifecycle} from '../packages/phantom-core/src/admin-lifecycle.js';
import {createInvite,parseInvite} from '../packages/phantom-core/src/invite.js';
import {createFileDistribution,createMemoryChunkStore} from '../packages/phantom-core/src/files.js';
function storage(){const m=new Map();return {getItem:k=>m.get(k)??null,setItem:(k,v)=>m.set(k,v)};}
test('admin seed survives reload and cannot be swapped for another key',async()=>{
 const a=await createAdminIdentity(),seed=await a.exportSeed(),restored=await importAdminIdentity(seed);
 const b=await createAdminIdentity();await assert.rejects(importAdminIdentity({...seed,publicKey:b.publicKey}),/match/);
 const record=await restored.sign({networkId:'n',action:'network.retire',target:'n',operationId:'op'});
 const l=await createAdminLifecycle({networkId:'n',adminPublicKey:a.publicKey,store:createControlStore(storage())});assert.equal(await l.apply(record),true);assert.equal(l.retired,true);
 const invite=createInvite({relays:['wss://example.test'],adminPublicKey:a.publicKey});assert.equal(parseInvite(invite).adminPublicKey,a.publicKey);assert.ok(invite.startsWith('phantom://v3/'));assert.ok(!invite.includes(seed.privateKey));
});
test('revoke and retirement persist, reject forgery, and do not resend duplicate proofs',async()=>{
 const admin=await createAdminIdentity(),store=createControlStore(storage()),sent=[];
 const l=await createAdminLifecycle({networkId:'n',adminPublicKey:admin.publicKey,adminIdentity:admin,store,send:r=>sent.push(r)});
 const r=await l.issue('content.revoke','patch-7');assert.equal(l.isRevoked('patch-7'),true);assert.equal(await l.apply(r),true);assert.equal(sent.length,1);
 assert.equal(await l.apply({...r,target:'patch-8'}),false);assert.equal(l.isRevoked('patch-8'),false);
 await l.issue('network.retire','n');const restored=await createAdminLifecycle({networkId:'n',adminPublicKey:admin.publicKey,store});assert.equal(restored.retired,true);assert.equal(restored.isRevoked('patch-7'),true);
 const failed=await createAdminLifecycle({networkId:'x',adminPublicKey:admin.publicKey,adminIdentity:admin,store:{load:async()=>[],append:async()=>{throw Error('disk');}}});await assert.rejects(failed.issue('network.retire','x'),/disk/);assert.equal(failed.retired,false);
});
function bus(){const cores=new Map(),counts=new Map();let corrupt=null;return {counts,setCorrupt:fn=>corrupt=fn,make(id){const listeners=new Set();const core={onEvent:fn=>{listeners.add(fn);return()=>listeners.delete(fn);},emit:e=>{for(const fn of listeners)fn(e);},command(type,p){const target=cores.get(p.to);if(!target)return;if(type==='SEND_DM')queueMicrotask(()=>target.emit({type:'INCOMING_MSG',payload:{from:id,sys:p.sys,file:structuredClone(p.file)}}));else if(type==='SEND_FILE_BIN_CHUNK'){counts.set(id,(counts.get(id)||0)+1);let bytes=p.bytes.slice(0);if(corrupt)bytes=corrupt(id,p.file.index,bytes);queueMicrotask(()=>target.emit({type:'FILE_BIN_CHUNK',payload:{from:id,file:p.file,bytes}}));queueMicrotask(()=>core.emit({type:'FILE_BIN_CHUNK_SENT',payload:{to:p.to,file:p.file,ok:true}}));}}};cores.set(id,core);return core;}};}
const ids=['a','b','c'].map(x=>x.repeat(16));
async function fixture(){const b=bus(),events=[[],[],[]],revoked=new Set(),services=ids.map((id,i)=>createFileDistribution({core:b.make(id),emit:(t,p)=>events[i].push({t,p}),allowed:(id,target)=>!revoked.has(id)&&!revoked.has(target),requestTimeoutMs:40}));const file=new Blob([Uint8Array.from({length:16384},(_,i)=>i%251)]);const m=await services[0].publish(file,{chunkSize:1024,distributionId:'patch-1'});return {b,events,revoked,services,file,m};}
async function offered(s,target,id,events){await s.share(target,id);for(let i=0;i<100&&!events.some(e=>e.t==='fileOffer');i++)await new Promise(r=>setTimeout(r,2));assert.ok(events.some(e=>e.t==='fileOffer'));}
test('partial holders serve verified pieces to a multi-source download',async()=>{
 const {b,events,services:s,file,m}=await fixture();try{
 await offered(s[0],ids[1],m.id,events[1]);await s[1].request(m.id,{peers:[ids[0]],indexes:[0,2,4,6,8,10,12,14]});
 await offered(s[1],ids[2],m.id,events[2]);const before=b.counts.get(ids[1])??0;const blob=await s[2].request(m.id,{peers:[ids[1],ids[0]]});assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),new Uint8Array(await file.arrayBuffer()));assert.ok((b.counts.get(ids[1])??0)>before);assert.equal(events[2].filter(e=>e.t==='fileProgress').length,16);
 }finally{s.forEach(x=>x.close());}
});
test('corrupt chunks are rejected and a good source supplies the remainder',async()=>{
 const {b,events,services:s,file,m}=await fixture();try{await offered(s[0],ids[1],m.id,events[1]);await s[1].request(m.id,{peers:[ids[0]]});await offered(s[0],ids[2],m.id,events[2]);b.setCorrupt((from,index,bytes)=>{if(from===ids[1])new Uint8Array(bytes)[0]^=255;return bytes;});const blob=await s[2].request(m.id,{peers:[ids[1],ids[0]]});assert.equal(blob.size,file.size);assert.ok(events[2].some(e=>e.t==='fileError'&&e.p.error.includes('verification')));}finally{s.forEach(x=>x.close());}
});
test('revocation cancels transfers and prevents subsequent serving',async()=>{
 const {events,services:s,m,revoked}=await fixture();try{await offered(s[0],ids[2],m.id,events[2]);const pending=s[2].request(m.id,{peers:['d'.repeat(16)]});const rejected=assert.rejects(pending,/revoked/);revoked.add('patch-1');s[2].stop('patch-1');await rejected;await assert.rejects(s[0].share(ids[1],m.id),/revoked/);}finally{s.forEach(x=>x.close());}
});
test('cancelled download resumes from verified cached chunks; empty files roundtrip',async()=>{
 const {events,services:s,m,file}=await fixture();try{await offered(s[0],ids[2],m.id,events[2]);
 const original=events[2].push.bind(events[2]);let cancel=true;events[2].push=(e)=>{const n=original(e);if(cancel&&e.t==='fileProgress'&&e.p.received===4)s[2].cancel(m.id);return n;};
 await assert.rejects(s[2].request(m.id,{peers:[ids[0]]}),/cancelled/);cancel=false;events[2].length=0;const blob=await s[2].request(m.id,{peers:[ids[0]]});assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),new Uint8Array(await file.arrayBuffer()));assert.equal(events[2].filter(e=>e.t==='fileProgress').length,12);
 const empty=await s[0].publish(new Blob([]));events[1].length=0;await offered(s[0],ids[1],empty.id,events[1]);assert.equal((await s[1].request(empty.id,{peers:[ids[0]]})).size,0);
 }finally{s.forEach(x=>x.close());}
});
