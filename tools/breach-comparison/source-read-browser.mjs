/** Native browser File reads without WebRTC: isolate read granularity, retain chunk hashes. */
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
const {chromium}=await import(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES+'/playwright/index.mjs':'playwright');
const temp=await mkdtemp(path.join(tmpdir(),'phantom-source-read-')),file=path.join(temp,'source.bin');
await writeFile(file,Uint8Array.from({length:32*1024*1024},(_,i)=>i%251));
const server=http.createServer((req,res)=>{res.setHeader('Content-Type','text/html');res.end('<!doctype html><input id="file" type="file">');});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const browser=await chromium.launch({headless:true,...(process.env.PHANTOM_CHROMIUM_EXECUTABLE?{executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE}:{}),args:['--no-sandbox','--disable-dev-shm-usage']});
try{
 const page=await browser.newPage();await page.goto('http://127.0.0.1:'+server.address().port);await page.locator('#file').setInputFiles(file);
 let expected;
 for(const mode of ['chunk-parallel-8','block-4MiB','chunk-parallel-8','block-4MiB']){
  const result=await page.evaluate(async mode=>{
   const file=document.querySelector('#file').files[0],chunk=65536,total=Math.ceil(file.size/chunk),hashes=new Array(total),times=[];
   let calls=0,active=0,peak=0,maxReadBytes=0,hashMs=0;
   const hex=b=>Array.from(new Uint8Array(b),v=>v.toString(16).padStart(2,'0')).join('');
   async function read(start,end){calls++;maxReadBytes=Math.max(maxReadBytes,end-start);active++;peak=Math.max(peak,active);const at=performance.now();try{return await file.slice(start,end).arrayBuffer();}finally{times.push(performance.now()-at);active--;}}
   async function hash(index,bytes){const at=performance.now();hashes[index]=hex(await crypto.subtle.digest('SHA-256',bytes));hashMs+=performance.now()-at;}
   const started=performance.now();
   if(mode==='chunk-parallel-8'){
    let next=0;await Promise.all(Array.from({length:8},async()=>{while(next<total){const i=next++;await hash(i,await read(i*chunk,Math.min(file.size,(i+1)*chunk)));}}));
   }else{
    for(let base=0;base<total;base+=64){const bytes=await read(base*chunk,Math.min(file.size,(base+64)*chunk));
     for(let offset=0;offset<Math.min(64,total-base);offset+=8)await Promise.all(Array.from({length:Math.min(8,total-base-offset)},(_,j)=>{const start=(offset+j)*chunk;return hash(base+offset+j,new Uint8Array(bytes,start,Math.min(chunk,bytes.byteLength-start)));}));
    }
   }
   const elapsedMs=performance.now()-started,sorted=[...times].sort((a,b)=>a-b),pick=q=>sorted[Math.ceil(q*sorted.length)-1];
   return {mode,bytes:file.size,chunks:total,readCalls:calls,maxReadBytes,peakConcurrentReads:peak,elapsedMs,readSumMs:times.reduce((a,b)=>a+b,0),readP50Ms:pick(.5),readP95Ms:pick(.95),hashSumMs:hashMs,hashRoot:hex(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(hashes.join(''))))};
  },mode);
  expected??=result.hashRoot;assert.equal(result.hashRoot,expected);assert.equal(result.readCalls,mode==='block-4MiB'?8:512);assert.ok(result.maxReadBytes<=4*1024*1024);
  console.log(JSON.stringify({...result,verified:true,scope:'Native browser File reads on this runtime only; no WebRTC, phone or physical-device throughput evidence. Overlapping read/hash sums are not wall-time attribution.'}));
 }
}finally{await browser.close();await new Promise(r=>server.close(r));await rm(temp,{recursive:true,force:true});}
