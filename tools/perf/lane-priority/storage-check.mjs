// Browser-only diagnostic for the CI storage environment, not a transport benchmark.
import {createServer} from 'node:http';
import {createRequire} from 'node:module';
const require=createRequire(new URL('../../swarm/package.json',import.meta.url));
const {chromium}=require('playwright');
const server=createServer((req,res)=>{res.setHeader('Content-Type','text/html');res.end('<!doctype html><title>storage check</title>');});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const browser=await chromium.launch({headless:true,executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE,args:['--no-sandbox','--disable-dev-shm-usage']});
try{
 const context=await browser.newContext(),page=await context.newPage();await page.goto(`http://127.0.0.1:${server.address().port}/`);
 const result=await page.evaluate(async()=>{
  const size=1024*1024, blocks=1200, before=await navigator.storage.estimate(),bytes=new Uint8Array(size),name='phantom-storage-check-'+crypto.randomUUID();
  const request=indexedDB.open(name,1);request.onupgradeneeded=()=>request.result.createObjectStore('chunks');
  const db=await new Promise((resolve,reject)=>{request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error);});let written=0,problem=null;
  try{for(let index=0;index<blocks;index++){new DataView(bytes.buffer).setUint32(0,index);await new Promise((resolve,reject)=>{const tx=db.transaction('chunks','readwrite'),put=tx.objectStore('chunks').put(new Blob([bytes]),index);tx.oncomplete=resolve;tx.onabort=()=>reject(put.error||tx.error||Error('transaction aborted'));});written++;}}
  catch(error){problem={name:error?.name||'Error',message:error?.message||''};}
  const after=await navigator.storage.estimate();db.close();await new Promise(resolve=>{const r=indexedDB.deleteDatabase(name);r.onsuccess=r.onerror=r.onblocked=()=>resolve();});
  return {writtenMiB:written,targetMiB:blocks,before,after,problem};
 });
 console.log('STORAGE_RESULT',JSON.stringify(result));await context.close();
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
