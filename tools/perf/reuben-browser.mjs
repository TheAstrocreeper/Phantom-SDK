/** Real browser/native RTC verification. Loopback, not an internet-capacity claim. */
import {createServer} from 'node:http';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {resolve,extname,sep} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const require=createRequire(new URL('../swarm/package.json',import.meta.url));
const {chromium}=process.env.PHANTOM_PLAYWRIGHT_ROOT?require(process.env.PHANTOM_PLAYWRIGHT_ROOT):require('playwright');
const root=resolve(fileURLToPath(new URL('../../',import.meta.url))),output=process.env.PHANTOM_REUBEN_BROWSER_OUTPUT??resolve(root,'tools/perf/reuben/browser-results.json');
const bytes=Number(process.env.PHANTOM_REUBEN_BROWSER_BYTES??(32*1024*1024+17));
const server=createServer(async(req,res)=>{try{const path=decodeURIComponent(new URL(req.url,'http://localhost').pathname),target=resolve(root,'.'+path);if(!target.startsWith(root+sep)||!/^\/(packages\/|tools\/perf\/reuben\/)/.test(path)||!['.js','.mjs','.html'].includes(extname(target))){res.writeHead(403).end();return;}res.setHeader('Content-Type',extname(target)==='.html'?'text/html':'text/javascript');res.setHeader('Cache-Control','no-store');res.end(await readFile(target));}catch{res.writeHead(404).end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));let browser;
const rows=[],errors=[];
try{
 browser=await chromium.launch({headless:true,...(process.env.PHANTOM_CHROMIUM_EXECUTABLE?{executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE}:{}),args:['--enable-precise-memory-info']});
 for(const [index,engine]of ['current','reuben','reuben','current'].entries()){
  const contexts=await Promise.all([0,1].map(()=>browser.newContext())),pages=await Promise.all(contexts.map(c=>c.newPage()));
  try{
   for(let i=0;i<2;i++){pages[i].on('pageerror',e=>errors.push(e.message));await pages[i].exposeFunction('bridge',message=>pages[1-i].evaluate(async message=>{try{await window.endpoint.handle(message);}catch(error){window.failure=error.message;}},message));}
   await Promise.all(pages.map(p=>p.goto(`http://127.0.0.1:${server.address().port}/tools/perf/reuben/browser.html`)));
   await Promise.all(pages.map(p=>p.waitForFunction(()=>window.ready)));
   const adminPublicKey=await pages[0].evaluate(()=>window.createAdmin()),ids=['1111111111111111','2222222222222222'];
   await Promise.all(pages.map((p,i)=>p.evaluate(options=>window.configure(options),{engine,role:i?'receiver':'sender',selfId:ids[i],peerId:ids[1-i],networkId:`browser-reuben-${index}`,adminPublicKey,bytes})));
   await pages[0].evaluate(()=>{void window.endpoint.start().catch(error=>window.failure=error.message);});
   await pages[1].waitForFunction(()=>window.result||window.failure,null,{timeout:120000});
   const result=await pages[1].evaluate(()=>({result:window.result,error:window.failure}));assert.equal(result.error,undefined);
   const sender=await pages[0].evaluate(async()=>{await window.endpoint.sample();return {report:window.endpoint.report(),error:window.failure};});assert.equal(sender.error,undefined);assert.equal(result.result.verified,true);assert.ok(result.result.peakWrites<=4);
   rows.push({index,repeat:Math.floor(index/2),engine,receiver:result.result,sender:sender.report});console.log(JSON.stringify({engine,index,elapsedMs:result.result.elapsedMs,verified:true}));
  }finally{for(const p of pages)await p.evaluate(()=>window.endpoint?.close()).catch(()=>{});await Promise.all(contexts.map(c=>c.close()));}
 }
 assert.deepEqual(errors,[]);
 const mean=engine=>{const matches=rows.filter(r=>r.engine===engine);return matches.reduce((n,r)=>n+r.receiver.elapsedMs,0)/matches.length;};
 const currentMs=mean('current'),reubenMs=mean('reuben');
 await mkdir(resolve(output,'..'),{recursive:true});await writeFile(output,JSON.stringify({schema:1,scope:'Real Chrome, separate browser contexts, native WebRTC over loopback, memory storage. No imposed RTT, bandwidth or loss; not a 5G result.',browser:browser.version(),bytes,summary:{currentMs,reubenMs,elapsedReductionPercent:(1-reubenMs/currentMs)*100},rows},null,2)+'\n');
 console.log(JSON.stringify({output,currentMs,reubenMs}));
}catch(error){await mkdir(resolve(output,'..'),{recursive:true});await writeFile(output,JSON.stringify({failed:true,error:error.message,rows,errors},null,2));throw error;}finally{await browser?.close();await new Promise(r=>server.close(r));}
