import {build} from 'esbuild';
import {readFile,mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import http from 'node:http';
import assert from 'node:assert/strict';
const {chromium}=await import(process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES?process.env.CODEX_PRIMARY_RUNTIME_NODE_MODULES+'/playwright/index.mjs':'playwright');
const root=new URL('../src/',import.meta.url),temp=await mkdtemp(path.join(tmpdir(),'breach-window-browser-'));
const fixture=`
const peer='b'.repeat(16),handlers=new Map();let serial=0,release;
const fire=(type,data)=>{for(const fn of handlers.get(type)||[])fn(data);};
const f=window.fixture={created:0,published:[],requested:[],sent:[]};
const net={invite:'fixture',getState:()=>({peerId:'a'.repeat(16),state:'admitted',admitted:true,role:'syscon1'}),snapshot:()=>({chatPairs:[{hKey:peer,dcState:'open'}]}),getPeers:()=>[],on(type,fn){if(!handlers.has(type))handlers.set(type,new Set());handlers.get(type).add(fn);return()=>handlers.get(type).delete(fn);},invitePeer(){},acceptPeer(){},close(){},send(to,text){f.sent.push(JSON.parse(text));},
async publishFile(file,options){const m={id:'sha256:'+String(++serial).padStart(64,'0'),name:'test.bin',size:file.size,chunkSize:65536,total:Math.ceil(file.size/65536)};f.published.push({m,options});return m;},async shareFile(){},
requestFile(id,options){f.requested.push({id,options});return new Promise(r=>release=()=>{const bytes=new Uint8Array([1,2,3,4]);fire('fileProgress',{id,index:0,received:1,total:1,bytes:4,verifiedBytes:4,elapsedMs:1,verificationMs:.1,inFlight:0});r(new Blob([bytes]));});}};
f.finishSend=()=>{const p=f.sent.findLast(m=>m.type==='file-hash');fire('message',{from:peer,text:JSON.stringify({app:'phantom-breach',version:1,type:'complete',id:p.id,hash:p.hash})});};
f.incoming=async()=>{const id='sha256:'+String(++serial).padStart(64,'0'),bytes=new Uint8Array([1,2,3,4]);const hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');fire('fileOffer',{from:peer,manifest:{id,name:'test.bin',size:4,chunkSize:65536,total:1}});fire('message',{from:peer,text:JSON.stringify({app:'phantom-breach',version:1,type:'file-hash',id,hash})});};
f.finishReceive=()=>release();
export function createBrowserPhantom(){return {async create(){f.created++;return net;},async join(){f.created++;return net;}};}
`;
await build({entryPoints:[new URL('app.js',root).pathname],bundle:true,format:'esm',outfile:path.join(temp,'app.js'),define:{__PHANTOM_BUILD__:JSON.stringify('breach-window-fixture'),__PHANTOM_DIAGNOSTIC_CODES__:'[]'},plugins:[{name:'controlled-network',setup(b){b.onResolve({filter:/browser-entry\.js$/},()=>({path:'sdk',namespace:'fixture'}));b.onResolve({filter:/shared-canvas\/src\/link\.js$/},()=>({path:'link',namespace:'fixture'}));b.onLoad({filter:/.*/,namespace:'fixture'},args=>({contents:args.path==='sdk'?fixture:'export const shareLink=async(invite,base)=>base,readLink=async()=>null;',loader:'js'}));}}]});
const server=http.createServer(async(req,res)=>{try{const name=new URL(req.url,'http://local').pathname.split('/').at(-1)||'index.html';const file=name==='app.js'?path.join(temp,name):new URL(name,root);res.setHeader('Content-Type',name.endsWith('.js')?'text/javascript':name.endsWith('.css')?'text/css':'text/html');res.end(await readFile(file));}catch{res.writeHead(404);res.end();}});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const browser=await chromium.launch({headless:true,...(process.env.PHANTOM_CHROMIUM_EXECUTABLE?{executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE}:{}),args:['--no-sandbox','--disable-dev-shm-usage']});
try{
 const page=await browser.newPage(),errors=[];page.on('pageerror',e=>errors.push(e.message));page.on('console',m=>{if(m.type()==='error'&&!m.text().includes('404'))errors.push(m.text());});
 await page.goto('http://127.0.0.1:'+server.address().port);await page.locator('#deploy').click();await page.waitForFunction(()=>window.fixture.created===1);
 await page.locator('#file').setInputFiles({name:'same.bin',mimeType:'application/octet-stream',buffer:Buffer.from([1,2,3,4])});
 for(let i=0;i<2;i++){
  await page.locator('#sourceRead').selectOption(i?'4194304':'0');
  await page.locator('#inject').click();await page.waitForFunction(n=>window.fixture.published.length===n,i+1);
  await page.waitForFunction(n=>window.fixture.sent.filter(m=>m.type==='file-hash').length===n,i+1);
  await page.evaluate(()=>window.fixture.finishSend());await page.waitForFunction(()=>document.querySelector('#result').textContent==='Peer verified');
 }
 assert.deepEqual(await page.evaluate(()=>window.fixture.published.map(p=>p.options.sourceReadBlockBytes)),[0,4194304]);
 const ids=await page.evaluate(()=>window.fixture.published.map(p=>p.options.distributionId));assert.equal(new Set(ids).size,2);assert.ok(ids.every(Boolean));
 for(const window of ['8','32']){
  await page.locator('#receiveWindow').selectOption(window);await page.evaluate(()=>window.fixture.incoming());
  await page.waitForFunction(n=>window.fixture.requested.length===n,window==='8'?1:2);
  assert.equal(await page.locator('#receiveWindow').isDisabled(),true);assert.equal(await page.locator('#sourceRead').isDisabled(),true);
  await page.evaluate(()=>window.fixture.finishReceive());await page.waitForFunction(()=>document.querySelector('#result').textContent==='File verified');
 }
 assert.deepEqual(await page.evaluate(()=>window.fixture.requested.map(p=>p.options.requestWindow)),[8,32]);assert.equal(await page.evaluate(()=>window.fixture.created),1);
 const download=page.waitForEvent('download');await page.locator('#saveLogs').click();const dl=await download;
 const report=JSON.parse(await readFile(await dl.path(),'utf8'));assert.deepEqual(report.events.filter(e=>e.type==='comparisonStart').map(e=>e.requestWindow),[8,32]);
 await page.setViewportSize({width:390,height:844});
 await page.locator('#inject').click();await page.waitForFunction(()=>window.fixture.sent.filter(m=>m.type==='file-hash').length===3);
 await page.evaluate(()=>window.fixture.finishSend());await page.waitForFunction(()=>document.querySelector('#result').textContent==='Peer verified');
 assert.equal(await page.evaluate(()=>window.fixture.created),1);
 assert.deepEqual(errors,[]);
 console.log('Fresh transfer IDs, both source modes, 8/32 receive settings, one session, busy locking, desktop/mobile Send access, verification and export pass. Controlled SDK fixture, not native throughput.');
}finally{await browser.close();await new Promise(r=>server.close(r));await rm(temp,{recursive:true,force:true});}
