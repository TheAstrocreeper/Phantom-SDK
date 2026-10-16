import assert from 'node:assert/strict';
import http from 'node:http';
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {chromium} from '../swarm/node_modules/playwright/index.mjs';
import {startServer} from '../swarm/server.mjs';
const bundle=readFileSync(new URL('./dist/app.js',import.meta.url)),html=readFileSync(new URL('./index.html',import.meta.url));
const output=new URL('./reports/',import.meta.url).pathname;mkdirSync(output,{recursive:true});
const relay=await startServer(bundle,()=>{}),app=http.createServer((req,res)=>{res.setHeader('Content-Type',req.url==='/app.js'?'text/javascript':'text/html');res.end(req.url==='/app.js'?bundle:html);});
await new Promise(r=>app.listen(0,'127.0.0.1',r));const url=`http://127.0.0.1:${app.address().port}`;let browser;const pages=[],pageErrors=[];
const inspect=p=>p.locator('#state').textContent().then(JSON.parse);
async function download(p,id,name){const [d]=await Promise.all([p.waitForEvent('download'),p.locator(id).click()]);const path=join(output,name);await d.saveAs(path);return path;}
try{
 browser=await chromium.launch({headless:true,...process.env.PHANTOM_CHROMIUM_PATH?{executablePath:process.env.PHANTOM_CHROMIUM_PATH}:{},args:['--no-sandbox','--disable-dev-shm-usage']});
 for(let i=0;i<3;i++){const ctx=await browser.newContext({acceptDownloads:true,...i===1?{viewport:{width:390,height:844},isMobile:true,hasTouch:true}:{}}),p=await ctx.newPage();p.on('pageerror',e=>pageErrors.push(e.message));await p.goto(url);await p.locator('#advanced').evaluate(e=>e.open=true);await p.locator('#doorway').fill(relay.url.replace('http:','ws:'));await p.locator('#rtc').fill('{"iceServers":[]}');await p.locator('#name').fill('peer-'+i);pages.push(p);}
 const [source,receiver,excluded]=pages;await source.locator('#create').click();await source.waitForFunction(()=>document.querySelector('#joinLink').value.length>0);
 const link=await source.locator('#joinLink').inputValue(),invite=await source.evaluate(()=>JSON.parse(new URLSearchParams(location.hash).get('phantom')||'null'));
 await receiver.goto('about:blank');await receiver.goto(link);await receiver.locator('#advanced').evaluate(e=>e.open=true);await receiver.locator('#rtc').fill('{"iceServers":[]}');await receiver.locator('#join').click();
 await excluded.goto('about:blank');await excluded.goto(link);await excluded.locator('#advanced').evaluate(e=>e.open=true);await excluded.locator('#rtc').fill('{"iceServers":[]}');await excluded.locator('#join').click();
 for(const p of pages)await p.waitForFunction(()=>{try{const s=JSON.parse(document.querySelector('#state').textContent);return s.state.state==='admitted'&&s.connections.length===2&&s.connections.every(c=>c.state==='ready');}catch{return false;}},{},{timeout:60000});
 const sourceId=(await inspect(source)).state.peerId,receiverId=(await inspect(receiver)).state.peerId;
 assert.equal(await source.locator('#run').count(),0,'no baseline wizard');assert.equal(await source.locator('#armRecovery').count(),0,'no recovery wizard');
 assert.equal(await receiver.locator('#publish').isDisabled(),true);assert.equal(await source.locator('#report').isEnabled(),true);
 await source.locator('#target').selectOption(receiverId);await source.locator('#text').fill('hello phone');await source.locator('#send').click();await receiver.waitForFunction(()=>document.querySelector('#messages').textContent.includes('hello phone'));
 await receiver.locator('#target').selectOption(sourceId);await receiver.locator('#text').fill('hello PC');await receiver.locator('#send').click();await source.waitForFunction(()=>document.querySelector('#messages').textContent.includes('hello PC'));
 const bytes=Buffer.from(Array.from({length:262144},(_,i)=>(i*31+(i>>>8)+17)%256)),expected=createHash('sha256').update(bytes).digest('hex');
 await source.locator('#file').setInputFiles({name:'fixture.bin',mimeType:'application/octet-stream',buffer:bytes});await source.locator('#publish').click();await receiver.locator('#files a').waitFor({timeout:60000});
 const path=await download(receiver,'#files a','received.bin');assert.equal(createHash('sha256').update(readFileSync(path)).digest('hex'),expected);
 const report=JSON.parse(readFileSync(await download(source,'#report','controller.json')));assert.equal(report.companions.find(([id])=>id===receiverId)[1].received[0][1].sha256,expected);assert.deepEqual(report.companions.find(([id])=>id!==receiverId)[1].received,[]);assert.equal(await excluded.locator('#files a').count(),0);assert.equal(await source.locator('#publish').isEnabled(),true,'log export preserves file sending');
 assert.equal(await receiver.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'mobile viewport fits');
 await source.screenshot({path:join(output,'desktop.png'),fullPage:true});await receiver.screenshot({path:join(output,'mobile.png'),fullPage:true});
 await receiver.evaluate(()=>window.dispatchEvent(new ErrorEvent('error',{error:Error('retained failure marker'),message:'retained failure marker'})));
 await receiver.locator('#leave').click();const closed=JSON.parse(readFileSync(await download(receiver,'#report','after-disconnect.json')));assert.ok(closed.events.some(e=>e.type==='before.close'));assert.ok(closed.events.some(e=>e.type==='error'&&e.data.message==='retained failure marker'));
 await receiver.reload();await receiver.locator('#advanced').evaluate(e=>e.open=true);await receiver.locator('#rtc').fill('{"iceServers":[]}');await receiver.locator('#join').click();await receiver.waitForFunction(()=>{try{return JSON.parse(document.querySelector('#state').textContent).state.state==='admitted';}catch{return false;}},{},{timeout:60000});
 const nextId=(await inspect(receiver)).state.peerId;await source.waitForFunction(id=>{try{return JSON.parse(document.querySelector('#state').textContent).connections.some(p=>p.peerId===id&&p.state==='ready');}catch{return false;}},nextId,{timeout:60000});
 const retained=JSON.parse(readFileSync(await download(source,'#report','retained.json'))).companions.find(([id])=>id===nextId)[1];assert.ok(retained.retained.events.some(e=>e.type==='error'&&e.data.message==='retained failure marker'));
 assert.deepEqual(pageErrors,[]);for(const p of pages)await p.locator('#leave').click();
 console.log(JSON.stringify({status:'passed',browser:browser.version(),checks:['three native peers','two-way visible messages','selected receiver downloaded exact bytes','excluded peer no bytes','PC downloads companion logs','export preserves sending','mobile viewport fits','logs after disconnect','failure logs collected after reload/rejoin','no baseline or recovery wizards'],sha256:expected}));
}catch(e){for(let i=0;i<pages.length;i++)try{writeFileSync(join(output,`failure-${i}.json`),JSON.stringify(await pages[i].evaluate(()=>({state:document.querySelector('#state').textContent,notice:document.querySelector('#notice').textContent,log:document.querySelector('#log').textContent})),null,2));}catch{}throw e;
}finally{await browser?.close();await relay.close();await new Promise(r=>app.close(r));}
