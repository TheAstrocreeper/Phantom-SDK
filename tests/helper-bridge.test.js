import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,readFile,rm,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {createHelperProbeAdapter,createBrowserHelperAdapter} from '../packages/phantom-tunnel/src/browser-helper.js';
import {installationPlan,installHelper} from '../packages/phantom-tunnel/tools/install-helper.mjs';
import {authorizedSender,boundedRequest} from '../packages/phantom-tunnel/browser-extension/policy.js';
import identity from '../packages/phantom-tunnel/browser-extension/identity.json' with {type:'json'};

const tick=()=>new Promise(r=>setImmediate(r));
function fixture(){const listeners=new Set(),sent=[];let closed=false;return {sent,get closed(){return closed;},send:m=>sent.push(m),subscribe(fn){listeners.add(fn);return()=>listeners.delete(fn);},close(){closed=true;},emit(m){for(const fn of listeners)fn(m);}};}
const scope=()=>({localPeer:'a'.repeat(16),remotePeer:'b'.repeat(16),sessionId:'ab'.repeat(16),bytes:1024,initiator:true});

test('extension identity is pinned to its public key and exact top-level site',()=>{
 const expected=[...createHash('sha256').update(Buffer.from(identity.key,'base64')).digest().subarray(0,16)].map(v=>String.fromCharCode(97+(v>>4),97+(v&15))).join('');assert.equal(identity.extensionId,expected);
 const sender={id:expected,tab:{id:1},frameId:0,url:'https://phantomshare.app/app',origin:'https://phantomshare.app'};
 assert(authorizedSender(sender,expected));
 for(const change of [{frameId:1},{url:'https://phantomshare.app.evil.test/'},{origin:'https://evil.test'},{id:'another-extension'},{tab:null}])assert(!authorizedSender({...sender,...change},expected));
 assert(boundedRequest({id:'a'.repeat(32),jobId:'b'.repeat(32),action:'close'}));
 assert(!boundedRequest({id:'a'.repeat(32),jobId:'b'.repeat(32),action:'shell'}));
});

test('helper adapter abort rejects pending setup immediately and sends scoped close',async()=>{
 const port=fixture(),adapter=createHelperProbeAdapter({port}),abort=new AbortController();
 const pending=adapter.prepare({...scope(),signal:abort.signal});abort.abort();await assert.rejects(pending,/cancelled/);
 assert.equal(port.sent[0].action,'prepare');assert.equal(port.sent[1].action,'close');assert.equal(port.sent[0].jobId,port.sent[1].jobId);
 port.emit({id:port.sent[0].id,ok:true,probeVersion:2,offer:{secret:'late'}});await tick();adapter.close();assert(port.closed);
});

test('helper adapter keeps receipts scoped, validates them and tears down on disconnect',async()=>{
 const port=fixture(),adapter=createHelperProbeAdapter({port});
 const prepared=adapter.prepare(scope());port.emit({id:port.sent[0].id,ok:true,probeVersion:2,offer:{test:'offer'}});const handle=await prepared;
 const connecting=handle.connect({test:'remote'});port.emit({id:port.sent[1].id,ok:true,connected:true});const tunnel=await connecting;
 port.emit({event:'complete',jobId:'ff'.repeat(16),result:{bytes:1024,sha256:'aa'.repeat(32),elapsedSeconds:1}});
 const done=tunnel.result;port.emit({event:'complete',jobId:port.sent[0].jobId,result:{bytes:1024,sha256:'aa'.repeat(32),elapsedSeconds:1,mbps:1}});
 assert.equal((await done).bytes,1024);
 const other=adapter.prepare(scope());port.emit({event:'unavailable'});await assert.rejects(other,/cancelled|closed/);assert(port.closed);
});

test('untrusted or framed page cannot construct the browser helper adapter',()=>{
 for(const origin of ['https://evil.test','http://phantomshare.app','https://phantomshare.app.evil.test']){const window={location:{origin}};window.top=window;assert.throws(()=>createBrowserHelperAdapter({window}),/trusted/);}
 const window={location:{origin:'https://phantomshare.app'},top:{}};assert.throws(()=>createBrowserHelperAdapter({window}),/trusted/);
});

test('per-user installer produces platform manifests and only writes when invoked',async()=>{
 const root=await mkdtemp(join(tmpdir(),'phantom-helper-install-'));
 try{
  const binary=join(root,'source-helper');await writeFile(binary,'test-native-binary');
  const plan=installationPlan({platform:'linux',home:root,binary});assert.equal(plan.manifest.allowed_origins[0],'chrome-extension://'+identity.extensionId+'/');
  await assert.rejects(stat(plan.manifestPath));await installHelper(plan);
  assert.equal(await readFile(plan.installed,'utf8'),'test-native-binary');assert.deepEqual(JSON.parse(await readFile(plan.manifestPath,'utf8')),plan.manifest);
  const win=installationPlan({platform:'win32',home:'C:\\Users\\Test',localAppData:'C:\\Users\\Test\\AppData\\Local',binary:'C:\\build\\phantom_helper.exe'});
  assert(win.registry.includes('HKCU\\Software\\Google\\Chrome\\NativeMessagingHosts\\app.phantomshare.tunnel'));
  assert(win.manifest.path.endsWith('phantom_helper.exe'));
  const mac=installationPlan({platform:'darwin',home:'/Users/test',binary:'/build/phantom_helper'});assert(mac.manifestPath.includes('Google/Chrome/NativeMessagingHosts'));
 }finally{await rm(root,{recursive:true,force:true});}
});

test('stale helper binaries are rejected before their offer can be forwarded',async()=>{
 const port=fixture(),adapter=createHelperProbeAdapter({port});
 const pending=adapter.prepare(scope());port.emit({id:port.sent[0].id,ok:true,probeVersion:1,offer:{old:'binary'}});
 await assert.rejects(pending,/version mismatch/);assert.equal(port.sent.at(-1).action,'close');
 adapter.close();
});
