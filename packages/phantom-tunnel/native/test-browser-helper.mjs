/** Optional Linux Chromium integration test. Uses a fresh temporary HOME/profile,
 * intercepted test pages, and a per-test native host registration. No real
 * phantomshare.app request, existing browser profile or user installation.
 * Requires Playwright + its Chromium (or PHANTOM_CHROMIUM_EXECUTABLE).
 */
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {installationPlan,installHelper} from '../tools/install-helper.mjs';
import {binary,identity} from './helper-test-port.mjs';
const {chromium}=await import(process.env.PHANTOM_PLAYWRIGHT_MODULE??'playwright');
const extension=fileURLToPath(new URL('../browser-extension',import.meta.url));

test('real Chromium extension connects its trusted page to two native helpers',{timeout:60000,skip:process.platform!=='linux'},async()=>{
 const root=await mkdtemp(join(tmpdir(),'phantom-browser-helper-')),home=join(root,'home'),profile=join(root,'profile');
 let context;
 try {
  const plan=installationPlan({platform:'linux',home,binary});await installHelper(plan);
  for(const dir of [join(home,'.config','chromium','NativeMessagingHosts'),join(home,'.config','google-chrome-for-testing','NativeMessagingHosts'),join(profile,'NativeMessagingHosts')]){await mkdir(dir,{recursive:true});await writeFile(join(dir,identity.hostName+'.json'),JSON.stringify(plan.manifest));}
  context=await chromium.launchPersistentContext(profile,{headless:true,...(process.env.PHANTOM_CHROMIUM_EXECUTABLE?{executablePath:process.env.PHANTOM_CHROMIUM_EXECUTABLE}:{channel:'chromium'}),args:['--disable-extensions-except='+extension,'--load-extension='+extension],env:{...process.env,HOME:home,XDG_CONFIG_HOME:join(home,'.config'),PHANTOM_HELPER_BIND:'127.0.0.1',PHANTOM_HELPER_STUN:'none'}});
  await context.route('https://phantomshare.app/**',async route=>{
   const pathname=new URL(route.request().url()).pathname;
   if(pathname==='/browser-helper.js')await route.fulfill({contentType:'text/javascript',body:await readFile(new URL('../src/browser-helper.js',import.meta.url),'utf8')});
   else await route.fulfill({contentType:'text/html',body:'<!doctype html><title>Phantom helper test</title>'});
  });
  const worker=context.serviceWorkers()[0]??await context.waitForEvent('serviceworker');
  assert(worker.url().startsWith('chrome-extension://'+identity.extensionId+'/'));
  const a=await context.newPage(),b=await context.newPage();await Promise.all([a.goto('https://phantomshare.app/'),b.goto('https://phantomshare.app/')]);
  const options={localPeer:'a'.repeat(16),remotePeer:'b'.repeat(16),sessionId:'ab'.repeat(16),bytes:32*1024*1024,initiator:true};
  const prepare=(page,scope)=>page.evaluate(async scope=>{const {createBrowserHelperAdapter}=await import('/browser-helper.js');window.adapter=createBrowserHelperAdapter();window.handle=await window.adapter.prepare(scope);return window.handle.offer;},scope);
  const [ao,bo]=await Promise.all([prepare(a,options),prepare(b,{...options,localPeer:options.remotePeer,remotePeer:options.localPeer,initiator:false})]);
  await Promise.all([a.evaluate(async offer=>{window.tunnel=await window.handle.connect(offer);},bo),b.evaluate(async offer=>{window.tunnel=await window.handle.connect(offer);},ao)]);
  const [sent,received]=await Promise.all([a.evaluate(()=>window.tunnel.result),b.evaluate(()=>window.tunnel.result)]);
  assert.equal(sent.bytes,32*1024*1024);assert.equal(sent.sha256,received.sha256);
  await Promise.all([a.evaluate(()=>window.adapter.close()),b.evaluate(()=>window.adapter.close())]);
 }finally{await context?.close();await rm(root,{recursive:true,force:true});}
});
