import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import vm from 'node:vm';
import {webcrypto} from 'node:crypto';
import {browserPakeGlue} from '@phantom-local/doorways/browser-pake-glue';
const root=new URL('../node_modules/spake2-wasm/mjs/',import.meta.url);
const original=await readFile(new URL('index_bg.js',root),'utf8');
const wasm=await readFile(new URL('index_bg.wasm',root));
async function restrictedLoader(source){
  // The browser's policy permits WASM but prohibits eval/new Function.
  const context=vm.createContext({crypto:webcrypto,TextEncoder,TextDecoder},{codeGeneration:{strings:false,wasm:true}});
  vm.runInContext('globalThis.self = globalThis;',context);
  assert.throws(()=>vm.runInContext('new Function("return this")',context),/Code generation/);
  const names=[...source.matchAll(/export (?:function|class)\s+(\w+)/g)].map(match=>match[1]);
  const glue=vm.runInContext(source.replaceAll('export ','')+'\n({'+names.join(',')+'});',context);
  const webAssembly=vm.runInContext('WebAssembly',context);
  const {instance}=await webAssembly.instantiate(wasm,{'./index_bg.js':glue});glue.initialize(instance.exports);
  return glue;
}
test('unpatched browser WASM reproduces the TV CSP string-evaluation failure',async()=>{
  const source=original.replace("import * as wasm from './index_bg.wasm';",'let wasm; export function initialize(exports){wasm=exports;}');
  const glue=await restrictedLoader(source);
  assert.throws(()=>glue.start('local-pin-csp-test','0007'),/Code generation/);
});
test('patched browser WASM completes real SPAKE2 with JS string evaluation blocked',async()=>{
  const glue=await restrictedLoader(browserPakeGlue(original));
  const host=glue.start('local-pin-csp-test','0007'),guest=glue.start('local-pin-csp-test','0007');
  try{
    const hm=glue.msg(host),gm=glue.msg(guest);
    const hk=glue.finish(host,gm),gk=glue.finish(guest,hm);
    assert.equal(hk.length,32);assert.deepEqual([...hk],[...gk]);hk.fill(0);gk.fill(0);
  }finally{if(host.ptr)host.free();if(guest.ptr)guest.free();}
});
test('loader adaptation rejects an unexpected upstream glue format',()=>{
  assert.throws(()=>browserPakeGlue(original.replace('var ret = new Function','var ret = OtherFunction')),/Unexpected/);
});
