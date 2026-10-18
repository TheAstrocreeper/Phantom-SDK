import test from 'node:test';
import assert from 'node:assert/strict';
import {createDiagnostics} from '../src/diagnostics.js';
test('diagnostics retain failures across reload, bound history, and exclude payloads and credentials',()=>{
 let saved;const storage={getItem:()=>saved,setItem:(k,v)=>{saved=v;}};
 const d=createDiagnostics({storage,limit:3,now:()=>123});
 d.record('one');d.record('two');d.record('error',{message:'failed https://example.test/#pair=SECRET',token:'SECRET',bytes:new ArrayBuffer(8),name:'private.mp4',proof:{secret:'SECRET'}});d.flush();
 const result=d.report();assert.equal(result.rows.length,3);assert.equal(result.dropped,1);assert.equal(JSON.stringify(result).includes('SECRET'),false);assert.equal(JSON.stringify(result).includes('private.mp4'),false);
 const restored=createDiagnostics({storage,limit:3});assert.equal(restored.report().rows.some(r=>r.type==='error'),true);
 const blocked=createDiagnostics({storage:{getItem(){throw Error();},setItem(){throw Error();}}});blocked.flush();assert.equal(blocked.report().storageError,true);
});
test('progress sampling preserves phase changes and failures',()=>{
 let at=1000;const d=createDiagnostics({now:()=>at});
 for(let i=0;i<200;i++)d.record('transfer-state',{id:'a',status:'preparing',received:i});
 d.record('transfer-state',{id:'a',status:'sharing'});d.record('membershipLost',{reason:'stale'});at+=1000;d.record('transfer-state',{id:'a',status:'sharing'});
 assert.equal(d.report().rows.length,5);
});
