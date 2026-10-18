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

test('stage timing percentiles survive diagnostic sanitization and reload',()=>{
 let saved;const storage={getItem:()=>saved,setItem:(k,v)=>saved=v};const d=createDiagnostics({storage});
 d.record('distributionTransferTiming',{id:'asset:test',phase:'receive',metrics:{hashMs:{count:32,p50:1,p95:3,max:4,sum:40,sampleCount:32},writeMs:{count:32,p50:2,p95:5,max:6,sum:70,sampleCount:32}},payload:'secret'});d.flush();
 const restored=createDiagnostics({storage});const row=restored.report().rows.find(r=>r.type==='distributionTransferTiming');assert.equal(row.data.metrics.hashMs.p95,3);assert.equal(row.data.metrics.writeMs.sum,70);assert.equal(JSON.stringify(row).includes('secret'),false);
});
test('membership path outcomes and refresh deadline evidence survive sanitization',()=>{
 const d=createDiagnostics();
 d.record('MEMBERSHIP_CONTROL_PATH',{type:'peer',reason:'revocations',status:'failure',code:'timeout',elapsedMs:2000,request:{token:'SECRET'}});
 d.record('MEMBERSHIP_REFRESH',{status:'failure',code:'timeout',elapsedMs:5000,freshForMs:0,credential:'SECRET'});
 const rows=d.report().rows.slice(-2);
 assert.equal(rows[0].data.code,'timeout');assert.equal(rows[0].data.elapsedMs,2000);
 assert.equal(rows[1].data.freshForMs,0);assert.equal(JSON.stringify(rows).includes('SECRET'),false);
});
