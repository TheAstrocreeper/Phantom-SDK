import test from 'node:test';
import assert from 'node:assert/strict';
import {createDiagnostics} from '../src/diagnostics.js';
test('comparison window and bounded stage metrics survive export without payloads',()=>{
 const d=createDiagnostics({build:'test'});
 d.record('comparisonStart',{run:2,requestWindow:32,role:'receiver',result:'fresh',name:'SECRET',token:'SECRET'});
 d.record('fileTiming',{phase:'send',metrics:{readMs:{count:32,p95:1,sum:20},bufferWaitMs:{count:32,p95:.2,sum:2},payload:'SECRET'},bytes:new ArrayBuffer(8)});
 const r=d.report();assert.equal(r.events[0].requestWindow,32);assert.equal(r.events[1].metrics.bufferWaitMs.p95,.2);
 assert.equal(JSON.stringify(r).includes('SECRET'),false);assert.equal(r.events[1].phase,'send');
});
