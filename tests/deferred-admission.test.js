import test from 'node:test';import assert from 'node:assert/strict';
import {resumeDeferredAdmissions} from '../packages/phantom-core/src/deferred-admission.js';
test('admission held behind a commit resumes once, then waits for its own mirror commit',()=>{
 const first={},second={},requests=new Map([['a',{key:'a',entry:first,inner:{bootId:'new-a'}}],['b',{key:'b',entry:second,inner:{bootId:'new-b'}}]]);
 const entries=new Map([['a',first],['b',second]]);let pending=true;const applied=[];
 const context={hasPending:()=>pending,currentEntry:key=>entries.get(key),pipeOpen:()=>true,apply:(key,p)=>{applied.push([key,p.bootId]);pending=true}};
 resumeDeferredAdmissions(requests,context);assert.equal(requests.size,2);assert.equal(applied.length,0);
 pending=false;resumeDeferredAdmissions(requests,context);assert.deepEqual(applied,[['a','new-a']]);assert.equal(requests.size,1);
 pending=false;resumeDeferredAdmissions(requests,context);assert.deepEqual(applied,[['a','new-a'],['b','new-b']]);assert.equal(requests.size,0);
});
test('retired or closed transport cannot replay a previously eligible admission',()=>{
 const old={},requests=new Map([['old',{key:'old',entry:old}],['closed',{key:'closed',entry:old}]]);let applied=0,retired=0;
 resumeDeferredAdmissions(requests,{hasPending:()=>false,currentEntry:key=>key==='old'?{}:old,pipeOpen:()=>false,apply:()=>applied++,onRetired:()=>retired++});
 assert.equal(applied,0);assert.equal(retired,2);assert.equal(requests.size,0);
});
