import {test} from 'node:test';import assert from 'node:assert/strict';
import {createPlanAgreement} from '../packages/phantom-core/src/plan-agreement.js';
import {createCapabilityReport} from '../packages/phantom-core/src/connection-planner.js';
const ids=['1111111111111111','2222222222222222'];
const report=maxChunkBytes=>createCapabilityReport({version:1,messageLimit:{status:'unknown'}},{protocols:['file-v1'],maxChunkBytes});
test('two admitted peers agree on a safe plan before a file is prepared',()=>{
 const nodes=[],events=[];for(let i=0;i<2;i++)nodes[i]=createPlanAgreement({send:(p,body)=>nodes[1-i].receive(ids[i],body),allowed:p=>p===ids[1-i],localReport:()=>report(i?65536:262144),remoteReport:()=>report(i?262144:65536),newId:()=>`plan-${i}`,onDecision:e=>events.push(e)});
 assert.equal(nodes[0].propose(ids[1],{protocolPreference:['file-v1'],maxPayloadBytes:262144}),'plan-0');assert.equal(nodes[0].get(ids[1]).plan.payloadBytes,65536);assert.equal(events[0].accepted,true);nodes[0].drop(ids[1]);assert.equal(nodes[0].get(ids[1]),null);
});
test('rejects proposals beyond receiver bounds and ignores spoofed decisions',()=>{
 const out=[];const n=createPlanAgreement({send:(p,b)=>out.push(b),allowed:()=>true,localReport:()=>report(65536),remoteReport:()=>report(65536),newId:()=> 'token'});
 n.receive(ids[0],{type:'planProposal',version:1,id:'x',protocol:'file-v1',payloadBytes:131072,framingBytes:0,messageBytes:131072});assert.equal(out[0].accepted,false);
 n.propose(ids[0],{protocolPreference:['file-v1']});assert.equal(n.receive(ids[1],{type:'planDecision',version:1,id:'token',accepted:true}),false);assert.equal(n.get(ids[0]),null);
});
test('a replacement proposal invalidates the earlier plan and late acknowledgments',()=>{
 let id=0;const n=createPlanAgreement({send(){},allowed:()=>true,localReport:()=>report(65536),remoteReport:()=>report(65536),newId:()=>`plan-${++id}`});
 const one=n.propose(ids[0],{protocolPreference:['file-v1']}),two=n.propose(ids[0],{protocolPreference:['file-v1']});
 assert.equal(n.receive(ids[0],{type:'planDecision',version:1,id:one,accepted:true}),false);assert.equal(n.get(ids[0]),null);
 assert.equal(n.receive(ids[0],{type:'planDecision',version:1,id:two,accepted:true}),true);assert.equal(n.get(ids[0]).id,two);
 n.propose(ids[0],{protocolPreference:['file-v1']});assert.equal(n.get(ids[0]),null);
});
test('peers agree larger chunks only when both support fragment fallback',()=>{
 const protocols=['file-v1','phantom-file-fragments-v1'];const cap=createCapabilityReport({version:1,messageLimit:{status:'unknown'}},{protocols,maxChunkBytes:262144});const nodes=[];
 for(let i=0;i<2;i++)nodes[i]=createPlanAgreement({send:(p,body)=>nodes[1-i].receive(ids[i],body),allowed:p=>p===ids[1-i],localReport:()=>cap,remoteReport:()=>cap,newId:()=>`wide-${i}`});
 nodes[0].propose(ids[1],{protocolPreference:['file-v1'],maxPayloadBytes:262144,allowFragmentation:true});assert.equal(nodes[0].get(ids[1]).plan.payloadBytes,262144);assert.equal(nodes[0].get(ids[1]).plan.fragmentFallback,'phantom-file-fragments-v1');
});
