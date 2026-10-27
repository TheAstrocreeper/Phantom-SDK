import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createCapabilityExchange} from '../packages/phantom-core/src/capability-exchange.js';
import {createCapabilityReport,planConnection} from '../packages/phantom-core/src/connection-planner.js';
const ids=['1111111111111111','2222222222222222'];
test('admitted peers exchange bounded capability reports and can select a compatible plan',()=>{
 const observations=[],nodes=[];const report=maxChunkBytes=>createCapabilityReport({version:1,messageLimit:{status:'unknown',bytes:null}},{protocols:['file-v1'],maxChunkBytes});
 for(let i=0;i<2;i++)nodes[i]=createCapabilityExchange({send:(peer,body)=>nodes[1-i].receive(ids[i],body),allowed:peer=>peer===ids[1-i],localReport:()=>report(i?65536:262144),onReport:(peer,r)=>observations.push({peer,r})});
 nodes[0].request(ids[1]);const remote=nodes[0].get(ids[1]);assert.equal(remote.maxChunkBytes,65536);assert.equal(observations.length,1);
 assert.equal(planConnection({local:report(262144),remote,protocolPreference:['file-v1'],maxPayloadBytes:262144}).payloadBytes,65536);
 nodes[0].drop(ids[1]);assert.equal(nodes[0].get(ids[1]),null);nodes.forEach(x=>x.close());
});
test('rejects unknown senders and malformed reports without disclosing local data',()=>{
 let sent=0;const n=createCapabilityExchange({send:()=>sent++,allowed:()=>false,localReport:()=>({})});
 assert.equal(n.receive(ids[0],{type:'capabilityRequest',version:1}),false);assert.throws(()=>n.request(ids[0]),/unavailable/);assert.equal(sent,0);
 const m=createCapabilityExchange({send(){},allowed:()=>true,localReport:()=>({})});assert.equal(m.receive(ids[0],{type:'capabilityReport',version:1,report:{version:1,protocols:['file-v1'],maxChunkBytes:999999}}),false);assert.equal(m.get(ids[0]),null);
});
