import {test} from 'node:test';
import assert from 'node:assert/strict';
import {createCapabilityReport,planConnection} from '../packages/phantom-core/src/connection-planner.js';
const report=(limit,protocols=['file-v1'])=>createCapabilityReport({version:1,messageLimit:limit,environment:{userAgent:'private'},stats:{rows:[{address:'192.0.2.1'}]}},{protocols});
test('report only exposes explicitly selected capabilities',()=>{
 const r=report({status:'known',bytes:262144});assert.deepEqual(r,{version:1,messageLimit:{status:'known',bytes:262144},protocols:['file-v1']});
});
test('plan respects smaller limit, overhead and actual implementation bound',()=>{
 const local=report({status:'known',bytes:262144}),remote=report({status:'known',bytes:131072});
 const p=planConnection({local,remote,maxPayloadBytes:65536,framingBytes:12,protocolPreference:['file-v1']});
 assert.equal(p.messageBytes,131072);assert.equal(p.payloadBytes,65536);assert.equal(p.status,'proposed');assert.equal(p.validateTargetConnection,true);
});
test('unknown limits use fallback and count framing bytes',()=>{
 const local=report({status:'unknown'}),remote=report({status:'known',bytes:262144});
 const p=planConnection({local,remote,framingBytes:9,protocolPreference:['file-v1']});assert.equal(p.payloadBytes,65527);assert.deepEqual(p.reasons,['local_message_limit_unknown']);
});
test('unsupported protocol or no room for payload blocks proposal',()=>{
 const local=report({status:'known',bytes:10}),remote=report({status:'known',bytes:10},['new']);
 const p=planConnection({local,remote,framingBytes:10,protocolPreference:['file-v1']});assert.equal(p.status,'unavailable');assert.deepEqual(p.reasons,['no_common_protocol','framing_exceeds_limit']);
});
test('unlimited observations still obey configured implementation cap',()=>{
 const local=report({status:'unlimited',bytes:null}),remote=report({status:'unlimited',bytes:null});const p=planConnection({local,remote,maxPayloadBytes:131072,defaultMessageBytes:262144,protocolPreference:['file-v1']});assert.equal(p.payloadBytes,131072);
});
