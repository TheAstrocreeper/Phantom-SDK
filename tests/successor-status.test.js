import test from 'node:test';
import assert from 'node:assert/strict';
import {acceptsSuccessorStatus} from '../packages/phantom-core/src/successor-status.js';
const one='1111111111111111',two='2222222222222222',self='3333333333333333',added='4444444444444444';
const row=hKey=>({hKey});
const base={room:'room',syscon1:one,syscon2:two,epoch:2,rosterSeq:7,roster:[one,two,self].map(row)};
const message={room:'room',from:two,syscon1:two,syscon2:self,role:'syscon1',epoch:3,rosterSeq:1,roster:[two,self].map(row)};
const options={base,message,sender:two,self,recoveryTarget:two,leaderPipeOpen:false};
test('historical mirror status preserves survivors when a new member joins during recovery',()=>{
 assert.equal(acceptsSuccessorStatus(options),true);
 assert.equal(acceptsSuccessorStatus({...options,message:{...message,rosterSeq:2,roster:[two,self,added].map(row)}}),true);
});
test('successor status rejects leader resurrection, missing survivors, duplicate identities and untrusted source',()=>{
 for(const roster of [[one,two,self],[two],[two,self,self]])assert.equal(acceptsSuccessorStatus({...options,message:{...message,roster:roster.map(row)}}),false);
 for(const change of [{sender:added},{recoveryTarget:added},{leaderPipeOpen:true},{message:{...message,room:'other'}},{message:{...message,epoch:2}}])assert.equal(acceptsSuccessorStatus({...options,...change}),false);
});
