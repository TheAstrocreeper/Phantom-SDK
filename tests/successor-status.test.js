import test from 'node:test';import assert from 'node:assert/strict';
import {acceptsSuccessorStatus} from '../packages/phantom-core/src/successor-status.js';
test('recovery status requires the historical mirror, newer committed roster and exact surviving members',()=>{
 const base={room:'net',syscon1:'a',syscon2:'b',epoch:2,roster:['a','b','c','d'].map(hKey=>({hKey}))};
 const message={room:'net',from:'b',syscon1:'b',syscon2:'c',role:'syscon1',epoch:3,rosterSeq:1,roster:['b','c','d'].map(hKey=>({hKey}))};
 const input={base,message,sender:'b',self:'d',recoveryTarget:'b',leaderPipeOpen:false};
 assert.equal(acceptsSuccessorStatus(input),true);
 for(const patch of [{leaderPipeOpen:true},{sender:'c'},{recoveryTarget:'c'},{self:'b'}])assert.equal(acceptsSuccessorStatus({...input,...patch}),false);
 for(const patch of [{epoch:2},{room:'other'},{role:'syscon2'},{syscon1:'c'},{roster:[...message.roster,{hKey:'e'}]},{roster:message.roster.slice(0,2)}])assert.equal(acceptsSuccessorStatus({...input,message:{...message,...patch}}),false);
});
