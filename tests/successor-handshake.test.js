import test from 'node:test';import assert from 'node:assert/strict';
import {acceptsSuccessorHandshake as accepts} from '../packages/phantom-core/src/successor-handshake.js';
test('successor pipe requires accepted historical mirror and exact base and scope',()=>{
 const base={syscon1:'a',syscon2:'b',epoch:3,rosterSeq:2,roster:[{hKey:'a'},{hKey:'b'},{hKey:'c'}]};
 const proof={room:'room',from:'b',to:'c',bootId:'boot',baseOne:'a',baseTwo:'b',baseEpoch:3,baseSeq:2,baseRoster:base.roster,terminalLeader:'a'};
 const input={proof,base,sender:'b',self:'c',room:'room',leaderPipeOpen:false,bootId:'boot'};
 assert.equal(accepts(input),true);assert.equal(accepts({...input,leaderPipeOpen:true}),false);
 for(const patch of [{from:'rogue'},{baseEpoch:2},{baseSeq:1},{room:'other'},{to:'other'},{bootId:'old'},{baseRoster:[]},{terminalLeader:'b'}])assert.equal(accepts({...input,proof:{...proof,...patch}}),false);
});
