import test from 'node:test';
import assert from 'node:assert/strict';
import {acceptsBootstrapRouteHint} from '../packages/phantom-core/src/bootstrap-route-hint.js';
const self='1111111111111111',source='2222222222222222',leader='3333333333333333';
const packet={type:'rtc_route_hint',pipe:'sys',videoId:'room',from:source,to:self,bootId:'source-boot',requestBootId:'own-boot',sysAttemptId:'attempt',leader};
const options={packet,room:'room',self,bootId:'own-boot',role:'peer',committed:false,expectedPeer:source,attemptId:'attempt',awaitingAnswer:true,livePeers:new Set([source,leader]),visited:new Set(),knownResponderBootId:'source-boot'};
test('a pending unadmitted attempt can follow a correlated live routing hint',()=>assert.equal(acceptsBootstrapRouteHint(options),true));
test('routing hints cannot replace committed authority or an answered attempt',()=>{
 for(const change of [{committed:true},{role:'syscon1'},{role:'syscon2'},{awaitingAnswer:false}])assert.equal(acceptsBootstrapRouteHint({...options,...change}),false);
});
test('routing hints reject replay, wrong room/recipient/incarnation, unknown targets and loops',()=>{
 for(const change of [{videoId:'other'},{to:source},{from:leader},{sysAttemptId:'retired'},{requestBootId:'old-boot'},{bootId:'retired-source'},{pipe:'chat'},{leader:self},{leader:source},{leader:'4444444444444444'}])assert.equal(acceptsBootstrapRouteHint({...options,packet:{...packet,...change}}),false);
 assert.equal(acceptsBootstrapRouteHint({...options,visited:new Set([leader])}),false);
 assert.equal(acceptsBootstrapRouteHint({...options,livePeers:new Set([leader])}),false);
});
