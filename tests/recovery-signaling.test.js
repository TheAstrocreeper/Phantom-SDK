import test from 'node:test';
import assert from 'node:assert/strict';
import {canCarrySysRecovery} from '../packages/phantom-core/src/recovery-signaling.js';

const packet={type:'rtc_offer',videoId:'network',from:'a',to:'b',pipe:'sys',sysCarrier:true,bootId:'boot'};
const context={packet,room:'network',self:'a',remote:'b',member:true,terminal:false,accepted:true,ready:true,current:true};
test('recovery signaling needs a current accepted peer pipe and committed membership',()=>{
 assert.equal(canCarrySysRecovery(context),true);
 for(const change of [{current:false},{member:false},{terminal:true},{accepted:false},{ready:false},{room:'other'},{self:'impostor'},{remote:'other'}])assert.equal(canCarrySysRecovery({...context,...change}),false);
 for(const change of [{type:'authority_exit'},{type:'MSG'},{pipe:'chat'},{sysCarrier:false},{bootId:''},{from:'impostor'},{to:'other'}])assert.equal(canCarrySysRecovery({...context,packet:{...packet,...change}}),false);
 for(const type of ['rtc_answer','rtc_ice'])assert.equal(canCarrySysRecovery({...context,packet:{...packet,type}}),true);
});
