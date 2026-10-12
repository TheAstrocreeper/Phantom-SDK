import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {withNativeEntry} from '../packages/phantom-doorways/src/native-entry.js';
import {createDoorway} from '../packages/phantom-doorways/src/interface.js';

test('only selected leader SYS offer restores provisional pair; admission preserves later hints',()=>{
 let c;const hints=[],order=[];
 const doorway=createDoorway({discovery:true,open(context){c=context;return{announce(){},reconcile(){},sendSignal(){throw Error('adapter must not invent packets');},close(){}};}});
 const adapter=withNativeEntry(doorway);
 const s=adapter.open({roomId:'network',peerId:'echo',onOpen(){},onDiscovery:h=>{hints.push(h);order.push('hint');},onSignal:()=>order.push('signal')});
 const hint={kind:'concierge_update',roomId:'network',leader:'astro',mirror:'echo',epoch:2,extensions:{syscon2EndpointId:'echo-endpoint'}};
 c.onDiscovery(hint);assert.equal(hints.at(-1).extensions.bootstrapAdmission,true);
 c.onSignal({type:'rtc_offer',videoId:'network',from:'intruder',to:'echo',pipe:'sys'});assert.equal(hints.length,1);
 c.onSignal({type:'rtc_offer',videoId:'network',from:'astro',to:'echo',pipe:'chat'});assert.equal(hints.length,1);
 c.onSignal({type:'rtc_offer',videoId:'other',from:'astro',to:'echo',pipe:'sys'});assert.equal(hints.length,1);
 c.onSignal({type:'rtc_offer',videoId:'network',from:'astro',to:'echo',pipe:'sys'});
 assert.deepEqual(order.slice(-2),['hint','signal']);assert.deepEqual(hints.at(-1),hint);
 adapter.observeNetworkState({state:'admitted'});c.onDiscovery({...hint,leader:'nova'});assert.equal(hints.at(-1).extensions.bootstrapAdmission,undefined);
 s.close();const count=hints.length;c.onDiscovery(hint);assert.equal(hints.length,count);
});

for(const [layout,roster]of [['mixed','anchors'],['mixed','full'],['cf-only','full'],['cf-only','delayed']]){
 test(`actual SDK admission, committed roster and messages through native-only CF: ${layout}/${roster}`,async()=>{
  const {stdout}=await promisify(execFile)(process.execPath,['tests/fixtures/local-mixed-trace.mjs'],{
   cwd:new URL('../',import.meta.url),timeout:15000,
   env:{...process.env,PHANTOM_CF_ROSTER:roster==='delayed'?'full':roster,PHANTOM_TEST_LAYOUT:layout,PHANTOM_SIGNAL_DELAY:roster==='delayed'?'20':'0'}
  });
  assert.match(stdout,/true undefined/);
  assert.match(stdout,/Messages still arrive with all doorway sockets cut pass/);
 });
}

for(const [roster,abrupt]of [['anchors',false],['full',false],['full',true]]){
 test(`lifecycle: role departures, same-identity alternate-doorway returns, revision agreement; ${roster}, abrupt=${abrupt}`,async()=>{
  const {stdout}=await promisify(execFile)(process.execPath,['tests/fixtures/local-mixed-trace.mjs'],{
   cwd:new URL('../',import.meta.url),timeout:45000,
   env:{...process.env,PHANTOM_CF_ROSTER:roster,PHANTOM_TEST_LAYOUT:'mixed',PHANTOM_LIFECYCLE:'1',PHANTOM_ABRUPT:abrupt?'1':'0'}
  });
  assert.match(stdout,/Astro leaves; survivors agree on promotion pass/);
  assert.match(stdout,/Astro returns through nostr with the same identity pass/);
  if(abrupt)assert.match(stdout,/Echo loses all transports without a departure notice; survivors agree on promotion pass/);
  assert.match(stdout,/true undefined/);
 });
}
