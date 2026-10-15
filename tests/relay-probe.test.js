import {test} from 'node:test';
import assert from 'node:assert/strict';
import {probeRelay} from '../packages/phantom-doorways/src/relay-probe.js';
function ephemeralBus({reject=false}={}){
 const clients=[];
 return config=>{
  const slot={config,open:false,announcement:null};clients.push(slot);
  const emit=()=>{for(const other of clients)if(other!==slot&&other.open)other.config.onAnnouncement({...slot.announcement,s7pPublicKey:config.identity.publicKey});};
  const timer=setTimeout(()=>{slot.open=true;config.onStatus({type:'open'});if(slot.announcement)emit();},clients.length===1?1:30);
  return {announce(p){slot.announcement=p;if(slot.open)queueMicrotask(emit);},close(){slot.open=false;clearTimeout(timer);},sendSignal(target,control){
   queueMicrotask(()=>{config.onStatus({type:'publish',packetType:'SIGNAL',accepted:!reject,reason:reject?'blocked: test policy':''});if(!reject)for(const other of clients)if(other.open&&other.config.identity.publicKey===target.s7pPublicKey)other.config.onSignal({s7pPublicKey:config.identity.publicKey,control});});
  }};
 };
}
test('staggered ephemeral subscriptions discover both peers and deliver probe controls',async()=>{
 const result=await probeRelay('wss://example.test/',{timeoutMs:500,busFactory:ephemeralBus()});
 assert.equal(result.passed,true);assert.equal(result.signalsReceived,2);assert.equal(result.signalsAccepted,2);
 assert.equal(result.timeline.filter(e=>e.type==='peer_discovered').length,2);
});
test('publication rejection remains distinct from successful delivery',async()=>{
 const result=await probeRelay('wss://example.test/',{timeoutMs:500,busFactory:ephemeralBus({reject:true})});
 assert.equal(result.passed,false);assert.match(result.reason,/publication_rejected: blocked/);assert.equal(result.signalsReceived,0);
});
