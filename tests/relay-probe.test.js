import {test} from 'node:test';
import assert from 'node:assert/strict';
import {probeRelay} from '../packages/phantom-doorways/src/relay-probe.js';
function ephemeralBus({reject=false}={}){
 const clients=[];
 return config=>{
  const slot={config,open:false,announcement:null};clients.push(slot);
  const emit=()=>{for(const other of clients)if(other!==slot&&other.open)other.config.onAnnouncement({...slot.announcement,s7pPublicKey:config.identity.publicKey});};
  const timer=setTimeout(()=>{slot.open=true;config.onStatus({type:'open'});if(slot.announcement)emit();},clients.length===1?1:30);
  return {reconnect(){slot.open=false;setTimeout(()=>{slot.open=true;config.onStatus({type:'open'});if(slot.announcement)emit();},1);},announce(p){slot.announcement=p;if(slot.open)queueMicrotask(emit);},close(){slot.open=false;clearTimeout(timer);},sendSignal(target,control){
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
test('explicitly delayed second peer still discovers and timeout prevents late creation',async()=>{
 const result=await probeRelay('wss://example.test/',{timeoutMs:500,secondPeerDelayMs:60,busFactory:ephemeralBus()});
 assert.equal(result.passed,true);assert.ok(result.timings[1].openedMs>=60);
 let creations=0;
 const fail=await probeRelay('wss://example.test/',{timeoutMs:100,secondPeerDelayMs:50,busFactory:c=>{creations++;queueMicrotask(()=>c.onStatus({type:'exhausted'}));return {announce(){},close(){}};}});
 await new Promise(r=>setTimeout(r,70));assert.equal(fail.reason,'connection_exhausted');assert.equal(creations,1);
});
test('stagger delay starts at first open, rather than first connection construction',async()=>{
 const start=Date.now();let creations=0,secondAt;
 const result=await probeRelay('wss://example.test/',{timeoutMs:300,secondPeerDelayMs:40,busFactory:c=>{
  creations++;if(creations===2){secondAt=Date.now()-start;queueMicrotask(()=>c.onStatus({type:'exhausted'}));}
  const timer=creations===1?setTimeout(()=>c.onStatus({type:'open'}),60):null;
  return {announce(){},close(){clearTimeout(timer);}};
 }});
 assert.equal(result.reason,'connection_exhausted');assert.ok(secondAt>=100,`second connection created at ${secondAt}ms`);
});
test('idle and reconnect require three separately acknowledged and delivered rounds',async()=>{
 const result=await probeRelay('wss://example.test/',{timeoutMs:600,idleReconnectMs:15,busFactory:ephemeralBus()});
 assert.equal(result.passed,true);assert.equal(result.reason,'idle_and_reconnect_delivery');
 assert.deepEqual(result.stages.map(s=>s.stage),['initial','after_idle','reconnect']);
 assert.equal(result.timeline.filter(e=>e.type==='signal_delivered').length,6);
});
test('initial success cannot hide missing delivery after idle',async()=>{
 const factory=ephemeralBus();
 const result=await probeRelay('wss://example.test/',{timeoutMs:180,idleReconnectMs:10,busFactory:config=>{
  const bus=factory(config),send=bus.sendSignal;
  bus.sendSignal=(target,control)=>{if(control.probeStage==='after_idle')return;return send(target,control);};return bus;
 }});
 assert.equal(result.passed,false);assert.deepEqual(result.stages.map(s=>s.stage),['initial']);
 assert.equal(result.signalsReceived,0);assert.equal(result.reason,'delivery_timeout');
});
test('sized probe verifies exact payload delivery and rejects altered contents',async()=>{
 const good=await probeRelay('wss://example.test/',{timeoutMs:400,payloadBytes:8192,busFactory:ephemeralBus()});assert.equal(good.passed,true);assert.equal(good.payloadBytes,8192);
 const factory=ephemeralBus();const bad=await probeRelay('wss://example.test/',{timeoutMs:150,payloadBytes:64,busFactory:c=>factory({...c,onSignal:p=>c.onSignal({...p,control:{...p.control,padding:'wrong'}})})});assert.equal(bad.passed,false);assert.equal(bad.signalsReceived,0);assert.equal(bad.signalsAccepted,2);
 assert.throws(()=>probeRelay('wss://example.test/',{payloadBytes:16385}),/payload/);
});
