import test from 'node:test';import assert from 'node:assert/strict';
import {normalizeRelays,probeRelay} from '../packages/phantom-doorways/src/relay-probe.js';
test('candidate normalization excludes insecure and credential-bearing endpoints',()=>{assert.deepEqual(normalizeRelays(['wss://a','wss://a/','ws://b','https://c','wss://user:pass@d','not-url']),['wss://a/']);});
function fixture({deliver=true,reject=false}={}){const sessions=[];let closed=0;return {get closed(){return closed;},factory(config){const session={close(){closed++;},announce(fields){queueMicrotask(()=>{for(const s of sessions)if(s!==session)s.config.onAnnouncement({...fields,s7pPublicKey:config.identity.publicKey});});},sendSignal(target,control,messageId){queueMicrotask(()=>{config.onStatus({type:'publish',packetType:'SIGNAL',accepted:!reject,reason:'fixture',messageId});if(deliver)for(const s of sessions)if(s.config.identity.publicKey===target.s7pPublicKey)s.config.onSignal({s7pPublicKey:config.identity.publicKey,control});});},config};sessions.push(session);queueMicrotask(()=>{config.onStatus({type:'open'});});return session;}};}
test('relay passes only with bidirectional signed-bus delivery and receipts',async()=>{const f=fixture();const r=await probeRelay('wss://fixture.example',{busFactory:f.factory,timeoutMs:100});assert.equal(r.passed,true);assert.equal(r.signalsReceived,2);assert.equal(r.signalsAccepted,2);assert.equal(f.closed,2);});
test('accepted publication without receipt by the other client fails',async()=>{const f=fixture({deliver:false});const r=await probeRelay('wss://fixture.example',{busFactory:f.factory,timeoutMs:20});assert.equal(r.passed,false);assert.equal(r.signalsReceived,0);assert.equal(f.closed,2);});
test('relay rejection fails and releases both clients',async()=>{const f=fixture({reject:true});const r=await probeRelay('wss://fixture.example',{busFactory:f.factory,timeoutMs:100});assert.equal(r.passed,false);assert.match(r.reason,/rejected/);assert.equal(f.closed,2);});
test('directory failure uses fallback and normalizes candidates',async()=>{
 const {discoverRelays}=await import('../packages/phantom-doorways/src/relay-discovery.js');let calls=0;
 const r=await discoverRelays({fetch:async()=>{calls++;if(calls===1)return{ok:false,status:502};return{ok:true,text:async()=> 'wss://extra.example\nhttps://wrong.example\nwss://extra.example/'};}});
 assert.equal(r.discovery[0].ok,false);assert.equal(r.discovery[1].ok,true);assert.ok(r.urls.includes('wss://extra.example/'));assert.ok(!r.urls.includes('https://wrong.example/'));
});
test('bundled defaults are nonempty, unique, credential-free secure relay URLs',async()=>{
 const {getDefaultRelays}=await import('../packages/phantom-doorways/src/defaults.js');const values=getDefaultRelays();
 assert.ok(values.length>0);assert.deepEqual(normalizeRelays(values),values);
});
