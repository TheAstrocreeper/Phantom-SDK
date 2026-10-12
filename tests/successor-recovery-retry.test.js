import test from 'node:test';
import assert from 'node:assert/strict';
import {runDistributionTest} from './fixtures/distribution-scenario.js';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {Socket,RTC} from './fixtures/native-network.js';
const runtime={crypto:globalThis.crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval};

async function scenario(mode){
 let next=0,armed=false,failures=0,sawExhausted=false,leaf;const attempts=[];
 const recovering=()=>armed&&leaf?.snapshot().gossipRecovery?.targets[leaf.snapshot().gossipRecovery.index];
 class UnavailableSuccessorRTC extends RTC{
  async createOffer(){if(recovering()&&this.channels.some(c=>c.label==='six7-sys')){
   attempts.push(Date.now());if(mode==='exception'&&failures<2){failures++;throw Error('Successor not ready yet');}
  }return super.createOffer();}
  async setRemoteDescription(description){if(mode==='silence'&&description.type==='answer'&&recovering()&&failures<2){failures++;return;}return super.setRemoteDescription(description);}
 }
 function sdkFactory(config){const index=next++,sdk=createBrowserPhantom({...config,runtime:index===3?{...config.runtime,RTCPeerConnection:UnavailableSuccessorRTC,
   // Only speed up the native ten-second deadline for the silence fixture.
   setTimeout:(fn,ms,...args)=>setTimeout(fn,mode==='silence'&&ms===10000?300:ms,...args)}:config.runtime});
  async function watch(network){if(index===3){leaf=network;network.on('event',event=>{const r=event.type==='SNAPSHOT'?event.payload?.gossipRecovery:null;if(r?.targets.length&&r.index>=r.targets.length)sawExhausted=true;});}return network;}
  return {...sdk,create:async options=>watch(await sdk.create(options)),join:async(invite,options)=>watch(await sdk.join(invite,options))};
 }
 const report=await runDistributionTest({relays:['wss://nostr.fixture/'],WebSocket:Socket,runtime,sdkFactory,deadlineMs:12000,
  onUpdate:r=>{if(r.steps.some(s=>s.label.startsWith('Original leader leaves')&&s.status==='running'))armed=true;}});
 assert.equal(failures,2,'exercise repeated successor transport failures');
 assert.equal(sawExhausted,true,'reproduce exhausted recovery target and missing system route');
 assert.equal(report.passed,true,JSON.stringify(report));
 assert.equal(attempts.length,3,'one bounded connection attempt at a time, then stop after recovery');
 assert.ok(attempts[1]-attempts[0]>=900,'first retry waits at least one second');
 assert.ok(attempts[2]-attempts[1]>=1900,'second retry waits at least two seconds');
}
test('ordinary peer recovers after its first two successor pipe attempts fail',()=>scenario('exception'));
test('silent successor pipes expire and retry instead of permanently stranding a committed peer',()=>scenario('silence'));
