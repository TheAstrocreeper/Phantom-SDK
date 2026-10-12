import test from 'node:test';import assert from 'node:assert/strict';
import {runMixedTest,socketGate} from './fixtures/mixed-doorways-scenario.js';
class Socket{static OPEN=1;constructor(){this.closed=false;}addEventListener(){}close(){this.closed=true;}}
const endpoint='wss://fixture.example/';
function setup({blockCloudflare=false}={}){
 const peers=new Map(),calls=[],sent=[];let sockets=[];
 function sdkFactory(config){
  const socket=new config.WebSocket(endpoint);sockets.push(socket);
  function network(options){
   const id=options.name,chatPairs=[];
   const n={invite:'fixture',getState:()=>({state:'admitted',peerId:id}),snapshot:()=>({concierge:{sysReady:true,membershipAudit:{topology:{committedCount:peers.size,committedKeys:[...peers.keys()].sort().join(','),committedOne:'Astro',committedTwo:'Nova'}}},chatPairs}),
    invitePeer(to){peers.get(to).options.onEvent({type:'VEIL_INVITE_RECEIVED',payload:{from:id}});},
    acceptPeer(from){chatPairs.push({hKey:from,dcState:'open',chatState:'ready'});peers.get(from).chatPairs.push({hKey:id,dcState:'open',chatState:'ready'});options.onEvent({type:'P2P_CHAT_READY',payload:{from}});},
    send(to,text){sent.push({from:id,to,offline:sockets.every(s=>s.closed)});peers.get(to).options.onEvent({type:'INCOMING_MSG',payload:{from:id,text}});},
    close(){this.closed=true;}};
   peers.set(id,{n,options,chatPairs});options.onState(n.getState());return n;
  }
  return {create:async options=>{calls.push({kind:'create',options,config});return network(options);},join:async(invite,options)=>{
   calls.push({kind:'join',options,config});if(blockCloudflare&&options.name==='Echo')throw new Error('CF fixture rejected');return network(options);
  }};
 }
 return {sdkFactory,calls,sent,peers};
}
test('mixed harness selects different paths and requires six deliveries after cuts',async()=>{
 const f=setup();const r=await runMixedTest({endpoint,sdkFactory:f.sdkFactory,WebSocket:Socket,runtime:{crypto:globalThis.crypto},deadlineMs:100});
 assert.equal(r.passed,true);assert.equal(r.steps.length,8);assert.ok(r.steps.every(s=>s.status==='pass'));
 assert.deepEqual(f.calls.map(c=>c.options.doorwayTypes),[['nostr','websocket'],['nostr'],['websocket']]);
 assert.equal(f.sent.length,12);assert.equal(f.sent.filter(s=>s.offline).length,6);assert.ok([...f.peers.values()].every(p=>p.n.closed));
});
test('mixed harness identifies failed CF admission and cleans established clients',async()=>{
 const f=setup({blockCloudflare:true});const r=await runMixedTest({endpoint,sdkFactory:f.sdkFactory,WebSocket:Socket});
 assert.equal(r.passed,false);assert.equal(r.steps.at(-1).status,'fail');assert.match(r.steps.at(-1).label,/Cloudflare/);assert.equal(r.error,'CF fixture rejected');assert.ok([...f.peers.values()].every(p=>p.n.closed));assert.equal(f.sent.length,0);
});
test('cancelled test does not create a client or report success',async()=>{
 const f=setup(),c=new AbortController();c.abort();const r=await runMixedTest({endpoint,sdkFactory:f.sdkFactory,WebSocket:Socket,signal:c.signal});assert.equal(r.passed,false);assert.equal(f.calls.length,0);assert.equal(r.error,'Test cancelled');
});
test('socket gate closes existing setup sockets and prevents reconnect',()=>{
 const gate=socketGate(Socket),socket=new gate.Socket(endpoint);gate.cut();assert.ok(socket.closed);assert.ok(gate.offline);assert.throws(()=>new gate.Socket(endpoint),/offline/);
});
