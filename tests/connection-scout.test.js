import {test} from 'node:test';
import assert from 'node:assert/strict';
import {scoutConnection} from '../packages/phantom-core/src/connection-scout.js';

test('scout retains raw browser stats locally and separately summarizes the route', async () => {
  const rows = new Map([['t',{type:'transport',selectedCandidatePairId:'p'}],['p',{type:'candidate-pair',localCandidateId:'l',remoteCandidateId:'r',currentRoundTripTime:0.12,bytesSent:17,bytesReceived:8}],['l',{candidateType:'host',protocol:'udp',address:'192.168.1.7'}],['r',{candidateType:'srflx',address:'203.0.113.5'}]]);
  const pc={connectionState:'connected',iceConnectionState:'connected',sctp:{state:'connected',maxMessageSize:65536,maxChannels:100,transport:{state:'connected'}},getStats:async()=>rows};
  const channel={readyState:'open',ordered:true,bufferedAmount:11,bufferedAmountLowThreshold:4,maxRetransmits:null,maxPacketLifeTime:null};
  const result=await scoutConnection(pc,{channel,runtime:{}});
  assert.deepEqual(result.messageLimit,{status:'known',bytes:65536});
  assert.deepEqual(result.route,{localType:'host',remoteType:'srflx',protocol:'udp',rttSeconds:0.12,bytesSent:17,bytesReceived:8});
  assert.equal(result.channel.bufferedBytes,11);
  assert.equal(result.statsStatus,'available');
  assert.equal(result.stats.total,4);
  assert.equal(result.stats.containsNetworkIdentifiers,true);
  assert.match(JSON.stringify(result.stats.rows),/192\.168\.1\.7/);
});

test('unavailable data stays unknown, including before SCTP negotiation', async () => {
  const result=await scoutConnection({sctp:null},{runtime:{}});
  assert.deepEqual(result.messageLimit,{status:'unknown',bytes:null});
  assert.equal(result.route,null);assert.equal(result.statsStatus,'unavailable');
  assert.equal(result.channel,null);
});

test('unlimited message size is JSON-safe and stats failures retain capabilities', async () => {
  const result=await scoutConnection({sctp:{maxMessageSize:Infinity},getStats:async()=>{throw Error('not available');}},{runtime:{}});
  assert.deepEqual(result.messageLimit,{status:'unlimited',bytes:null});
  assert.equal(result.statsStatus,'error');assert.deepEqual(JSON.parse(JSON.stringify(result)),result);
});

test('multiple selected transports do not produce a guessed route', async () => {
  const rows=new Map([['t1',{type:'transport',selectedCandidatePairId:'p1'}],['t2',{type:'transport',selectedCandidatePairId:'p2'}],['p1',{type:'candidate-pair'}]]);
  const result=await scoutConnection({getStats:async()=>rows},{runtime:{}});
  assert.equal(result.route,null);assert.equal(result.statsStatus,'available');
});

test('raw stats are bounded and disclose truncation', async () => {
  const rows=new Map(Array.from({length:520},(_,i)=>[String(i),{id:String(i),type:'candidate-pair'}]));
  const result=await scoutConnection({getStats:async()=>rows},{runtime:{}});
  assert.equal(result.stats.total,520);assert.equal(result.stats.rows.length,512);assert.equal(result.stats.truncated,true);
});

test('selected-pair counters preserve unsupported fields instead of inventing zeros', async()=>{
 const rows=new Map([['t',{id:'t',type:'transport',selectedCandidatePairId:'p'}],['p',{id:'p',type:'candidate-pair',timestamp:100,packetsSent:12,packetsReceived:8,bytesSent:1000,packetsDiscardedOnSend:0}]]);
 const result=await scoutConnection({getStats:async()=>rows},{runtime:{}});
 assert.equal(result.transportCounters.packetsSent,12);
 assert.equal(result.transportCounters.packetsDiscardedOnSend,0);
 assert.equal(result.transportCounters.bytesDiscardedOnSend,null);
 assert.equal(result.transportCounters.bytesReceived,null);
});

test('transport differences expose only comparable counters, never kernel loss estimates',async()=>{
 const {compareScoutTransport}=await import('../packages/phantom-core/src/connection-scout.js');
 const before={transportId:'t',pairId:'p',timestampMs:100,packetsSent:10,packetsReceived:5,packetsDiscardedOnSend:2};
 const after={...before,timestampMs:200,packetsSent:20,packetsReceived:9,packetsDiscardedOnSend:3};
 const result=compareScoutTransport(before,after);
 assert.deepEqual(result.counters.packetsSent,{status:'observed',value:10});
 assert.deepEqual(result.counters.packetsDiscardedOnSend,{status:'observed',value:1});
 assert.equal(result.counters.bytesReceived.status,'unknown');
 assert.equal(result.kernelReceiveDrops.status,'unavailable');
 assert.equal(result.socketReceiveBufferBytes.status,'unavailable');
 assert.equal(result.elapsedMs,100);
 for(const [changed,reason] of [[{pairId:'new'},'selected_route_changed'],[{transportId:null},'route_identity_unavailable'],[{timestampMs:100},'non_increasing_stats_timestamp']]){
  const comparison=compareScoutTransport(before,{...after,...changed});
  assert.equal(comparison.counters.packetsSent.reason,reason);assert.equal(comparison.counters.packetsSent.value,null);
 }
 assert.equal(compareScoutTransport(before,{...after,packetsSent:1}).counters.packetsSent.reason,'counter_reset');
 assert.equal(compareScoutTransport(null,after).counters.packetsSent.reason,'missing_observation');
});
