import {test} from 'node:test';
import assert from 'node:assert/strict';
import {scoutConnection} from '../packages/phantom-core/src/connection-scout.js';

test('scout returns only explicitly selected connection facts, without addresses', async () => {
  const rows = new Map([['t',{type:'transport',selectedCandidatePairId:'p'}],['p',{type:'candidate-pair',localCandidateId:'l',remoteCandidateId:'r',currentRoundTripTime:0.12,bytesSent:17,bytesReceived:8}],['l',{candidateType:'host',protocol:'udp',address:'192.168.1.7'}],['r',{candidateType:'srflx',address:'203.0.113.5'}]]);
  const pc={connectionState:'connected',iceConnectionState:'connected',sctp:{state:'connected',maxMessageSize:65536,maxChannels:100,transport:{state:'connected'}},getStats:async()=>rows};
  const channel={readyState:'open',ordered:true,bufferedAmount:11,bufferedAmountLowThreshold:4,maxRetransmits:null,maxPacketLifeTime:null};
  const result=await scoutConnection(pc,{channel,runtime:{}});
  assert.deepEqual(result.messageLimit,{status:'known',bytes:65536});
  assert.deepEqual(result.route,{localType:'host',remoteType:'srflx',protocol:'udp',rttSeconds:0.12,bytesSent:17,bytesReceived:8});
  assert.equal(result.channel.bufferedBytes,11);
  assert.equal(result.statsStatus,'available');
  assert.doesNotMatch(JSON.stringify(result),/192\.168|203\.0\.113/);
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
