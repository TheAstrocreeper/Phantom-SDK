import {test} from 'node:test';
import assert from 'node:assert/strict';
import {fitsMessageLimits,maxMessagePayload} from '../packages/phantom-core/src/message-size-guard.js';
const pc=n=>({sctp:{maxMessageSize:n}});
test('negotiated limits include wrapper and metadata bytes',()=>{
 assert.equal(fitsMessageLimits({connections:[pc(262144)],payloadBytes:262144,headerBytes:100}),true);
 assert.equal(fitsMessageLimits({connections:[pc(262144)],payloadBytes:262144,headerBytes:100,wrapperBytes:9}),false);
 assert.equal(fitsMessageLimits({connections:[pc(262144),pc(65536)],payloadBytes:65528,headerBytes:100,wrapperBytes:9}),false);
 assert.equal(fitsMessageLimits({connections:[pc(65536)],payloadBytes:1000,headerBytes:65530,wrapperBytes:9}),false);
});
test('fragment bound uses the smallest actual limit and subtracts wrapper bytes',()=>{
 assert.equal(maxMessagePayload([pc(262144),pc(65536)],9),65527);
 assert.equal(maxMessagePayload([{}],9),65527);
});
test('unknown connection size permits legacy payload but rejects larger chunks',()=>{
 assert.equal(fitsMessageLimits({connections:[{}],payloadBytes:65536,headerBytes:100}),true);
 assert.equal(fitsMessageLimits({connections:[{}],payloadBytes:65537,headerBytes:100}),false);
 assert.equal(fitsMessageLimits({connections:[],payloadBytes:1,headerBytes:1}),false);
});
