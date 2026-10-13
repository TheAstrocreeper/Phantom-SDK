import test from 'node:test';
import assert from 'node:assert/strict';
import {runDistributionTest} from './fixtures/distribution-scenario.js';
import {Socket,RTC} from './fixtures/native-network.js';
const runtime={crypto:globalThis.crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval};
test('browser runner exercises SDK distribution, handover, revoke and retirement',async()=>{
 const report=await runDistributionTest({endpoint:'wss://cf.fixture/',useNostr:false,WebSocket:Socket,runtime,deadlineMs:12000});
 assert.equal(report.passed,true,JSON.stringify(report));assert.equal(report.steps.length,5);
 assert.ok(report.metrics.adminChecks>=2);assert.ok(report.metrics.jobs>0);
 assert.ok(report.metrics.peakConnections<=3);assert.ok(JSON.stringify(report).length<10000);
});
test('same browser scenario supports mixed doors and same-identity return with a surviving bridge',async()=>{
 const report=await runDistributionTest({endpoint:'wss://cf.fixture/',relays:['wss://nostr.fixture/'],WebSocket:Socket,runtime,deadlineMs:12000});
 assert.equal(report.passed,true,JSON.stringify(report));assert.equal(report.steps.length,6);
 assert.ok(report.steps.some(s=>s.label.includes('same identity')&&s.status==='pass'));
});
test('default Nostr-only browser scenario requires no developer switchboard',async()=>{
 const report=await runDistributionTest({relays:['wss://nostr.fixture/'],WebSocket:Socket,runtime,deadlineMs:12000});
 assert.equal(report.passed,true,JSON.stringify(report));assert.equal(report.steps.length,5);
});

test('100 MiB survives loss of sole complete seed and revokes a moving second file',async(t)=>{
 const report=await runDistributionTest({large:true,endpoint:'wss://cf.fixture/',useNostr:false,WebSocket:Socket,runtime,deadlineMs:15000,transferDeadlineMs:180000});
 assert.equal(report.passed,true,JSON.stringify(report));
 const departure=report.checks.find(c=>c.sourceDeparture).sourceDeparture;
 t.diagnostic(JSON.stringify(report));
 assert.equal(departure.collectivePieces,1600);assert.ok(departure.receiverPieces.every(n=>n<1600));
 assert.equal(report.checks.find(c=>c.verifiedReceivers).bytesEach,104857600);
 assert.ok(report.metrics.peakConnections<=3);
 assert.ok(report.checks.find(c=>c.revocationPiecesReceived).revocationPiecesReceived.some(n=>n>=16));
});
