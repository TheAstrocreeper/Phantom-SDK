import test from 'node:test';
import assert from 'node:assert/strict';
import {createSysOutageClock} from '../packages/phantom-core/src/sys-outage-clock.js';
test('outage credit requires confirmed failure and preserves the first disconnected observation',()=>{
  let time=0;const clock=createSysOutageClock({now:()=>time});
  const pc={connectionState:'disconnected',iceConnectionState:'disconnected'};
  clock.observe(pc);time=7000;clock.observe(pc);
  assert.equal(clock.confirmedStart(pc),null);
  pc.connectionState=pc.iceConnectionState='failed';clock.observe(pc);
  assert.equal(clock.confirmedStart(pc),0);
});
test('restoration clears outage credit and subsequent failure starts fresh',()=>{
  let time=100;const clock=createSysOutageClock({now:()=>time});
  const pc={connectionState:'disconnected',iceConnectionState:'disconnected'};clock.observe(pc);
  pc.connectionState='connected';clock.observe(pc);
  pc.connectionState=pc.iceConnectionState='failed';assert.equal(clock.confirmedStart(pc),null);
  time=900;pc.connectionState=pc.iceConnectionState='disconnected';clock.observe(pc);
  pc.connectionState='failed';assert.equal(clock.confirmedStart(pc),900);
});
test('stale transports cannot transfer evidence to replacements or credit closure alone',()=>{
  const clock=createSysOutageClock({now:()=>10});
  const old={connectionState:'disconnected'};clock.observe(old);old.connectionState='failed';
  const replacement={connectionState:'failed'};
  assert.equal(clock.confirmedStart(replacement),null);
  old.connectionState='closed';old.iceConnectionState='failed';assert.equal(clock.confirmedStart(old),null);
  assert.equal(clock.confirmedStart(undefined),null);
});
