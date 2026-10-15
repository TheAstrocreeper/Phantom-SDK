import test from 'node:test';
import assert from 'node:assert/strict';
import {createDeferredSysOffers} from '../packages/phantom-core/src/deferred-sys-offers.js';
import {acceptsSuccessorHandshake} from '../packages/phantom-core/src/successor-handshake.js';
const peer='1111111111111111',other='2222222222222222',offer={type:'offer',sdp:'valid SDP'};
const meta=attempt=>({bootId:'boot',sysAttemptId:attempt,room:'room',pipe:'sys'});
const candidate={candidate:'candidate:1 1 udp 1 192.0.2.1 1234 typ host',sdpMid:'0',sdpMLineIndex:0};
function fixture(options={}){let time=0,id=0;const timers=new Map(),events=[];const q=createDeferredSysOffers({now:()=>time,setTimeout:(fn,ms)=>{const key=++id;timers.set(key,{fn,at:time+ms});return key;},clearTimeout:key=>timers.delete(key),emit:e=>events.push(e),...options});return {q,events,timers,advance(ms){time+=ms;for(const [key,t]of [...timers])if(t.at<=time){timers.delete(key);t.fn();}}};}
test('leadership commit drains matching offer and ICE immediately without advancing time',()=>{
 const {q,timers,events}=fixture();assert.ok(q.hold(peer,offer,meta('a')));assert.equal(q.ice(peer,candidate,meta('a')),'held');
 const accepted=[];let leader=false;const hooks={isLeader:()=>leader,eligible:()=>true,accept:r=>accepted.push(r)};
 q.resume(hooks);assert.equal(accepted.length,0);leader=true;q.resume(hooks);
 assert.equal(accepted.length,1);assert.deepEqual(accepted[0].ice[0].candidate,{...candidate,usernameFragment:null});assert.equal(q.snapshot().length,0);assert.equal(timers.size,0);assert.equal(events.at(-1).reason,'leadership_committed');
});
test('superseding an offer discards its ICE and cannot revive the retired attempt',()=>{
 const {q}=fixture();q.hold(peer,offer,meta('a'));q.ice(peer,candidate,meta('a'));assert.ok(q.hold(peer,offer,meta('b')));
 assert.equal(q.ice(peer,candidate,meta('a')),'mismatch');assert.equal(q.hold(peer,offer,meta('a')),false);
 const accepted=[];q.resume({isLeader:()=>true,eligible:()=>true,accept:r=>accepted.push(r)});assert.equal(accepted[0].meta.sysAttemptId,'b');assert.equal(accepted[0].ice.length,0);
});
test('permission/incarnation changes are rechecked and expiration/cleanup release retained signaling',()=>{
 const f=fixture();f.q.hold(peer,offer,meta('a'));f.q.resume({isLeader:()=>true,eligible:()=>false,accept:()=>assert.fail('revoked request resumed')});assert.equal(f.events.at(-1).reason,'eligibility_changed');
 f.q.hold(peer,offer,meta('b'));f.advance(15000);assert.equal(f.q.snapshot().length,0);assert.equal(f.events.at(-1).reason,'expired');
 f.q.hold(peer,offer,meta('c'));f.q.clear();assert.equal(f.timers.size,0);
});
test('retention is bounded and duplicate candidates do not consume extra capacity',()=>{
 const {q}=fixture({maxPeers:1,maxCandidates:1,maxSdpBytes:32});assert.equal(q.hold(peer,{type:'offer',sdp:'x'.repeat(33)},meta('a')),false);
 assert.ok(q.hold(peer,offer,meta('a')));assert.equal(q.hold(other,offer,meta('a')),false);
 assert.equal(q.ice(peer,candidate,meta('a')),'held');assert.equal(q.ice(peer,candidate,meta('a')),'duplicate');assert.equal(q.ice(peer,{...candidate,candidate:'another'},meta('a')),'full');
 assert.equal(q.ice(peer,{...candidate,candidate:'x'.repeat(2049)},meta('a')),'invalid');
});


test('retention keeps only RTC signaling fields, excluding arbitrary attached payloads',()=>{
 const {q}=fixture();q.hold(peer,{...offer,extra:'not SDP'},{...meta('a'),extra:'not signaling'});
 let row;q.resume({isLeader:()=>true,eligible:()=>true,accept:r=>{row=r;}});
 assert.deepEqual(row.offer,offer);assert.equal(row.meta.extra,undefined);
});

test('early successor offer and ICE resume on leader pipe loss without a timeout retry',()=>{
 const {q,timers}=fixture();const leader='3333333333333333';
 const base={syscon1:leader,syscon2:peer,epoch:4,rosterSeq:8,roster:[{hKey:leader},{hKey:peer},{hKey:other}]};
 const proof={room:'room',from:peer,to:other,bootId:'boot',baseOne:leader,baseTwo:peer,baseEpoch:4,baseSeq:8,baseRoster:base.roster,terminalLeader:leader};
 let open=true;const valid=row=>acceptsSuccessorHandshake({proof:row.meta.successorProof,base,sender:peer,self:other,room:'room',bootId:'boot',leaderPipeOpen:false});
 // Before this retention path, the same offer fails the actual acceptance guard.
 assert.equal(acceptsSuccessorHandshake({proof,base,sender:peer,self:other,room:'room',bootId:'boot',leaderPipeOpen:open}),false);
 assert.ok(q.hold(peer,offer,{...meta('first'),successorProof:proof}));
 assert.equal(q.ice(peer,candidate,meta('first')),'held');
 const accepted=[];const hooks={isLeader:()=>true,eligible:valid,waiting:()=>open,accept:row=>accepted.push(row)};
 q.resume(hooks);assert.equal(accepted.length,0);assert.equal(q.snapshot().length,1);
 open=false;q.resume(hooks);assert.equal(accepted.length,1);assert.equal(accepted[0].meta.sysAttemptId,'first');
 assert.equal(accepted[0].ice.length,1);assert.equal(timers.size,0);
 assert.ok(acceptsSuccessorHandshake({proof:accepted[0].meta.successorProof,base,sender:peer,self:other,room:'room',bootId:'boot',leaderPipeOpen:open}));
});
test('changed authority discards a waiting successor proof; oversized proofs cannot enter retention',()=>{
 const {q}=fixture({maxSdpBytes:32});assert.equal(q.hold(peer,offer,{...meta('a'),successorProof:{data:'x'.repeat(33)}}),false);
 const f=fixture();f.q.hold(peer,offer,{...meta('a'),successorProof:{baseEpoch:1}});
 f.q.resume({isLeader:()=>true,eligible:()=>false,waiting:()=>true,accept:()=>assert.fail('stale authority resumed')});
 assert.equal(f.q.snapshot().length,0);
});

test('handoff and successor offers share capacity, supersession and ICE ownership',()=>{
 const {q,timers}=fixture({maxPeers:1});
 q.hold(peer,offer,meta('a'),'successor');q.ice(peer,candidate,meta('a'));
 assert.ok(q.hold(peer,offer,meta('b'),'handoff'));
 assert.equal(q.snapshot().length,1);assert.equal(q.snapshot()[0].kind,'handoff');assert.equal(timers.size,1);
 assert.equal(q.hold(peer,offer,meta('a'),'successor'),false);
 assert.equal(q.ice(peer,candidate,meta('a')),'mismatch');
 assert.equal(q.ice(peer,candidate,meta('b')),'held');
 assert.equal(q.hold(other,offer,meta('c'),'successor'),false);
 let accepted;q.resume({isLeader:()=>true,eligible:()=>true,accept:row=>{accepted=row;}});
 assert.equal(accepted.kind,'handoff');assert.equal(accepted.ice.length,1);assert.equal(timers.size,0);
});
