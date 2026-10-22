import test from 'node:test';
import assert from 'node:assert/strict';
import {createMembershipControlRequest} from '../packages/phantom-doorways/src/membership-control-request.js';

test('peer timeout is recorded before successful fallback without signed request or remote error text',async()=>{
 const rows=[];let at=0;
 const request=createMembershipControlRequest({now:()=>at,onControlPath:e=>rows.push(e),
  getPeer:()=>({ready:()=>true,async request(){at=2000;throw Error('Membership peer request timed out: SECRET');}}),
  pairing:{async request(){at=2400;return [];}}});
 assert.deepEqual(await request('revocations',{request:{token:'SECRET'}}),[]);
 assert.deepEqual(rows.map(e=>[e.path,e.status,e.code]),[['peer','start',undefined],['peer','failure','timeout'],['pairing','start',undefined],['pairing','success',undefined]]);
 assert.equal(rows[1].elapsedMs,2000);assert.equal(rows[3].elapsedMs,400);
 assert.equal(JSON.stringify(rows).includes('SECRET'),false);
});
test('aborted peer attempt does not begin fallback',async()=>{
 const controller=new AbortController(),rows=[];let fallback=0;
 const request=createMembershipControlRequest({onControlPath:e=>rows.push(e),getPeer:()=>({ready:()=>true,async request(){controller.abort();throw Error('aborted');}}),pairing:{request(){fallback++;}}});
 await assert.rejects(request('revocations',{}, {signal:controller.signal}),/aborted/);
 assert.equal(fallback,0);assert.equal(rows.at(-1).code,'aborted');
});
test('both path failures are retained and observer exceptions do not alter request results',async()=>{
 const rows=[];
 const request=createMembershipControlRequest({onControlPath:e=>{rows.push(e);throw Error('observer');},getPeer:()=>({ready:()=>true,async request(){throw Object.assign(Error('private response'),{code:'MEMBERSHIP_REJECTED'});}}),pairing:{async request(){throw Error('Pairing channel closed');}}});
 await assert.rejects(request('revocations',{}),/closed/);
 assert.deepEqual(rows.filter(e=>e.status==='failure').map(e=>e.code),['rejected','unavailable']);
 assert.equal(JSON.stringify(rows).includes('private response'),false);
});
