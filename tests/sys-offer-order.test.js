import test from 'node:test';
import assert from 'node:assert/strict';
import {createSysOfferOrder} from '../packages/phantom-core/src/sys-offer-order.js';
const peer='1'.repeat(16),target='2'.repeat(16),boot='sender-boot';
const id=n=>`${boot}:${target}:${n}`;
test('older SYS offer cannot replace the latest attempt; duplicates and newer attempts remain valid',()=>{const q=createSysOfferOrder();q.accept(peer,boot,target,id(3));assert.equal(q.allows(peer,boot,target,id(2)),false);assert.equal(q.allows(peer,boot,target,id(3)),true);assert.equal(q.allows(peer,boot,target,id(4)),true);q.accept(peer,boot,target,id(2));assert.equal(q.allows(peer,boot,target,id(2)),false);});
test('attempt counters are scoped to sender boot, recipient and identity',()=>{const q=createSysOfferOrder();q.accept(peer,boot,target,id(9));assert.equal(q.allows(peer,'new-boot',target,`new-boot:${target}:1`),true);assert.equal(q.allows('3'.repeat(16),boot,target,id(1)),true);assert.equal(q.allows(peer,boot,'4'.repeat(16),`${boot}:${'4'.repeat(16)}:1`),true);});
test('opaque legacy identifiers stay compatible; malformed scope cannot advance a counter',()=>{const q=createSysOfferOrder();q.accept(peer,boot,target,'wrong:'+target+':999');assert.equal(q.allows(peer,boot,target,id(1)),true);assert.equal(q.allows(peer,boot,target,'legacy-attempt'),true);q.accept(peer,boot,target,id(3));q.clear();assert.equal(q.allows(peer,boot,target,id(1)),true);});
