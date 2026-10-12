import test from 'node:test';
import assert from 'node:assert/strict';
import {createBoard,apply,history} from '../examples/shared-canvas/src/model.js';
import {shareLink,readLink} from '../examples/shared-canvas/src/link.js';
import {encodeInvite} from '../packages/phantom-core/src/invite.js';
const peer='a'.repeat(16),other='b'.repeat(16);
const stroke=(id='s',epoch={id:'initial',clock:0,author:''})=>({type:'stroke',epoch,id,author:peer,clock:1,color:'#7556f5',width:5,points:[[.1,.2],[.3,.4]]});
test('duplicate delivery, clear races, and history converge',()=>{
 const a=createBoard(),b=createBoard();const s=stroke();assert.equal(apply(a,s),true);assert.equal(apply(a,s),false);
 const clear={type:'clear',epoch:{id:'c',clock:2,author:other}};apply(a,clear);assert.equal(apply(a,s),false);
 apply(a,stroke('new',clear.epoch));for(const op of history(a).reverse())apply(b,op);
 assert.deepEqual(history(a),history(b));
});
test('malformed coordinates and unbounded operation batches are not accepted',()=>{
 const b=createBoard();assert.equal(apply(b,{...stroke(),points:[[Infinity,0]]}),false);
 assert.equal(apply(b,{...stroke(),points:Array(49).fill([0,0])}),false);assert.equal(b.strokes.size,0);
});
test('QR/share link round-trips the actual SDK invitation',async()=>{
 const invite=encodeInvite({networkId:'a'.repeat(64),relays:['wss://relay.example']});
 const url=await shareLink(invite,'https://example.com/canvas');assert.equal(await readLink(new URL(url).search),invite);
 assert.equal(await readLink('#'),null);
 assert.equal(new URL(url).hash,'');
 assert.ok(new URL(url).searchParams.has('p'));
 assert.equal(await readLink('', '#invite='+encodeURIComponent(invite)),invite);
 assert.equal(await readLink('?other=value','#invite='+encodeURIComponent(invite)),invite);
});
