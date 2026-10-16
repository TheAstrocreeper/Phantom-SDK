import test from 'node:test';
import assert from 'node:assert/strict';
import QRCode from '../swarm/node_modules/qrcode/lib/index.js';
import {createJournal} from './journal.js';
test('journal preserves failure evidence across reload without requiring SDK state',()=>{
 const data=new Map(),storage={getItem:k=>data.get(k),setItem:(k,v)=>data.set(k,v),removeItem:k=>data.delete(k)};
 const one=createJournal(storage,'tab');assert.equal(one.save({events:[{type:'test.recoveryFailed'}]}),true);
 const two=createJournal(storage,'tab');assert.equal(two.previous.events[0].type,'test.recoveryFailed');two.clear();assert.equal(data.size,0);
});
test('journal bounds storage and exposes quota errors',()=>{
 const j=createJournal({getItem:()=>null,setItem:()=>{throw Error('quota exceeded');}},'tab');
 assert.equal(j.save({events:[]}),false);assert.match(j.error,/quota/);
 assert.equal(j.save({large:'x'.repeat(350001)}),false);assert.match(j.error,/exceeded/);
});
test('QR encodes representative HTTPS companion link entirely locally',()=>{
 const invite='phantom://v3/'+'a'.repeat(64)+'?doorway='+encodeURIComponent(JSON.stringify({type:'test-websocket',url:'wss://example.invalid/doorway'}))+'&admin='+'b'.repeat(64);
 const url='https://example.invalid/#'+new URLSearchParams({phantom:JSON.stringify({invite,doorway:'wss://example.invalid/doorway'})});
 const qr=QRCode.create(url,{errorCorrectionLevel:'L'});assert.ok(qr.modules.size>20);
});
