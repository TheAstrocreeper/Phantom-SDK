import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
const source=await readFile(new URL('../src/app.js',import.meta.url),'utf8');
function fixture(){const els=new Map();const byId=id=>{if(!els.has(id))els.set(id,{value:'invitation',textContent:'',setAttribute(k,v){this[k]=v;}});return els.get(id);};let finish,calls=0,closes=0;const c={connectionQueue:{phase:"closed",busy:false},byId,connecting:false,network:null,codeFromInput:v=>v,connect:()=>{calls++;c.connecting=true;return new Promise(r=>finish=r);},closeInviteModal(){closes++;},showError(){},cleanupSession(){},openPairingModal:async()=>{}};vm.createContext(c);vm.runInContext(source.slice(source.indexOf('byId("join-network").onclick ='),source.indexOf('byId("profile-action").onclick')),c);return {c,byId,calls:()=>calls,closes:()=>closes,finish:v=>finish(v)};}
test('Join ignores duplicate taps and closes only on successful connection',async()=>{const f=fixture();const p=f.byId('join-network').onclick();await f.byId('join-network').onclick();assert.equal(f.calls(),1);f.finish(true);await p;assert.equal(f.closes(),1);});
test('invalid entry reports inline without erasing input or closing dialog',async()=>{const f=fixture();f.c.codeFromInput=()=>{throw Error('Invalid invitation');};await f.byId('join-network').onclick();assert.equal(f.calls(),0);assert.equal(f.byId('paste-invite').value,'invitation');assert.equal(f.byId('join-status').textContent,'Invalid invitation');assert.equal(f.closes(),0);});
test('busy presentation locks entry and enables cancellation; idle restores Join',()=>{const f=fixture();f.c.pairingIntent='connect';const start=source.indexOf('function renderAvailability() {')+'function renderAvailability() {'.length,end=source.indexOf('  const nameLocked',start);const paint=()=>vm.runInContext("{"+source.slice(start,end)+"}",f.c);f.c.connecting=true;paint();assert.equal(f.byId('join-network').disabled,true);assert.equal(f.byId('join-network').textContent,'Joining…');assert.equal(f.byId('paste-invite').disabled,true);assert.equal(f.byId('cancel-join').hidden,false);f.c.connecting=false;paint();assert.equal(f.byId('join-network').disabled,false);assert.equal(f.byId('join-network').textContent,'Join session');});

test('ending keeps Join disabled and ignores submitted input until cleanup completes',async()=>{
 const f=fixture();f.c.connectionQueue={phase:'ending',busy:true};
 await f.byId('join-network').onclick();assert.equal(f.calls(),0);
 const start=source.indexOf('function renderAvailability() {')+'function renderAvailability() {'.length;
 vm.runInContext(source.slice(start,source.indexOf('  const nameLocked',start)),f.c);
 assert.equal(f.byId('join-network').disabled,true);assert.equal(f.byId('choose-create').disabled,true);
 assert.equal(f.byId('scan-qr').disabled,true);
});
