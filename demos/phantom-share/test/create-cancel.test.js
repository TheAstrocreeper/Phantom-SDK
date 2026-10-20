import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
const source=await readFile(new URL('../src/app.js',import.meta.url),'utf8');
function fixture(created,peers=[]){
 const events=[];let finish;
 const c={pairingCreatesSession:created,connectionGeneration:1,peers:()=>peers,
 closeInviteModal(){events.push('hide');c.pairingCreatesSession=false;},
 cleanupSession(){events.push('cleanup');c.connectionGeneration++;},
 connectionQueue:{settled:()=>new Promise(r=>finish=r)},openConnectionMenu(){events.push('menu');}};
 vm.createContext(c);vm.runInContext(source.slice(source.indexOf('function dismissPairingModal()'),source.indexOf('async function copyShareLink()')),c);
 return {c,events,finish:()=>finish()};
}
test('close cancels a newly created empty owner, including an unfinished create',()=>{
 const f=fixture(true);f.c.dismissPairingModal();assert.deepEqual(f.events,['hide','cleanup']);
 f.c.dismissPairingModal();assert.deepEqual(f.events,['hide','cleanup','hide']);
});
test('closing an existing invitation or a newly paired session preserves its network',()=>{
 for(const f of [fixture(false),fixture(true,[{hKey:'peer'}])]){f.c.dismissPairingModal();assert.deepEqual(f.events,['hide']);}
});
test('Back tears down accidental Create and waits before showing usable Create/Join',async()=>{
 const f=fixture(true),back=f.c.backFromPairingModal();assert.deepEqual(f.events,['hide','cleanup']);
 f.finish();await back;assert.deepEqual(f.events,['hide','cleanup','menu']);
});
test('late Back completion cannot open its menu over a replacement session',async()=>{
 const f=fixture(true),back=f.c.backFromPairingModal();f.c.connectionGeneration++;f.finish();await back;
 assert.deepEqual(f.events,['hide','cleanup']);
});
