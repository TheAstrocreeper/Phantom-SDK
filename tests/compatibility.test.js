import test from 'node:test';
import assert from 'node:assert/strict';
import {migrateStorage,COMPATIBILITY_WIRE} from '../packages/phantom-core/src/compatibility.js';
import {createPhantom} from '../packages/phantom-core/src/index.js';
import {createWebSocketDoorway,createLegacyDoorway,adaptWebSocketDoorway,adaptLegacyDoorway} from '../packages/phantom-doorways/src/index.js';

function memory(entries=[]){const values=new Map(entries);return {values,getItem:k=>values.get(k)??null,setItem:(k,v)=>values.set(k,v),removeItem:k=>values.delete(k)};}

test('stored identities migrate without overwriting current values or reviving consumed session records',()=>{
 const storage=memory([['six7_keystore_v1','saved identity'],['six7_tofu_hkey','old peer'],['phantom_tofu_hkey','current peer'],['six7_refresh_relationships_v1','saved session']]);
 migrateStorage(storage);
 assert.equal(storage.getItem('phantom_keystore_v1'),'saved identity');
 assert.equal(storage.getItem('phantom_tofu_hkey'),'current peer');
 assert.equal(storage.getItem('six7_keystore_v1'),'saved identity');
 storage.removeItem('phantom_refresh_relationships_v1');migrateStorage(storage);
 assert.equal(storage.getItem('phantom_refresh_relationships_v1'),null);
});

test('migration retries a failed write instead of marking incomplete state as migrated',()=>{
 const storage=memory([['six7_keystore_v1','saved identity']]);let blocked=true;
 const set=storage.setItem;storage.setItem=(k,v)=>{if(blocked)throw Error('unavailable');set(k,v);};
 migrateStorage(storage);assert.equal(storage.getItem('phantom_storage_migration_v1'),null);
 blocked=false;migrateStorage(storage);assert.equal(storage.getItem('phantom_keystore_v1'),'saved identity');
});

test('the public WebSocket API uses Phantom terminology and retains earlier API aliases',()=>{
 assert.equal(createWebSocketDoorway,createLegacyDoorway);assert.equal(adaptWebSocketDoorway,adaptLegacyDoorway);
 assert.equal(createWebSocketDoorway({url:'wss://relay.example',WebSocket:class{}}).protocol,'phantom-concierge-v1');
 assert.deepEqual(COMPATIBILITY_WIRE,{chatChannel:'six7',systemChannel:'six7-sys'});
});

for(const protocol of ['phantom-concierge-v1','six7-concierge-v1'])test(`core accepts ${protocol} and normalizes earlier session commands`,()=>{
 const core=createPhantom({doorway:{protocol,url:'ws://relay.example/',WebSocket:class{}}});
 const events=[];core.onEvent(e=>events.push(e));
 core.command('SIX7_SESSION_SUSPEND',{});
 assert.ok(events.some(e=>e.type==='PHANTOM_SESSION_SUSPENDED'));
 core.dispose();
});
