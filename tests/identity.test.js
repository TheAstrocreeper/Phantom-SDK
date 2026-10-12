import test from 'node:test';import assert from 'node:assert/strict';
import {createPhantom,createPeerIdentity} from '../packages/phantom-core/src/index.js';
import {createLegacyDoorway} from '../packages/phantom-doorways/src/index.js';
class Socket{constructor(){this.readyState=0;}close(){this.readyState=3;}send(){}}
const setup=()=>createPhantom({doorway:createLegacyDoorway({url:'wss://example.test',WebSocket:Socket})});
test('identical display names have independent generated network identities',async()=>{
 const a=setup(),b=setup();try{const x=await a.join({roomId:'game',name:'Player One'}),y=await b.join({roomId:'game',name:'Player One'});assert.equal(x.handle,'Player One');assert.equal(y.handle,'Player One');assert.notEqual(x.hex,y.hex);assert.notEqual(x.hKey,y.hKey);}finally{a.dispose();b.dispose();}
});
test('saved identity survives a new instance and different display name',async()=>{
 const a=setup(),b=setup();try{const first=await a.join({roomId:'game',name:'Astro'});const saved=JSON.parse(JSON.stringify(a.exportIdentity()));a.dispose();const second=await b.join({roomId:'game',name:'New gamer tag',identity:saved});assert.equal(first.hex,second.hex);assert.equal(second.handle,'New gamer tag');}finally{a.dispose();b.dispose();}
});
test('invalid configuration fails before joining; missing secure randomness fails closed',async()=>{
 const a=setup();try{await assert.rejects(a.join({roomId:'game',name:'bad\nname'}));await assert.rejects(a.join({roomId:'game',name:'Astro',identity:{version:1,id:'short'}}));await a.join({roomId:'game',name:'Valid'});}finally{a.dispose();}assert.throws(()=>createPeerIdentity({crypto:{}}),/randomness/);
});
