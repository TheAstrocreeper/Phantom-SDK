import test from 'node:test';
import assert from 'node:assert/strict';
import {transportConfig,observeSignaling} from './live-transport.mjs';
test('live mode uses production Nostr discovery with explicit optional CF fallback',()=>{
 assert.deepEqual(transportConfig({cf:'wss://switchboard.example/'}),{mode:'cloudflare',doorways:[{type:'websocket',url:'wss://switchboard.example/'}]});
 assert.throws(()=>transportConfig({cf:'wss://user:secret@cf.example'}),/credential/);
 assert.deepEqual(transportConfig({nostr:true}),{mode:'nostr',doorways:[{type:'nostr',discover:true}]});
 assert.deepEqual(transportConfig({nostr:true,relays:'wss://relay.example,wss://relay.example',cf:'wss://cf.example/ws'}).doorways,[{type:'nostr',relays:['wss://relay.example/']},{type:'websocket',url:'wss://cf.example/ws'}]);
 assert.throws(()=>transportConfig({relays:'wss://relay.example'}),/require/);
 assert.throws(()=>transportConfig({nostr:true,relays:'ws://relay.example'}),/wss/);
 assert.throws(()=>transportConfig({nostr:true,cf:'wss://user:secret@cf.example'}),/credential/);
});
test('signaling blackout drops socket frames, restores delivery and logs ACK metadata',()=>{
 class Socket extends EventTarget {constructor(url){super();this.url=url;this.sent=[];}send(data){this.sent.push(data);}}
 const logs=[],observer=observeSignaling(Socket,(type,data)=>logs.push({type,data})),socket=new observer.WebSocket('wss://relay.example');let received=0;socket.addEventListener('message',()=>received++);
 observer.setPaused(true);socket.send('["EVENT",{}]');socket.dispatchEvent(new MessageEvent('message',{data:'["OK","id",true,"accepted"]'}));assert.equal(received,0);assert.equal(socket.sent.length,0);
 observer.setPaused(false);socket.send('["REQ","sub",{}]');socket.dispatchEvent(new MessageEvent('message',{data:'["OK","id",true,"accepted"]'}));assert.equal(received,1);assert.equal(socket.sent.length,1);
 assert.ok(logs.some(r=>r.type==='signaling.rx'&&r.data.accepted&&r.data.eventId==='id'));
 assert.deepEqual(observer.counts,{sent:1,received:1,dropped:2});
});
