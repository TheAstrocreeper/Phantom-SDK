import test from 'node:test';
import assert from 'node:assert/strict';
import {WebSocket} from 'ws';
import {startServer} from './server.mjs';
const delay=ms=>new Promise(r=>setTimeout(r,ms));
async function wait(check){for(let i=0;i<100;i++){if(check())return;await delay(10);}throw Error('Relay assertion deadline');}
test('isolated sockets discover, route without alteration, and observe departure',async()=>{
 const events=[],server=await startServer('/* fixture */',(type,data)=>events.push({type,data})),clients=[];
 try{
  async function open(id){const messages=[];const socket=new WebSocket(server.url.replace('http:','ws:')+'/ws?v=room&to='+id);clients.push(socket);socket.on('message',m=>messages.push(JSON.parse(m)));await new Promise((resolve,reject)=>{socket.once('open',resolve);socket.once('error',reject);});socket.send(JSON.stringify({type:'concierge_connect',videoId:'room',from:id,handle:id,bootId:crypto.randomUUID(),t:Date.now()}));await wait(()=>messages.length);return {socket,messages,id};}
  const a=await open('1111111111111111'),b=await open('2222222222222222');await wait(()=>a.messages.at(-1)?.list?.length===2);
  const packet={type:'rtc_offer',videoId:'room',from:a.id,to:b.id,sdp:{type:'offer',sdp:'preserve this'},sysCarrier:true};a.socket.send(JSON.stringify(packet));await wait(()=>b.messages.some(m=>m.type==='rtc_offer'));assert.deepEqual(b.messages.find(m=>m.type==='rtc_offer'),packet);
  a.socket.send(JSON.stringify({...packet,from:b.id,sdp:{sdp:'spoof'}}));await delay(30);assert.equal(b.messages.filter(m=>m.type==='rtc_offer').length,1);
  server.setSignalingPaused(true);a.socket.send(JSON.stringify(packet));await wait(()=>events.some(e=>e.type==='relay.dropped'));assert.equal(b.messages.filter(m=>m.type==='rtc_offer').length,1);assert.equal(a.socket.readyState,1);assert.equal(b.socket.readyState,1);
  server.setSignalingPaused(false);a.socket.send(JSON.stringify(packet));await wait(()=>b.messages.filter(m=>m.type==='rtc_offer').length===2);assert.deepEqual(b.messages.filter(m=>m.type==='rtc_offer').at(-1),packet);
  a.socket.close();await wait(()=>b.messages.at(-1)?.list?.length===1);assert.equal(b.messages.at(-1).syscon1,b.id);assert.ok(events.some(e=>e.type==='relay.leave'));
 }finally{for(const c of clients)c.terminate();await server.close();}
});
