import test from 'node:test';
import assert from 'node:assert/strict';
import {openMembershipPeerChannel} from '../packages/phantom-doorways/src/membership-peer-channel.js';
const keys=['a'.repeat(64),'b'.repeat(64),'c'.repeat(64)];
function fixture(){const listeners=keys.map(()=>new Set()),packets=[];const networks=keys.map((key,i)=>({getPeerConnections:()=>keys.filter(k=>k!==key).map(k=>({peerId:k.slice(0,16),state:'ready'})),on(type,fn){listeners[i].add(fn);return()=>listeners[i].delete(fn);},send(to,text){packets.push({to,text,from:key.slice(0,16)});const j=keys.findIndex(k=>k.slice(0,16)===to);queueMicrotask(()=>{for(const fn of listeners[j])fn({from:key.slice(0,16),text});});}}));return {networks,packets,listeners};}
test('membership peer RPC pins responses to its owner, supports abort, and settles on close',async()=>{
 const f=fixture();let release;
 const host=openMembershipPeerChannel({network:f.networks[0],onRequest:()=>new Promise(r=>release=r)});
 const client=openMembershipPeerChannel({network:f.networks[1],remotePublicKey:keys[0]});
 try{
  const response=client.request('status',{request:{peerPublicKey:keys[1]}});
  await new Promise(r=>setImmediate(r));const id=JSON.parse(f.packets[0].text).id;
  for(const fn of f.listeners[1])fn({from:keys[2].slice(0,16),text:JSON.stringify({app:'phantom-membership/1',kind:'response',id,data:'forged'})});
  release('owner');assert.equal(await response,'owner');
  const controller=new AbortController();const aborted=client.request('status',{request:{peerPublicKey:keys[1]}},{signal:controller.signal});controller.abort();await assert.rejects(aborted,/aborted/);
  const closed=client.request('status',{request:{peerPublicKey:keys[1]}});client.close();await assert.rejects(closed,/closed/);
 }finally{client.close();host.close();}
});
