import test from 'node:test';
import assert from 'node:assert/strict';
import {Socket,RTC} from './fixtures/native-network.js';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createAdminIdentity} from '../packages/phantom-core/src/index.js';
const runtime={crypto:globalThis.crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval};
const storage=()=>{const data=new Map();return {getItem:k=>data.get(k)??null,setItem:(k,v)=>data.set(k,v)};};
async function until(fn){const end=Date.now()+8000;while(!fn()){if(Date.now()>end)throw Error('Admin gate test timed out');await new Promise(r=>setTimeout(r,5));}}
test('actual roster duties wait for admin without distribution enabled; graceful handover rechecks',async()=>{
 const admin=await createAdminIdentity(),peers=[],checks=[],release=[];
 function sdk(i){return createBrowserPhantom({adminPublicKey:admin.publicKey,doorways:[{type:'websocket',url:'wss://cf.fixture/'}],WebSocket:Socket,runtime,storage:storage(),readAdminOrders:async context=>{checks.push({i,...context});return new Promise(r=>release[i]=r);}});}
 try{
 const a=await sdk(0).create({name:'A'});peers.push(a);
 await until(()=>release[0]);assert.equal(a.getAdminState().phase,'checking');
 assert.equal(a.snapshot().concierge.membershipAudit.topology.committedCount,0);
 release[0]([]);await until(()=>a.getAdminState().phase==='approved');
 const b=await sdk(1).join(a.invite,{name:'B'});peers.push(b);
 const c=await sdk(2).join(a.invite,{name:'C'});peers.push(c);
 assert.equal(checks.filter(x=>x.i===0).length,1);assert.equal(checks.filter(x=>x.i===1).length,0);
 a.close();await until(()=>release[1]);assert.equal(b.getAdminState().phase,'checking');
 assert.equal(b.snapshot().concierge.membershipAudit.topology.committedOne,a.getState().peerId.slice(0,8));
 release[1]([]);await until(()=>b.getState().role==='syscon1'&&b.getState().state==='admitted'&&b.getAdminState().phase==='approved');
 assert.equal(b.getAdminState().phase,'approved');assert.equal(checks.filter(x=>x.i===1).length,1);
 }finally{for(const p of peers)p.close();}
});
