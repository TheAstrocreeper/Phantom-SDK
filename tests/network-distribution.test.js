import test from 'node:test';
import assert from 'node:assert/strict';
import {Socket,RTC,rtc,setDistributionFault} from './fixtures/native-network.js';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createAdminIdentity,createAdminController} from '../packages/phantom-core/src/index.js';
const runtime={crypto:globalThis.crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval};
function storage(){const data=new Map();return {getItem:k=>data.get(k)??null,setItem:(k,v)=>data.set(k,v)};}
async function until(fn,label,timeout=15000){const deadline=Date.now()+timeout;while(!fn()){if(Date.now()>deadline)throw Error(label+' timed out');await new Promise(r=>setTimeout(r,10));}}
async function setup(count,{isolated=false,retireGraceMs}={}){const admin=await createAdminIdentity(),controller=createAdminController({identity:admin}),peers=[],events=[];let checks=0;
 for(let i=0;i<count;i++){
  const peer={name:'Peer '+i,completed:new Map(),events:[]};
  const sdk=createBrowserPhantom({adminPublicKey:admin.publicKey,adminIdentity:i===0&&!isolated?admin:undefined,doorways:[{type:'websocket',url:'wss://cf.fixture/'}],storage:storage(),WebSocket:Socket,runtime,retireGraceMs,distribution:{maxConnections:3,batchSize:4,intervalMs:25,jobTimeoutMs:2000,requestTimeoutMs:250},readAdminOrders:async({networkId})=>{checks++;return isolated?controller.getOrders(networkId):[];}});
  const options={name:peer.name,timeoutMs:15000};peer.network=i===0?await sdk.create(options):await sdk.join(peers[0].network.invite,options);
  peer.network.on('distributionComplete',e=>peer.completed.set(e.id,e.blob));
  for(const type of ['distributionJob','distributionError','distributionAdminChecked','distributionConnectionOpened','distributionConnectionClosed'])peer.network.on(type,e=>{peer.events.push({type,...e});events.push({peer:i,type,...e});});
  peers.push(peer);
 }
 await until(()=>peers.every(p=>p.network.getState().state==='admitted'),'all admitted');
 return {admin,controller,peers,events,get checks(){return checks;},close(){for(const p of peers)p.network.close();}};
}
test('automatic distribution uses temporary connections without private invitations',async()=>{
 const run=await setup(6);try{
 const payload=Uint8Array.from({length:48*1024},(_,i)=>i%251),file=new Blob([payload]);
 const before=run.peers.map(p=>JSON.stringify(p.network.snapshot().concierge.membershipAudit.topology));
 const manifest=await run.peers[0].network.distributeFile(file,{chunkSize:4096,distributionId:'release-1'});
 await until(()=>run.peers.slice(1).every(p=>p.completed.has(manifest.id)),'distribution');
 for(const p of run.peers.slice(1))assert.deepEqual(new Uint8Array(await p.completed.get(manifest.id).arrayBuffer()),payload);
 assert.ok(run.events.some(e=>e.type==='distributionJob'&&e.source!==run.peers[0].network.getState().peerId),'receivers become sources');
 await until(()=>run.peers.every(p=>p.network.getDistributionState().connections.length===0),'temporary connections released');
 for(let i=0;i<run.peers.length;i++){
  assert.equal(JSON.stringify(run.peers[i].network.snapshot().concierge.membershipAudit.topology),before[i]);
  assert.ok(!(run.peers[i].network.snapshot().chatPairs??[]).some(p=>p.chatState==='ready'));
 }
 assert.ok(run.checks>0);assert.equal(run.events.filter(e=>e.type==='distributionError').length,0,JSON.stringify(run.events.filter(e=>e.type==='distributionError')));
 }finally{run.close();}
});


test('50 actual SDK peers distribute pieces with bounded connections and no chat invitations',async()=>{
 const run=await setup(50);let peak=0;const monitor=setInterval(()=>{peak=Math.max(peak,...run.peers.map(p=>p.network.getDistributionState().connections.length));},5);
 try{
  const before=run.peers.map(p=>JSON.stringify(p.network.snapshot().concierge.membershipAudit.topology));
  const payload=Uint8Array.from({length:128*1024},(_,i)=>(i*7)%251);
  const manifest=await run.peers[0].network.distributeFile(new Blob([payload]),{chunkSize:4096,distributionId:'fifty-peer-release'});
  await until(()=>run.peers.slice(1).every(p=>p.completed.has(manifest.id)),'50 deliveries',30000);
  for(const p of run.peers.slice(1))assert.deepEqual(new Uint8Array(await p.completed.get(manifest.id).arrayBuffer()),payload);
  await until(()=>run.peers.every(p=>p.network.getDistributionState().connections.length===0),'all transfer links released');
  for(let i=0;i<50;i++){assert.equal(JSON.stringify(run.peers[i].network.snapshot().concierge.membershipAudit.topology),before[i]);assert.ok(!(run.peers[i].network.snapshot().chatPairs??[]).some(p=>p.chatState==='ready'));}
  assert.ok(peak>0&&peak<=3);assert.ok(run.events.some(e=>e.type==='distributionJob'&&e.source!==run.peers[0].network.getState().peerId));
  console.log(JSON.stringify({scenario:'50-peer automatic distribution',peers:50,receivers:49,bytesPerFile:payload.length,verifiedDeliveries:49,peakConnectionsPerPeer:peak,jobs:run.events.filter(e=>e.type==='distributionJob').length,privateChatConnections:0,rosterUnchanged:true}));
 }finally{clearInterval(monitor);run.close();}
});

test('distribution survives original holder and SysCon 1 leaving; successor checks admin',async()=>{
 const run=await setup(8);try{
  const payload=Uint8Array.from({length:512*1024},(_,i)=>(i*13)%251);
  const manifest=await run.peers[0].network.distributeFile(new Blob([payload]),{chunkSize:4096,distributionId:'succession-release'});
  await until(()=>run.peers.slice(1).some(p=>p.completed.has(manifest.id)),'a replacement full holder',30000);
  assert.ok(run.peers.slice(1).some(p=>!p.completed.has(manifest.id)),'distribution is still active at leader departure');
  const before=run.checks;run.peers[0].network.close();
  await until(()=>run.peers.slice(1).some(p=>p.network.getState().role==='syscon1'),'successor leader',30000);
  await until(()=>run.peers.slice(1).every(p=>p.completed.has(manifest.id)),'remaining deliveries after leader exit',15000);
  assert.ok(run.checks>before,'successor checks admin before scheduling');
  for(const p of run.peers.slice(1))assert.deepEqual(new Uint8Array(await p.completed.get(manifest.id).arrayBuffer()),payload);
 }finally{run.close();}
});

test('admin revocation stops active automatic transfers and later assignments',async()=>{
 const run=await setup(8);try{
  const manifest=await run.peers[0].network.distributeFile(new Blob([new Uint8Array(1024*1024)]),{chunkSize:4096,distributionId:'revoked-release'});
  await until(()=>run.events.some(e=>e.type==='distributionConnectionOpened'),'active transfer');
  // An isolated signer supplies the order to SysCon 1; receivers have no private admin key.
  const proof=await run.admin.sign({networkId:run.peers[0].network.networkId,action:'content.revoke',target:'revoked-release',operationId:crypto.randomUUID()});
  await run.peers[0].network.submitAdminOrder(proof);
  await until(()=>run.peers.every(p=>p.network.isRevoked('revoked-release')),'revoke propagated');
  await until(()=>run.peers.every(p=>p.network.getDistributionState().connections.length===0),'revoked links closed');
  const jobs=run.events.filter(e=>e.type==='distributionJob').length;await new Promise(r=>setTimeout(r,100));
  assert.equal(run.events.filter(e=>e.type==='distributionJob').length,jobs);
  assert.ok(run.peers.slice(1).every(p=>!p.completed.has(manifest.id)));
  await assert.rejects(run.peers[0].network.distributeFile(new Blob([new Uint8Array(4096)]),{distributionId:'revoked-release'}),/revoked|unavailable/);
 }finally{run.close();}
});


test('a stalled piece connection is released and unfinished work is reassigned',async()=>{
 const run=await setup(6);let stalled=null,dropped=0;
 setDistributionFault((channel,data)=>{if(data instanceof ArrayBuffer){stalled??=channel;if(channel===stalled){dropped++;return 'drop';}}});
 try{
  const payload=Uint8Array.from({length:128*1024},(_,i)=>i%239);
  const manifest=await run.peers[0].network.distributeFile(new Blob([payload]),{chunkSize:4096,distributionId:'stall-recovery'});
  await until(()=>run.peers.slice(1).every(p=>p.completed.has(manifest.id)),'delivery around stalled connection',15000);
  assert.ok(dropped>0);assert.ok(run.events.some(e=>e.type==='distributionError'&&/remaining chunks/.test(e.error)));
  for(const p of run.peers.slice(1))assert.deepEqual(new Uint8Array(await p.completed.get(manifest.id).arrayBuffer()),payload);
  await until(()=>run.peers.every(p=>p.network.getDistributionState().connections.length===0),'stalled connection closed');
 }finally{setDistributionFault(null);run.close();}
});


test('isolated admin authorizes an asset without any SDK peer holding its private key',async()=>{
 const run=await setup(3,{isolated:true});try{
  const file=new Blob([new Uint8Array(8192)]),source=run.peers[0].network;
  const manifest=await source.publishFile(file,{chunkSize:4096,distributionId:'isolated-admin'});
  await assert.rejects(source.distributeFile(file,{chunkSize:4096,distributionId:'isolated-admin'}),/Admin seed/);
  const authorization=await run.controller.publish(source.networkId,manifest.id);
  const shared=await source.distributeFile(file,{chunkSize:4096,distributionId:'isolated-admin',authorization});
  await until(()=>run.peers.slice(1).every(p=>p.completed.has(shared.id)),'externally authorized delivery');
  const revoke=await run.controller.revoke(source.networkId,'isolated-admin');await source.submitAdminOrder(revoke);
  await until(()=>run.peers.every(p=>p.network.isRevoked('isolated-admin')),'isolated revoke');
  assert.equal((await run.controller.getOrders(source.networkId)).length,2);
 }finally{run.close();}
});


test('successor checks isolated admin kill order and propagates it instead of scheduling',async()=>{
 const run=await setup(4,{isolated:true,retireGraceMs:100});try{
  await run.controller.retire(run.peers[0].network.networkId);
  const before=run.checks;run.peers[0].network.close();
  await until(()=>run.peers.slice(1).every(p=>p.network.getState().state==='retired'),'successor propagates retained kill');
  assert.ok(run.checks>before);assert.equal(run.events.filter(e=>e.type==='distributionJob').length,0);
 }finally{run.close();}
});

test('empty assets distribute without opening private chat',async()=>{
 const run=await setup(3);try{
  const manifest=await run.peers[0].network.distributeFile(new Blob([]),{distributionId:'empty-release'});
  await until(()=>run.peers.slice(1).every(p=>p.completed.has(manifest.id)),'empty asset delivery');
  for(const p of run.peers.slice(1))assert.equal(p.completed.get(manifest.id).size,0);
 }finally{run.close();}
});


test('concurrent assets use their own transfer channels and keep bytes separate',async()=>{
 const run=await setup(4);try{
  const a=new Blob([new Uint8Array(32*1024).fill(17)]),b=new Blob([new Uint8Array(32*1024).fill(231)]);
  const settings={chunkSize:4096,distributionId:'second-source'};
  const prepared=await run.peers[1].network.publishFile(b,settings),authorization=await run.controller.publish(run.peers[1].network.networkId,prepared.id);
  const [first,second]=await Promise.all([run.peers[0].network.distributeFile(a,{chunkSize:4096,distributionId:'first-source'}),run.peers[1].network.distributeFile(b,{...settings,authorization})]);
  await until(()=>run.peers.every((p,i)=>(i===0||p.completed.has(first.id))&&(i===1||p.completed.has(second.id))),'concurrent asset delivery');
  for(let i=0;i<4;i++){if(i!==0)assert.deepEqual(new Uint8Array(await run.peers[i].completed.get(first.id).arrayBuffer()),new Uint8Array(await a.arrayBuffer()));if(i!==1)assert.deepEqual(new Uint8Array(await run.peers[i].completed.get(second.id).arrayBuffer()),new Uint8Array(await b.arrayBuffer()));}
 }finally{run.close();}
});
