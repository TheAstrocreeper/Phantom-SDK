/** Full file/session flow comparison over a deterministic transport; not a native throughput benchmark. */
import assert from 'node:assert/strict';
import {createFileDistribution} from '../../../packages/phantom-core/src/files.js';
import {createNetworkDistribution} from '../../../packages/phantom-core/src/network-distribution.js';
import {createAdminIdentity} from '../../../packages/phantom-core/src/authorization.js';
import {memoryRTC} from '../../../tests/fixtures/memory-rtc.js';
import {createShareSession} from '../src/session.js';
const ids=['1111111111111111','2222222222222222'];
const hash=async blob=>Buffer.from(await crypto.subtle.digest('SHA-256',await blob.arrayBuffer())).toString('hex');
async function until(check){const end=Date.now()+15000;while(!check()&&Date.now()<end)await new Promise(r=>setTimeout(r,2));assert.ok(check(),'flow did not finish');}
async function fixture(){
 const admin=await createAdminIdentity(),events=[],listeners=ids.map(()=>new Set()),handlers=ids.map(()=>new Map()),errors=[],RTC=memoryRTC();
 const emit=(i,type,data)=>{events.push({i,type,data,at:performance.now()});for(const fn of handlers[i].get(type)||[])fn(data);};
 const dispatch=(i,event)=>{for(const fn of listeners[i])fn(event);};
 const pcs=[new RTC(),new RTC()],direct=[pcs[0].createDataChannel('peer')];
 pcs[1].ondatachannel=e=>direct[1]=e.channel;
 await pcs[0].setLocalDescription(await pcs[0].createOffer());await pcs[1].setRemoteDescription(pcs[0].localDescription);await pcs[1].setLocalDescription(await pcs[1].createAnswer());await pcs[0].setRemoteDescription(pcs[1].localDescription);await until(()=>direct[1]?.readyState==='open');
 const cores=ids.map((peerId,i)=>({
  getState:()=>({peerId,leader:ids[0],mirror:ids[1],role:i?'syscon2':'syscon1',state:'admitted',admitted:true}),
  onEvent(fn){listeners[i].add(fn);return()=>listeners[i].delete(fn);},
  command(type,p){
   if(type==='PHANTOM_DIST_SEND'){queueMicrotask(()=>dispatch(ids.indexOf(p.frame.to),{type:'PHANTOM_DIST',payload:{from:peerId,frame:p.frame}}));return;}
   if(type==='SEND_DM')direct[i].send(JSON.stringify({kind:'message',sys:p.sys,file:p.file}));
   if(type==='SEND_FILE_BIN_CHUNK'){
    direct[i].send(JSON.stringify({kind:'binary',file:p.file}));direct[i].send(p.bytes);
    queueMicrotask(()=>dispatch(i,{type:'FILE_BIN_CHUNK_SENT',payload:{...p,ok:true}}));
   }
  },
 }));
 for(let i=0;i<2;i++){let binary;direct[i].onmessage=({data})=>{
  if(data instanceof ArrayBuffer){dispatch(i,{type:'FILE_BIN_CHUNK',payload:{from:ids[1-i],file:binary,bytes:data}});binary=null;return;}
  const m=JSON.parse(data);if(m.kind==='binary')binary=m.file;else dispatch(i,{type:'INCOMING_MSG',payload:{from:ids[1-i],sys:m.sys,file:m.file}});
 };}
 const files=cores.map((core,i)=>createFileDistribution({core,emit:(type,data)=>emit(i,type,data)}));
 const distributions=cores.map((core,i)=>createNetworkDistribution({core,networkId:'comparison',adminPublicKey:admin.publicKey,lifecycle:{issue:(action,target,fields)=>admin.sign({networkId:'comparison',action,target,...fields,operationId:crypto.randomUUID()})},runtime:{crypto,RTCPeerConnection:RTC},options:{intervalMs:10,batchSize:32,jobTimeoutMs:15000},emit:(type,data)=>emit(i,type,data)}));
 const networks=cores.map((core,i)=>({networkId:'comparison',getState:core.getState,getPeers:()=>ids.map(hKey=>({hKey})),getPeerConnections:()=>[{peerId:ids[1-i],state:'ready'}],
  on(type,fn){if(!handlers[i].has(type))handlers[i].set(type,new Set());handlers[i].get(type).add(fn);return()=>handlers[i].get(type)?.delete(fn);},
  send(to,text){queueMicrotask(()=>emit(ids.indexOf(to),'message',{from:ids[i],text}));},
  prepareDistributionFile:(f,o)=>distributions[i].prepare(f,o),distributeFile:(f,o)=>distributions[i].publish(f,o),getDistributedFile:id=>distributions[i].getFile(id),
 }));
 const sessions=networks.map((network,i)=>createShareSession({network,admin:i?undefined:admin,adminPublicKey:admin.publicKey,onError:e=>errors.push(e.message)}));
 for(let i=0;i<2;i++){dispatch(i,{type:'PRESENCE_ROSTER',payload:{list:ids.map(hKey=>({hKey}))}});emit(i,'peerConnection',{peerId:ids[1-i],state:'ready'});}
 // Let the real Share owner-proof exchange settle before exercising guest authorization.
 await new Promise(r=>setTimeout(r,20));
 return {files,distributions,sessions,events,errors,close(){sessions.forEach(s=>s.close());distributions.forEach(s=>s.close());files.forEach(s=>s.close());pcs.forEach(pc=>pc.close());}};
}
const source=new File([Uint8Array.from({length:32*1024*1024},(_,i)=>i%251)],'comparison.mp4',{type:'video/mp4'}),expected=await hash(source);
const f=await fixture();
try{
 for(const path of ['direct','share-distribution'])for(const from of [0,1]){
  const to=1-from,start=performance.now(),eventStart=f.events.length;let preparedAt,submittedAt,id,blob;
  if(path==='direct'){
   // Breach adds a full-file hash before the SDK's chunk preparation.
   await hash(source);const m=await f.files[from].publish(source,{chunkSize:65536,distributionId:crypto.randomUUID()});id=m.id;preparedAt=performance.now();
   await f.files[from].share(ids[to],id);await until(()=>f.events.slice(eventStart).some(e=>e.i===to&&e.type==='fileOffer'&&e.data.manifest.id===id));submittedAt=performance.now();blob=await f.files[to].request(id,{peers:[ids[from]]});
  }else{
   const row=await f.sessions[from].publish(source,[ids[to]]);id=row.id;submittedAt=performance.now();
   const prep=f.events.slice(eventStart).findLast(e=>e.i===from&&e.type==='distributionFileEvent'&&e.data.fileEvent==='filePublished');preparedAt=prep.at;
   await until(()=>f.sessions[to].assets.get(id)?.status==='received');blob=f.sessions[to].assets.get(id).blob;
   assert.equal(f.sessions[from].assets.get(id).status,'sent');
   const progress=f.events.slice(eventStart).filter(e=>e.i===to&&e.type==='distributionProgress').map(e=>e.data.received);assert.equal(progress.at(-1),512);assert.ok(progress.every((n,i)=>!i||n>=progress[i-1]));
  }
  const completedAt=performance.now();assert.equal(blob.size,source.size);assert.equal(await hash(blob),expected);assert.deepEqual(f.errors,[]);
  assert.equal(f.events.slice(eventStart).some(e=>['fileError','distributionError','distributionConnectionFailed'].includes(e.type)),false);
  console.log(JSON.stringify({transport:'deterministic memory RTC; not native speed',path,direction:from?'guest-to-owner':'owner-to-guest',bytes:blob.size,preparationMs:Math.round(preparedAt-start),submissionMs:Math.round(submittedAt-preparedAt),receiveAndVerifyMs:Math.round(completedAt-submittedAt),totalMs:Math.round(completedAt-start),verified:true}));
 }
}finally{f.close();}
