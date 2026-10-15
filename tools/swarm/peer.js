import {observeSignaling} from './live-transport.mjs';
import {parseInvite} from '../../packages/phantom-core/src/invite.js';
import {observeRTC} from './observed-rtc.mjs';
import {createBrowserPhantom} from '../../packages/phantom-doorways/src/browser-entry.js';
import {createAdminIdentity} from '../../packages/phantom-core/src/authorization.js';
import {createWebSocketDoorway, adaptWebSocketDoorway} from '../../packages/phantom-doorways/src/index.js';
let network, admin, name, lastSnapshot=0,lastState,lastCoreSnapshot,admissionTimeout=60000;
const completed={},progress={},errors=[];let publication=0;
const log=(type,data)=>globalThis.harnessLog({type,data});
const observed=observeRTC(globalThis.RTCPeerConnection,log),rtc=observed.active,ObservedRTC=observed.RTCPeerConnection;
const signaling=observeSignaling(globalThis.WebSocket,log);
const sha=async bytes=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
function watch(n){
 network=n;
 for(const type of ['distributionChunkTrace','distributionJob','distributionJobFailed','distributionConnectionFailed','distributionProgress','distributionError','distributionConnectionOpened','distributionConnectionClosed','distributionAdminChecked','distributionFileEvent','controlError'])n.on(type,data=>{
  if(type==='distributionProgress'){const p=progress[data.id]??={received:0,bySource:{}};p.received=Object.values(p.bySource).reduce((sum,n)=>sum+n,0)+1;p.total=data.total;p.bySource[data.from]=(p.bySource[data.from]??0)+1;}
  if(type.endsWith('Error'))errors.push({type,...data});
  void log(type,data);
 });
 n.on('distributionComplete',async({id,blob})=>{try{const result={id,size:blob.size,sha256:await sha(await blob.arrayBuffer())};completed[id]=result;await log('verifiedFile',result);}catch(e){errors.push({error:e.message});void log('verificationError',{error:e.message});}});
}
function options(){return {name,timeoutMs:admissionTimeout,onState:state=>{lastState=state;void log('sdk.state',state);},onEvent:event=>{
 if(event.type==='SNAPSHOT'&&event.payload?.reason==='sys_birth_exception_detail')void log('recovery.exception',{reason:event.payload.reason,error:event.payload.concierge?.sysLastErr});
 if(event.type==='SNAPSHOT'&&['sys_recovery_signal_existing_peer_pipe','sys_recovery_signal_received_existing_peer_pipe'].includes(event.payload?.reason))void log('recovery.peer_pipe',{reason:event.payload.reason});
 if(event.type==='SNAPSHOT'){lastCoreSnapshot=event.payload;if(Date.now()-lastSnapshot<1000)return;lastSnapshot=Date.now();}
 void log('sdk.event',event);
}};}
globalThis.swarm={
 async open({label,invite,timeoutMs,transport,batchSize=8}){
  name=label;admissionTimeout=timeoutMs??60000;if(!invite)admin=await createAdminIdentity();
  const sdk=createBrowserPhantom({...(admin?{adminIdentity:admin}:{}),distribution:{batchSize,maxConnections:3,traceChunks:true},runtime:{crypto,RTCPeerConnection:ObservedRTC,setTimeout,clearTimeout,setInterval,clearInterval,timerTarget:globalThis,localStorage},WebSocket:signaling.WebSocket,onRelayProgress:data=>void log('relay.discovery',data),onDoorwayStatus:data=>void log('doorway.status',data),doorways:transport?.doorways??[{type:'harness'}],adapters:{harness:()=>adaptWebSocketDoorway(createWebSocketDoorway({url:location.origin.replace('http:','ws:')+'/',WebSocket:signaling.WebSocket}))}});
  watch(await (invite?sdk.join(invite,options()):sdk.create(options())));
  return {invite:network.invite,peerId:network.getState().peerId,networkId:network.networkId,doorways:parseInvite(network.invite).doorways};
 },
 invitePeer(id){network.invitePeer(id);},
 acceptPeer(id){network.acceptPeer(id);},
 peerPipes(){return network.snapshot().chatPairs;},
 setSignalingPaused(value){signaling.setPaused(value);},
 async exportAdminSeed(){if(!admin)throw Error('No admin signer on this peer');return admin.exportSeed();},
 async publish(config){const size=typeof config==='number'?config:config.size;publication=typeof config==='number'?publication+1:config.variant;const bytes=Uint8Array.from({length:size},(_,i)=>(i*31+(i>>>8)+publication)%256);const expected=await sha(bytes);const file=new Blob([bytes]),settings={name:'swarm-fixture.bin'};let authorization;if(!admin){const prepared=await network.publishFile(file,settings);authorization=await globalThis.harnessAuthorizePublish({networkId:network.networkId,target:prepared.id});await log('publish.authorization',{networkId:network.networkId,target:prepared.id,operationId:authorization.operationId});}const manifest=await network.distributeFile(file,{...settings,...(authorization?{authorization}:{})});completed[manifest.id]={id:manifest.id,size,sha256:expected,publisher:true};await log('published',{manifest,sha256:expected});return {id:manifest.id,size,sha256:expected};},
 async inspect(){const connections=[];for(const pc of rtc){try{const stats=await pc.getStats();const rows=[];stats.forEach(s=>{if(['candidate-pair','local-candidate','remote-candidate','transport','data-channel'].includes(s.type))rows.push(s);});connections.push({connectionState:pc.connectionState,iceConnectionState:pc.iceConnectionState,stats:rows});}catch{}}return {state:network?.getState()??lastState,snapshot:network?.snapshot()??lastCoreSnapshot,distribution:network?.getDistributionState(),completed,progress,errors,rtc:connections,rtcCounts:{...observed.counts,live:rtc.size},signalingCounts:{...signaling.counts}};},
 close(){network?.close();}
};
