// Optional real ICE/DTLS/SCTP smoke test. It uses loopback, not device NAT.
import assert from 'node:assert/strict';
import os from 'node:os';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
import {createBrowserPhantom} from '../../packages/phantom-doorways/src/browser-entry.js';
import {memoryDoorway} from './memory-doorway.js';
const root=process.env.PHANTOM_REAL_RTC_ROOT;
if(!root)throw Error('Set PHANTOM_REAL_RTC_ROOT to a directory with werift@0.25.0 installed.');
const {RTCPeerConnection}=await import(pathToFileURL(resolve(root,'node_modules/werift/lib/webrtc/src/index.js')).href);
// Some managed environments deny interface enumeration. Advertise only the
// loopback interface we explicitly bind; socket permissions remain in force.
try{os.networkInterfaces();}catch{os.networkInterfaces=()=>({lo:[{address:'127.0.0.1',netmask:'255.0.0.0',family:'IPv4',mac:'00:00:00:00:00:00',internal:true,cidr:'127.0.0.1/8'}]});}
const config={iceServers:[],iceUseIpv6:false,iceAdditionalHostAddresses:['127.0.0.1'],iceInterfaceAddresses:{udp4:'127.0.0.1'}};
const pcs=[],networks=[];
class RTC extends RTCPeerConnection{constructor(){super(config);pcs.push(this);}}
function deadline(promise,ms,label){let timer;return Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error(label)),ms);})]).finally(()=>clearTimeout(timer));}
try{
 // Verify the test environment independently before judging SDK admission.
 const a=new RTC(),b=new RTC();
 a.onicecandidate=e=>{if(e.candidate)void b.addIceCandidate(e.candidate);};
 b.onicecandidate=e=>{if(e.candidate)void a.addIceCandidate(e.candidate);};
 const received=new Promise(resolve=>{b.ondatachannel=e=>{e.channel.onmessage=m=>resolve(m.data);};});
 const ch=a.createDataChannel('control');ch.onopen=()=>ch.send('control');
 await a.setLocalDescription(await a.createOffer());await b.setRemoteDescription(a.localDescription);
 await b.setLocalDescription(await b.createAnswer());await a.setRemoteDescription(b.localDescription);
 assert.equal(await deadline(received,5000,'Bare RTC control failed'),'control');
 await a.close();await b.close();
 console.log('PASS bare loopback RTC control');
 const memory=memoryDoorway({copies:3}),reasons=[];
 const runtime={crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval};
 for(let i=0;i<2;i++){
  const sdk=createBrowserPhantom({transportDiagnostics:true,runtime,doorways:[{type:'memory'}],adapters:{memory:({identity})=>memory.register(identity)}});
  const options={name:i?'Phone':'PC',timeoutMs:10000,onEvent:e=>{if(e.type==='SNAPSHOT')reasons.push(e.payload.reason);}};
  const n=i?await sdk.join(networks[0].invite,options):await sdk.create(options);networks.push(n);
 }
 for(const n of networks){assert.equal(n.getState().state,'admitted');assert.equal(n.snapshot().concierge.membershipAudit.topology.committedCount,2);}
 console.log('PASS SDK admission and mirror commitment over real loopback RTC; deferred='+reasons.includes('sys_admission_waiting_for_local_pipe_open'));
}finally{networks.forEach(n=>n.close());await Promise.all(pcs.map(p=>p.close()));}
