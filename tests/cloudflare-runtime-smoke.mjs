import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import assert from 'node:assert/strict';
import {createAdminIdentity} from '../packages/phantom-core/src/authorization.js';
import {createAdmissionCheckpoint,createAdmissionVerifier} from '../packages/phantom-core/src/admission.js';
import {createNostrIdentity} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {createLegacyDoorway} from '../packages/phantom-doorways/src/index.js';
import {adaptLegacyDoorway} from '../packages/phantom-doorways/src/interface.js';
import {withAdmissionDoorway} from '../packages/phantom-doorways/src/admission-doorway.js';
const admin=await createAdminIdentity();
const root=fileURLToPath(new URL('../',import.meta.url));
const server=spawn(process.platform==='win32'?'npx.cmd':'npx',['--yes','wrangler@4.149.0','dev','--config','templates/cloudflare-doorway/wrangler.jsonc','--local','--ip','127.0.0.1','--port','8787','--var','ADMISSION_AUTHORITY_PUBLIC_KEY:'+admin.publicKey],{cwd:root,detached:process.platform!=='win32',stdio:['ignore','pipe','pipe']});
let diagnostics='',a,b;
server.stdout.on('data',chunk=>{diagnostics=(diagnostics+chunk).slice(-8000);});
server.stderr.on('data',chunk=>{diagnostics=(diagnostics+chunk).slice(-8000);});
const serverFailure=new Promise((_,reject)=>server.on('error',reject));
const room=crypto.randomUUID().replaceAll('-','').repeat(2),sent=[];
class TrackingSocket extends WebSocket{send(raw){sent.push(JSON.parse(raw));return super.send(raw);}}
const verify=createAdmissionVerifier({authorityPublicKey:admin.publicKey});
async function waitFor(check){const deadline=Date.now()+5000;while(!check()){if(Date.now()>deadline)throw Error('Timed out');await new Promise(r=>setTimeout(r,20));}}
async function peer(){
 const identity=createNostrIdentity();const checkpoint=createAdmissionCheckpoint({networkId:room,peerPublicKey:identity.publicKey,policy:{authorize:({networkId,peerPublicKey})=>admin.signAdmission({networkId,peerPublicKey,expiresAt:Date.now()+60000}),verify}});
 const credential=await checkpoint.authorize();const states=[],signals=[],errors=[];let open=false;
 const raw=adaptLegacyDoorway(createLegacyDoorway({url:'ws://127.0.0.1:8787/',WebSocket:TrackingSocket}));
 const session=withAdmissionDoorway(raw,{identity,checkpoint,credential}).open({roomId:room,peerId:identity.publicKey.slice(0,16),onOpen:()=>{open=true;},onDiscovery:p=>states.push(p),onSignal:p=>signals.push(p),onClose(){},onError:e=>errors.push(e)});
 await waitFor(()=>open);session.announce({roomId:room,peerId:identity.publicKey.slice(0,16),handle:'Local test',bootId:crypto.randomUUID(),t:Date.now()});
 await waitFor(()=>states.length>0);return {identity,session,states,signals,errors};
}
try{
 const deadline=Date.now()+45000;let ready=false;
 while(Date.now()<deadline){try{const r=await fetch('http://127.0.0.1:8787/health',{signal:AbortSignal.timeout(1000)});if(r.ok){ready=true;break;}}catch{}await new Promise(r=>setTimeout(r,250));}
 if(!ready)throw Error('Local worker unreachable\n'+diagnostics);
 a=await Promise.race([peer(),serverFailure]);b=await peer();
 await waitFor(()=>a.states.at(-1)?.peers.length===2&&b.states.at(-1)?.peers.length===2);
 const packet={type:'rtc_offer',videoId:room,from:a.identity.publicKey.slice(0,16),to:b.identity.publicKey.slice(0,16),sdp:{type:'offer',sdp:'runtime smoke'}};
 a.session.sendSignal(packet);await waitFor(()=>b.signals.length===1);assert.deepEqual(b.signals[0],packet);
 const captured=sent.find(p=>p.type==='concierge_connect'&&p.from===a.identity.publicKey.slice(0,16));assert.ok(captured.connectionChallenge);
 const rogue=new WebSocket(`ws://127.0.0.1:8787/ws?v=${room}&to=${captured.from}`);let closed;
 rogue.addEventListener('open',()=>rogue.send(JSON.stringify(captured)));
 rogue.addEventListener('close',e=>{closed=e;});await waitFor(()=>closed);assert.equal(closed.code,1008);
 assert.deepEqual(a.errors,[]);assert.deepEqual(b.errors,[]);
 console.log('PASS: two authorized peers discover each other; signed signal arrives unchanged; replayed join is rejected (1008).');
}finally{
 a?.session.close();b?.session.close();
 try{if(process.platform==='win32')server.kill('SIGTERM');else process.kill(-server.pid,'SIGTERM');}catch{}
}
