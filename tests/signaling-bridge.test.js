import test from 'node:test';
import assert from 'node:assert/strict';
import {createSignalingBridge} from '../packages/phantom-core/src/signaling-bridge.js';
import {createNostrIdentity} from '../packages/phantom-doorways/src/nostr-crypto.js';
import {verifyPeerProof} from '../packages/phantom-doorways/src/peer-proof.js';
function fixture(){
 let time=Date.now();const identities=Array.from({length:3},()=>createNostrIdentity()),keys=identities.map(i=>i.publicKey.slice(0,16)),connections=[[1],[0,2],[1]],received=[[],[],[]],events=[],frames=[],missing=new Set();
 const rows=new Map(identities.map((i,n)=>[keys[n],{hKey:keys[n],hex:i.publicKey,bootId:'boot-'+n}]));
 const nodes=identities.map((identity,i)=>createSignalingBridge({self:()=>keys[i],room:()=> 'room',member:key=>missing.has(i+":"+key)?{...rows.get(key),hex:undefined}:rows.get(key),bindIdentity:(key,hex)=>{if(rows.get(key)?.hex!==hex)return false;missing.delete(i+":"+key);return true;},peers:()=>connections[i].map(j=>keys[j]),now:()=>time,
  authentication:{sign:payload=>identity.signPeerProof({networkId:'room',kind:'sys_recovery_bridge',payload,issuedAt:time,nonce:crypto.randomUUID()}),verify:(proof,payload,key)=>proof.peerPublicKey===key&&verifyPeerProof(proof,{networkId:'room',kind:'sys_recovery_bridge',payload,now:time})},
  send:(to,envelope)=>{frames.push({from:keys[i],to,envelope:structuredClone(envelope)});return true;},deliver:packet=>received[i].push(packet),status:detail=>events.push(detail)}));
 function flush(){while(frames.length){const f=frames.shift();nodes[keys.indexOf(f.to)].receive(f.envelope,f.from);}}
 const offer={type:'rtc_offer',from:keys[0],to:keys[2],videoId:'room',pipe:'sys',sysCarrier:true,bootId:'boot-0',sysAttemptId:'attempt',sdp:{type:'offer',sdp:'test'}};
 return {nodes,keys,rows,offer,received,events,frames,flush,missing,advance:ms=>{time+=ms;}};
}
test('offer, answer, and ICE traverse one intermediary with original sender proof',()=>{
 const f=fixture();assert.equal(f.nodes[0].send(f.offer),true);f.flush();assert.deepEqual(f.received[2],[f.offer]);
 const answer={...f.offer,type:'rtc_answer',from:f.keys[2],to:f.keys[0],bootId:'boot-2',sdp:{type:'answer',sdp:'test'}};
 assert.equal(f.nodes[2].send(answer),true);f.flush();assert.deepEqual(f.received[0],[answer]);
 const ice={...f.offer,type:'rtc_ice',cand:{candidate:'test'}};f.nodes[0].send(ice);f.flush();assert.equal(f.received[2].at(-1).type,'rtc_ice');assert.equal(f.events.filter(e=>e.action==='forwarded').length,3);
});
test('platform description and candidate objects use their JSON wire representation',()=>{
 const f=fixture();
 const description={type:'offer',sdp:'native-shaped',toJSON(){return {type:this.type,sdp:this.sdp};}};
 assert.throws(()=>structuredClone(description));
 assert.equal(f.nodes[0].send({...f.offer,sdp:description}),true);f.flush();assert.deepEqual(f.received[2][0].sdp,{type:'offer',sdp:'native-shaped'});
 const candidate={candidate:'native-shaped',sdpMid:'0',toJSON(){return {candidate:this.candidate,sdpMid:this.sdpMid};}};
 f.nodes[0].send({...f.offer,type:'rtc_ice',sdp:undefined,cand:candidate});f.flush();assert.deepEqual(f.received[2].at(-1).cand,{candidate:'native-shaped',sdpMid:'0'});
});
test('tampering, impersonation, wrong room, extra hops, and replay are rejected',()=>{
 const f=fixture();f.nodes[0].send(f.offer);const frame=f.frames.shift(),envelope=frame.envelope;
 const changed=structuredClone(envelope);changed.proof.payload.packet.sdp.sdp='tampered';assert.equal(f.nodes[1].receive(changed,f.keys[0]),false);
 assert.equal(f.nodes[1].receive(envelope,f.keys[2]),false);assert.equal(f.nodes[1].receive({...envelope,hops:2},f.keys[0]),false);
 assert.equal(f.nodes[1].receive(envelope,f.keys[0]),true);assert.equal(f.nodes[1].receive(envelope,f.keys[0]),false);
 f.flush();assert.equal(f.received[2].length,1);assert.equal(f.nodes[0].send({...f.offer,videoId:'other'}),false);
});
test('expired packets and removed or reincarnated senders cannot revive a route',()=>{
 for(const mutation of [f=>f.advance(15001),f=>f.rows.delete(f.keys[0]),f=>f.rows.get(f.keys[0]).bootId='new']){
  const f=fixture();f.nodes[0].send(f.offer);const frame=f.frames.shift();mutation(f);assert.equal(f.nodes[1].receive(frame.envelope,f.keys[0]),false);
 }
});
test('clearing a room removes retained deduplication and return routes',()=>{
 const f=fixture();f.nodes[0].send(f.offer);const frame=f.frames.shift();f.nodes[1].receive(frame.envelope,f.keys[0]);f.flush();
 f.nodes[1].clear();assert.equal(f.nodes[1].receive(frame.envelope,f.keys[0]),true);
 f.nodes[0].clear();f.nodes[0].send({...f.offer,type:'rtc_ice'});assert.equal(f.frames.filter(x=>x.from===f.keys[0]).length,1);
});
test('signing failure yields back to the caller without sending a partial envelope',()=>{
 const identity=createNostrIdentity(),self=identity.publicKey.slice(0,16),other='2'.repeat(16),bridge='3'.repeat(16),events=[];let sent=0;
 const node=createSignalingBridge({self:()=>self,room:()=> 'room',member:()=>({}),peers:()=>[bridge],send:()=>{sent++;return true;},deliver(){},authentication:{sign(){throw Error('signer unavailable');}},status:e=>events.push(e)});
 assert.equal(node.send({type:'rtc_offer',from:self,to:other,videoId:'room',pipe:'sys',sysCarrier:true,bootId:'boot',sysAttemptId:'attempt'}),false);
 assert.equal(sent,0);assert.equal(events[0].action,'failed');
});
test('receiver reports the failed identity gate without exposing signed payloads',()=>{
 const f=fixture();f.nodes[0].send(f.offer);const frame=f.frames.shift();f.rows.get(f.keys[0]).hex=null;
 assert.equal(f.nodes[1].receive(frame.envelope,f.keys[0]),false);
 const event=f.events.at(-1);assert.equal(event.action,'rejected');assert.equal(event.reason,'sender_key_missing');assert.equal(event.attemptId,'attempt');assert.equal('proof' in event,false);assert.equal('sdp' in event,false);
});
test('destination learns a missing source key only from a verified intermediary attestation',()=>{
 const f=fixture();f.missing.add('2:'+f.keys[0]);f.nodes[0].send(f.offer);const first=f.frames.shift();f.nodes[1].receive(first.envelope,first.from);const forwarded=f.frames.shift();
 const tampered=structuredClone(forwarded.envelope);tampered.identityProof.payload.publicKey='a'.repeat(64);
 assert.equal(f.nodes[2].receive(tampered,forwarded.from),false);assert.equal(f.missing.has('2:'+f.keys[0]),true);
 assert.equal(f.nodes[2].receive(forwarded.envelope,forwarded.from),true);assert.equal(f.missing.has('2:'+f.keys[0]),false);assert.deepEqual(f.received[2],[f.offer]);
});
