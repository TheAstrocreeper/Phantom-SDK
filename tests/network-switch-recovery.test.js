import test from 'node:test';
import assert from 'node:assert/strict';
import {createBrowserPhantom} from '../packages/phantom-doorways/src/browser-entry.js';
import {createPhantom} from '../packages/phantom-core/src/index.js';
import {Socket,RTC,rtc} from './fixtures/native-network.js';
const delay=ms=>new Promise(r=>setTimeout(r,ms));
async function until(predicate,label){const end=Date.now()+8000;while(!predicate()){if(Date.now()>end)throw Error(label);await delay(25);}}
test('discovery-hint transport grace defaults to one minute and rejects invalid configuration',()=>{
 for(const [presence,expected] of [['discovery-hints',60000],['live-sockets',10000]]){
  const doorway={protocol:'phantom-concierge-v1',presence,url:'ws://fixture/',WebSocket:Socket};
  const core=createPhantom({doorway});try{assert.equal(core.snapshot().concierge.transportRecoveryGraceMs,expected);}finally{core.dispose();}
  for(const value of [0,NaN,Infinity,120001])assert.throws(()=>createPhantom({doorway,transportRecoveryGraceMs:value}),/transportRecoveryGraceMs/);
 }
});
test('two Nostr peers survive 45 seconds of lost signaling and transfer again after recovery',{timeout:65000},async()=>{
 let blackout=false;const sockets=[],networks=[],events=[];
 class SwitchSocket extends Socket{
  constructor(url){super(url);sockets.push(this);}
  send(raw){if(!blackout)super.send(raw);}
  receive(packet){if(!blackout)super.receive(packet);}
 }
 const runtime={crypto,RTCPeerConnection:RTC,setTimeout,clearTimeout,setInterval,clearInterval};
 const live=n=>n.snapshot().chatPairs?.some(p=>p.dcState==='open');
 async function transfer(bytes){const [sender,receiver]=networks;let offer;const off=receiver.on('fileOffer',row=>{offer=row;});try{const m=await sender.publishFile(new Blob([bytes]),{chunkSize:1024});await sender.shareFile(receiver.getState().peerId,m.id);await until(()=>offer,'file offer not delivered');const blob=await receiver.requestFile(m.id,{peers:[sender.getState().peerId]});assert.deepEqual(new Uint8Array(await blob.arrayBuffer()),bytes);}finally{off();}}
 try{
  for(let i=0;i<2;i++){const sdk=createBrowserPhantom({WebSocket:SwitchSocket,runtime,relays:['wss://nostr.fixture/']});const n=i?await sdk.join(networks[0].invite,{name:'Peer '+i}):await sdk.create({name:'Peer '+i});networks.push(n);n.on('event',e=>{if(e.type==='VEIL_INVITE_RECEIVED')n.acceptPeer(e.payload.from);if(e.type==='SNAPSHOT')events.push(e.payload.reason);});}
  await until(()=>networks.every(n=>n.getState().state==='admitted'),'initial admission failed');networks[0].invitePeer(networks[1].getState().peerId);await until(()=>networks.every(live),'initial chat failed');await transfer(new Uint8Array(2048).fill(7));
  blackout=true;for(const sock of [...sockets])sock.close();for(const pc of [...rtc.values()])if(pc.connectionState==='connected'){pc.connectionState='failed';pc.iceConnectionState='failed';pc.onconnectionstatechange?.();pc.oniceconnectionstatechange?.();}
  await delay(45000);
  for(const n of networks)assert.equal(n.snapshot().concierge.membershipAudit.topology.committedCount,2,'temporary transport loss must preserve room membership');
  assert.ok(!events.includes('sys_replacement_terminal_exit_recorded'));
  blackout=false;for(const sock of [...sockets])sock.close();
  try{await until(()=>networks.every(n=>n.getState().state==='admitted')&&networks.every(live),'peer channels did not recover after signaling returned');}catch(e){e.message+=JSON.stringify(networks.map(n=>({state:n.getState().state,sys:n.snapshot().concierge.sysAttempt?.state,sysFault:n.snapshot().concierge.sysAttempt?.faultReason,chat:n.snapshot().chatPairs?.map(p=>({state:p.chatState,fault:p.chatFault,pc:p.pcState,signal:p.signalingState}))})))+JSON.stringify(events.slice(-35));throw e;}
  await transfer(new Uint8Array(3072).fill(11));
  for(const n of networks)assert.equal(n.snapshot().concierge.membershipAudit.topology.committedCount,2);
 }finally{networks.forEach(n=>n.close());}
});
