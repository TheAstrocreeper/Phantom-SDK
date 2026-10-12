import {createBrowserPhantom} from '../../packages/phantom-doorways/src/browser-entry.js';

/** Cut setup sockets only: real peer data channels and engine remain alive. */
export function socketGate(Base) {
  let offline=false; const sockets=new Set();
  class Socket extends Base {
    constructor(...args) {
      if(offline)throw new Error('Test doorway offline');
      super(...args);sockets.add(this);
      this.addEventListener('close',()=>sockets.delete(this),{once:true});
    }
  }
  return {Socket,cut(){offline=true;for(const socket of [...sockets])socket.close();},restore(){offline=false;},get offline(){return offline;}};
}

/** Drop a peer's RTC transports without first sending a departure command. */
export function rtcGate(runtime){
  let offline=false;const connections=new Set(),Base=runtime.RTCPeerConnection;
  if(!Base)return {runtime,cut(){},restore(){}};
  class RTC extends Base{
    constructor(...args){if(offline)throw Error('Test peer RTC offline');super(...args);connections.add(this);}
    close(){connections.delete(this);super.close();}
  }
  return {runtime:{...runtime,RTCPeerConnection:RTC},cut(){offline=true;for(const pc of [...connections])pc.close();},restore(){offline=false;}};
}

export async function runMixedTest({endpoint,relays,sdkFactory=createBrowserPhantom,WebSocket=globalThis.WebSocket,runtime=globalThis,signal,onUpdate=()=>{},deadlineMs=45000,lifecycle=false,abrupt=false}={}) {
  const report={format:'phantom-mixed-test-v2',startedAt:new Date().toISOString(),endpoint,steps:[],events:[],peers:[],passed:false,
    lifecycle,abrupt,checkpoints:[],scope:'Three clients in one browser; public Nostr and CF setup paths; local WebRTC. Optional graceful departures, same-identity cross-doorway returns, and abrupt transport loss without departure notice. Does not test separate-device NAT or a real process crash.'};
  const clients=[],observers=new Set();
  const notify=()=>{for(const fn of [...observers])fn();};
  const record=(peer,event)=>{if(report.events.length>=6000)report.events.shift();report.events.push({peer,at:new Date().toISOString(),...structuredClone(event)});notify();};
  function until(predicate,label){return new Promise((resolve,reject)=>{
    const finish=error=>{clearTimeout(timer);observers.delete(check);signal?.removeEventListener('abort',abort);error?reject(error):resolve();};
    const abort=()=>finish(new Error('Test cancelled'));
    const check=()=>{try{if(predicate())finish();}catch(error){finish(error);}};
    const timer=setTimeout(()=>finish(new Error(label+' timed out')),deadlineMs);
    observers.add(check);signal?.addEventListener('abort',abort,{once:true});if(signal?.aborted)abort();else check();
  });}
  async function step(label,action){
    const row={label,status:'running'};report.steps.push(row);onUpdate(report);
    try{if(signal?.aborted)throw new Error('Test cancelled');await action();row.status='pass';}
    catch(error){row.status='fail';row.error=error.message;throw error;}
    finally{onUpdate(report);}
  }
  function make(name,types){
    const gate=socketGate(WebSocket),rtc=rtcGate(runtime),p={name,gate,rtc,network:null,state:null,lastSnapshot:null,inbox:[]};clients.push(p);
    const sdk=sdkFactory({doorwayMode:'parallel',doorways:[{type:'nostr',...(relays?.length?{relays}: {})},{type:'websocket',url:endpoint}],WebSocket:gate.Socket,runtime:rtc.runtime,
      onDoorwayStatus:detail=>record(name,{type:'DOORWAY_STATUS',payload:detail}),
      onRelayProgress:detail=>record(name,{type:'RELAY_SCAN',payload:detail})});
    p.options={name,doorwayTypes:types,timeoutMs:deadlineMs,signal,
      onState:state=>{p.state=state;record(name,{type:'NETWORK_STATE',payload:state});},
      onEvent:event=>{
        if(event.type==='SNAPSHOT')p.lastSnapshot=structuredClone(event.payload);
        if(event.type==='INCOMING_MSG')p.inbox.push(event.payload);
        if(event.type==='VEIL_INVITE_RECEIVED')queueMicrotask(()=>{if(p.network&&!signal?.aborted)p.network.acceptPeer(event.payload.from);});
        record(name,event);
      }};
    p.sdk=sdk;return p;
  }
  const key=p=>p.network.getState().peerId;
  const ready=(p,q)=>p.network.snapshot()?.chatPairs?.some(r=>r.hKey===key(q)&&r.dcState==='open'&&r.chatState==='ready');
  async function connect(a,b){
    a.network.invitePeer(key(b));
    await until(()=>ready(a,b)&&ready(b,a),a.name+' / '+b.name+' connection');
  }
  async function exchange(a,b){for(const [sender,receiver]of [[a,b],[b,a]]){
    const text=sender.name+' to '+receiver.name+' '+runtime.crypto.randomUUID();
    sender.network.send(key(receiver),text);
    await until(()=>receiver.inbox.some(m=>m.from===key(sender)&&m.text===text),'Message '+sender.name+' to '+receiver.name);
  }}
  async function agreement(active,leader,mirror,label){
    await until(()=>active.every(p=>{
      const c=p.network.snapshot()?.concierge,t=c?.membershipAudit?.topology;
      const expected=active.map(q=>key(q).slice(0,8)).sort().join(',');
      const base=active[0].network.snapshot()?.concierge?.membershipAudit?.topology;
      const role=p===leader?'syscon1':p===mirror?'syscon2':'peer';
      return p.network.getState().state==='admitted'&&c?.sysReady&&t?.committedCount===active.length&&
        t.committedKeys?.split(',').sort().join(',')===expected&&
        t.committedOne===key(leader).slice(0,8)&&t.committedTwo===(mirror?key(mirror).slice(0,8):'none')&&
        t.committedEpoch===base.committedEpoch&&t.committedSeq===base.committedSeq&&t.pendingId==='none'&&
        p.network.getState().role===role;
    }),label);
    report.checkpoints.push({label,leader:key(leader),mirror:mirror?key(mirror):null,
      peers:active.map(p=>({name:p.name,peerId:key(p),state:p.network.getState(),topology:p.network.snapshot()?.concierge?.membershipAudit?.topology}))});
  }
  async function departAndReturn(leaver,survivors,leader,mirror,types,crash=false){
    const oldKey=key(leaver),invite=leaver.network.invite;
    await step(leaver.name+(crash?' loses all transports without a departure notice':' leaves')+'; survivors agree on promotion',async()=>{
      leaver.lastSnapshot=leaver.network.snapshot();
      if(crash){leaver.rtc.cut();leaver.gate.cut();}
      leaver.network.close();leaver.network=null;
      record('harness',{type:'PEER_LEFT',payload:{name:leaver.name,peerId:oldKey,kind:crash?'abrupt_transport_loss':'graceful_sdk_close'}});
      await agreement(survivors,leader,mirror,leaver.name+' departure');
      await exchange(survivors[0],survivors[1]);
    });
    await step(leaver.name+' returns through '+types.join(' + ')+' with the same identity',async()=>{
      leaver.gate.restore();leaver.rtc.restore();leaver.inbox=[];leaver.options={...leaver.options,doorwayTypes:types};
      leaver.network=await leaver.sdk.join(invite,leaver.options);
      if(key(leaver)!==oldKey)throw Error('Returning peer identity changed');
      record('harness',{type:'PEER_RETURNED',payload:{name:leaver.name,peerId:key(leaver),doorwayTypes:types}});
      await agreement([...survivors,leaver],leader,mirror,leaver.name+' return');
      for(const survivor of survivors){await connect(leaver,survivor);await exchange(leaver,survivor);}
    });
  }
  try {
    let u;try{u=new URL(endpoint);}catch{throw new Error('Enter a secure Cloudflare switchboard URL');}if(u.protocol!=='wss:'||u.username||u.password||u.hash)throw new Error('Enter a secure Cloudflare switchboard URL');
    const astro=make('Astro',['nostr','websocket']);
    await step('Create network with both doorways',async()=>{astro.network=await astro.sdk.create(astro.options);});
    const nova=make('Nova',['nostr']);
    await step('Nova enters through Nostr only',async()=>{nova.network=await nova.sdk.join(astro.network.invite,nova.options);});
    const third=make('Echo',['websocket']);
    await step('Echo enters through Cloudflare only',async()=>{third.network=await third.sdk.join(astro.network.invite,third.options);});
    await step('All three share committed membership',()=>until(()=>clients.every(p=>{
      const c=p.network.snapshot()?.concierge,t=c?.membershipAudit?.topology;
      const expected=clients.map(q=>key(q).slice(0,8)).sort().join(',');
      return p.network.getState().state==='admitted'&&c?.sysReady&&t?.committedCount===3&&
        t.committedKeys?.split(',').sort().join(',')===expected&&
        t.committedOne===key(astro).slice(0,8)&&t.committedTwo===key(nova).slice(0,8);
    }),'Committed membership'));
    for(const [a,b]of [[astro,nova],[astro,third],[nova,third]])await step(a.name+' and '+b.name+' exchange messages',async()=>{await connect(a,b);await exchange(a,b);});
    if(lifecycle){
      await departAndReturn(third,[astro,nova],astro,nova,['nostr']);
      await departAndReturn(nova,[astro,third],astro,third,['websocket']);
      await departAndReturn(astro,[third,nova],third,nova,['nostr']);
      if(abrupt)await departAndReturn(third,[nova,astro],nova,astro,['websocket'],true);
    }
    await step('Messages still arrive with all doorway sockets cut',async()=>{
      for(const p of clients)p.gate.cut();
      record('harness',{type:'ALL_DOORWAYS_CUT',payload:{rtcClosed:false,reconnectsBlocked:true}});
      for(const [a,b]of [[astro,nova],[astro,third],[nova,third]])await exchange(a,b);
    });
    report.passed=true;
  }catch(error){report.error=error.message;}
  finally{
    report.peers=clients.map(p=>({name:p.name,state:p.network?.getState()??p.state,snapshot:p.network?.snapshot()??p.lastSnapshot}));
    for(const p of clients){p.network?.close();p.gate.cut();p.rtc.cut();}
    report.finishedAt=new Date().toISOString();onUpdate(report);
  }
  return report;
}
