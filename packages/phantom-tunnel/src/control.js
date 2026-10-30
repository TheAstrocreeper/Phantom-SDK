import {normalizeBulkControl} from '../../phantom-core/src/bulk-control.js';

/** Experimental probe coordinator. Control through Phantom; payloads native.
 * Native implementations provide prepare({scope,signal}) -> {offer,connect,close}.
 * Incoming resource use requires explicit allowIncoming approval by the embedder.
 */
export function createBulkProbeControl({network,native,allowIncoming=()=>false,onState=()=>{},onTunnel=()=>{},crypto=globalThis.crypto,setupMs=25000}) {
  if(!network?.onBulkControl||!network?.sendBulkControl||!native?.prepare||typeof allowIncoming!=='function'||!Number.isInteger(setupMs)||setupMs<1||setupMs>120000)throw new TypeError('Invalid bulk probe configuration');
  const local=network.getState().peerId;
  if(!/^[0-9a-f]{16}$/.test(local))throw new Error('Join Phantom before opening bulk control');
  const records=new Map(),recent=new Set();let closed=false;
  const permitted=peer=>{try{return !closed&&/^[0-9a-f]{16}$/.test(peer)&&peer!==local&&network.getPeerConnections().some(p=>p.peerId===peer&&p.state==='ready');}catch{return false;}};
  const valid=record=>records.get(record.peer)===record&&!record.abort.signal.aborted&&permitted(record.peer);
  const report=(record,state,reason)=>{try{onState({peer:record.peer,sessionId:record.id,state,reason});}catch{}};
  const remember=(peer,id)=>{recent.add(peer+':'+id);if(recent.size>256)recent.delete(recent.values().next().value);};
  const send=(peer,frame)=>{try{return permitted(peer)&&network.sendBulkControl(peer,frame)===true;}catch{return false;}};
  const cancelFrame=(id,reason)=>({version:1,type:'cancel',sessionId:id,reason});
  function drop(record,reason='closed',tell=true) {
    if(records.get(record.peer)!==record)return;
    records.delete(record.peer);remember(record.peer,record.id);clearTimeout(record.timer);record.abort.abort();
    try{record.native?.close();}catch{}
    record.reject(new Error('Bulk probe '+reason));
    if(tell)send(record.peer,cancelFrame(record.id,reason));
    report(record,'closed',reason);
  }
  function create(peer,id,bytes,initiator) {
    let resolve,reject;
    const ready=new Promise((a,b)=>{resolve=a;reject=b;});ready.catch(()=>{});
    const record={peer,id,bytes,initiator,resolve,reject,ready,abort:new AbortController(),state:'preparing',native:null,remote:null,connecting:false};
    records.set(peer,record);
    record.timer=setTimeout(()=>drop(record,'timeout'),setupMs);record.timer.unref?.();
    report(record,'preparing');return record;
  }
  async function connect(record) {
    if(!valid(record)||!record.native||!record.remote||record.connecting)return;
    record.connecting=true;
    try {
      const tunnel=await record.native.connect(record.remote);
      if(!valid(record)){try{tunnel?.close();}catch{}return;}
      clearTimeout(record.timer);record.state='connected';record.tunnel=tunnel;
      report(record,'connected');if(!valid(record))return;record.resolve(tunnel);
      try{onTunnel(record.peer,tunnel);}catch{}
      // The probe owns its finite native workload. A peer's 'complete' signal
      // alone never substitutes for the local native receipt/hash result.
      if(tunnel.result)Promise.resolve(tunnel.result).then(()=>drop(record,'complete'),()=>drop(record,'failed'));
    } catch {if(valid(record))drop(record,'failed');}
  }
  async function prepare(record) {
    try {
      if(!valid(record))return;
      const handle=await native.prepare({localPeer:local,remotePeer:record.peer,sessionId:record.id,bytes:record.bytes,initiator:record.initiator,signal:record.abort.signal});
      if(!valid(record)){try{handle?.close();}catch{}return;}
      record.native=handle;
      const body=normalizeBulkControl({version:1,type:'offer',sessionId:record.id,offer:handle.offer},local,record.peer);
      if(!body||!send(record.peer,body)){drop(record,'unavailable');return;}
      void connect(record);
    } catch {if(valid(record))drop(record,'failed');}
  }
  function receive(peer,input) {
    if(!permitted(peer))return;
    const frame=normalizeBulkControl(input,peer,local);if(!frame)return;
    let record=records.get(peer);
    if(frame.type==='request') {
      if(recent.has(peer+':'+frame.sessionId))return;
      if(record){if(record.id!==frame.sessionId)send(peer,cancelFrame(frame.sessionId,'busy'));return;}
      if(records.size>=8){send(peer,cancelFrame(frame.sessionId,'busy'));return;}
      record=create(peer,frame.sessionId,frame.bytes,false);
      Promise.resolve().then(()=>valid(record)&&allowIncoming(peer,{purpose:'probe',bytes:frame.bytes,sessionId:frame.sessionId,signal:record.abort.signal})).then(allowed=>{
        if(!valid(record))return;
        if(allowed!==true){drop(record,'declined');return;}
        void prepare(record);
      },()=>{if(valid(record))drop(record,'declined');});
      return;
    }
    if(!record||record.id!==frame.sessionId)return;
    if(frame.type==='cancel') {
      if(frame.reason==='complete'&&record.state==='connected'&&record.tunnel?.result)return;
      drop(record,frame.reason,false);return;
    }
    if(record.remote)return; // No renegotiation or credential replacement.
    record.remote=frame.offer;void connect(record);
  }
  const check=()=>{for(const record of [...records.values()])if(!permitted(record.peer))drop(record,'unavailable',false);};
  const offs=[network.onBulkControl(receive),network.on('peerConnection',check),network.on('membershipChanged',check),network.on('close',()=>close()),network.on('state',state=>{if(['closed','retired'].includes(state.state))close();else check();})];
  function close(){if(closed)return;for(const record of [...records.values()])drop(record,'closed');closed=true;for(const off of offs)off();recent.clear();}
  return {
    open(peer,{bytes=32*1024*1024}={}) {
      if(!permitted(peer)||records.has(peer)||records.size>=8)throw new Error('Bulk peer unavailable or busy');
      const id=Array.from(crypto.getRandomValues(new Uint8Array(16)),n=>n.toString(16).padStart(2,'0')).join('');
      const frame=normalizeBulkControl({version:1,type:'request',sessionId:id,purpose:'probe',bytes});if(!frame)throw new TypeError('Invalid probe size');
      const record=create(peer,id,bytes,true);
      if(valid(record)){if(!send(peer,frame))drop(record,'unavailable');else void prepare(record);}
      return record.ready;
    },
    dropPeer(peer){const record=records.get(peer);if(record)drop(record);},
    close,
    snapshot(){return [...records.values()].map(r=>({peer:r.peer,sessionId:r.id,state:r.state,bytes:r.bytes}));},
  };
}
