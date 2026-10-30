/** Finite native probe adapter for the installed extension/helper.
 * This accepts a narrow message port, not arbitrary socket or executable APIs.
 * All payload generation/I/O stays native. Offers remain private control data.
 */
export function createHelperProbeAdapter({port,crypto=globalThis.crypto,timeoutMs=30000}) {
  if(!port?.send||!port?.subscribe||!Number.isInteger(timeoutMs)||timeoutMs<1||timeoutMs>120000)throw new TypeError('Invalid helper port');
  const pending=new Map(),jobs=new Map();let closed=false;
  const nonce=()=>Array.from(crypto.getRandomValues(new Uint8Array(16)),n=>n.toString(16).padStart(2,'0')).join('');
  function rejectJob(job){const error=new Error('Native helper unavailable');job.reject(error);}
  function closeJob(job,tell=true){if(jobs.get(job.id)!==job)return;jobs.delete(job.id);job.signal?.removeEventListener('abort',job.abort);rejectJob(job);for(const [id,p] of pending)if(p.jobId===job.id){clearTimeout(p.timer);pending.delete(id);p.reject(new Error('Native helper cancelled'));}if(tell&&!closed)void rpc({action:'close',jobId:job.id}).catch(()=>{});}
  function rpc(body){
    if(closed)return Promise.reject(new Error('Native helper closed'));
    if(pending.size>=16){close();return Promise.reject(new Error('Native helper request limit'));}
    const id=nonce();return new Promise((resolve,reject)=>{
      const timer=setTimeout(()=>{pending.delete(id);reject(new Error('Native helper deadline'));},timeoutMs);timer.unref?.();
      pending.set(id,{resolve,reject,timer,jobId:body.jobId});
      try{port.send({...body,id});}catch{clearTimeout(timer);pending.delete(id);reject(new Error('Native helper unavailable'));}
    });
  }
  function receive(body){
    if(closed||!body||typeof body!=='object')return;
    if(body.event==='unavailable'){close();return;}
    if(body.id){const p=pending.get(body.id);if(!p)return;pending.delete(body.id);clearTimeout(p.timer);body.ok===true?p.resolve(body):p.reject(new Error('Native helper rejected operation'));return;}
    const job=jobs.get(body.jobId);if(!job)return;
    if(body.event==='complete'){
      const r=body.result;
      if(r?.bytes!==job.bytes||!/^[0-9a-f]{64}$/.test(r.sha256)||!Number.isFinite(r.elapsedSeconds)||r.elapsedSeconds<=0){closeJob(job);return;}
      job.resolve({bytes:r.bytes,sha256:r.sha256,elapsedSeconds:r.elapsedSeconds,MBps:r.MBps??r.mbps,localReceiveDrops:r.localReceiveDrops,localSendDrops:r.localSendDrops});closeJob(job,false);
    }else if(body.event==='failed')closeJob(job,false);
  }
  const off=port.subscribe(receive);
  function close(){if(closed)return;closed=true;for(const job of [...jobs.values()])closeJob(job,false);for(const p of pending.values()){clearTimeout(p.timer);p.reject(new Error('Native helper closed'));}pending.clear();off();port.close?.();}
  return {
    async prepare({localPeer,remotePeer,sessionId,bytes,initiator,signal}){
      signal?.throwIfAborted();
      if(closed||jobs.size>=4||!/^[0-9a-f]{16}$/.test(localPeer)||!/^[0-9a-f]{16}$/.test(remotePeer)||!/^[0-9a-f]{32}$/.test(sessionId)||!Number.isInteger(bytes)||bytes<1||bytes>32*1024*1024)throw new Error('Invalid helper scope');
      const id=nonce();let resolve,reject;const result=new Promise((a,b)=>{resolve=a;reject=b;});result.catch(()=>{});
      const job={id,bytes,resolve,reject,signal,abort:()=>closeJob(job),connecting:false};jobs.set(id,job);signal?.addEventListener('abort',job.abort,{once:true});
      try {
        const reply=await rpc({action:'prepare',jobId:id,localPeer,remotePeer,sessionId,bytes,initiator});
        if(reply.probeVersion!==2)throw new Error('Native helper probe version mismatch');
        if(jobs.get(id)!==job||signal?.aborted)throw new Error('Native helper cancelled');
        return {offer:reply.offer,close:()=>closeJob(job),async connect(offer){
          if(jobs.get(id)!==job||job.connecting)throw new Error('Native helper session consumed');job.connecting=true;
          try{await rpc({action:'connect',jobId:id,offer});if(jobs.get(id)!==job)throw new Error('Native helper closed');return {result,close:()=>closeJob(job)};}
          catch(error){closeJob(job);throw error;}
        }};
      }catch(error){closeJob(job);throw error;}
    },close,
  };
}

export function createBrowserHelperAdapter({window=globalThis.window,crypto=globalThis.crypto,timeoutMs}={}) {
  if(window?.location?.origin!=='https://phantomshare.app'||window.top!==window)throw new Error('Phantom helper requires its trusted top-level page');
  const listeners=new Set();
  const receive=event=>{if(event.source===window&&event.origin===window.location.origin&&event.data?.channel==='phantom-native/1'&&event.data?.direction==='reply')for(const fn of listeners)fn(event.data.body);};
  window.addEventListener('message',receive);
  return createHelperProbeAdapter({crypto,timeoutMs,port:{send:body=>window.postMessage({channel:'phantom-native/1',direction:'request',body},window.location.origin),subscribe(fn){listeners.add(fn);return()=>listeners.delete(fn);},close(){window.postMessage({channel:'phantom-native/1',direction:'disconnect'},window.location.origin);window.removeEventListener('message',receive);listeners.clear();}}});
}
