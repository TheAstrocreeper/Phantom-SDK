// A test adapter; a consumed ACK means the browser stream accepted the bytes,
// not that the download was saved successfully.
export async function createDownloadSink(size) {
  const registration = await navigator.serviceWorker.register('/tools/perf/continuous-prototype/download-worker.js');
  // The test page is outside this worker's scope; wait for its registration,
  // not navigator.serviceWorker.ready (which waits for a page controller).
  let worker = registration.active;
  if (!worker) {
    worker = registration.installing || registration.waiting;
    if (!worker) throw Error('Download worker unavailable');
    await new Promise((resolve,reject)=>{
      const changed=()=>{if(worker.state==='activated'){worker.removeEventListener('statechange',changed);resolve();}else if(worker.state==='redundant'){worker.removeEventListener('statechange',changed);reject(Error('Download worker activation failed'));}};
      worker.addEventListener('statechange',changed);changed();
    });
  }
  if (!navigator.serviceWorker.controller) await new Promise((resolve,reject)=>{const timeout=setTimeout(()=>reject(Error('Download worker did not take control')),10000);navigator.serviceWorker.addEventListener('controllerchange',()=>{clearTimeout(timeout);resolve();},{once:true});});
  const id = crypto.randomUUID(), pair = new MessageChannel(), pending = new Map();
  window.streamDebug={messages:[],puts:0};
  let finishResolve, finishReject, rejected;
  let registerResolve; const registered = new Promise(resolve => {registerResolve = resolve;});
  pair.port1.onmessage = e => {
    const m = e.data; if(window.streamDebug.messages.length<30)window.streamDebug.messages.push(m.type==='debug'?m:m.type);
    if (m.type === 'registered') registerResolve();
    else if (m.type === 'consumed') { pending.get(m.index)?.resolve(); pending.delete(m.index); }
    else if (m.type === 'closed') finishResolve?.();
    else if (m.type === 'cancel') { rejected = Error(m.error || 'Download cancelled'); for (const p of pending.values()) p.reject(rejected); pending.clear(); finishReject?.(rejected); }
  };
  worker.postMessage({type:'register',id,size},[pair.port2]);
  await registered;
  return {
    start(){const a=document.createElement('a');a.href=`/tools/perf/continuous-prototype/download/${id}`;document.body.append(a);a.click();a.remove();},
    put(_id,index,bytes){window.streamDebug.puts++;if(rejected)return Promise.reject(rejected);return new Promise((resolve,reject)=>{const transfer=bytes.slice(0);pending.set(index,{resolve,reject});pair.port1.postMessage({type:'chunk',index,bytes:transfer},[transfer]);});},
    finish(){return new Promise((resolve,reject)=>{finishResolve=resolve;finishReject=reject;pair.port1.postMessage({type:'end'});});},
    abort(error){pair.port1.postMessage({type:'abort',error:String(error)});}
  };
}
