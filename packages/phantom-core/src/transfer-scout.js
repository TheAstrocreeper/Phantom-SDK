import {defaultRTCConfiguration} from './rtc-configuration.js';
import {scoutConnection} from './connection-scout.js';

export const TRANSFER_SCOUT_PROTOCOL = 'phantom-transfer-scout-v1';
const LANES = 3, MAX_BYTES = 3 * 1024 * 1024, DEFAULT_BYTES = 768 * 1024;
const validId = id => typeof id === 'string' && /^[a-zA-Z0-9-]{1,64}$/.test(id);

/** Compare repeated, equal-byte deliveries. Overlapping observations are inconclusive.
 * This is evidence about a short synthetic transfer, never a link-capacity estimate.
 */
export function selectTransferSetup(samples, fallback = 3) {
  const groups = [1, 3].map(connections => samples.filter(s => s.connections === connections));
  if (![1, 3].includes(fallback) || samples.length !== 4 || groups.some(g => g.length !== 2) ||
      samples.some(s => !Number.isSafeInteger(s.bytes) || s.bytes < 1 || !Number.isFinite(s.elapsedMs) || s.elapsedMs <= 0) ||
      new Set(samples.map(s => s.bytes)).size !== 1) return {status:'inconclusive',connections:fallback,reason:'insufficient_comparable_samples'};
  const ranges = groups.map(g => ({min:Math.min(...g.map(s => s.elapsedMs)),max:Math.max(...g.map(s => s.elapsedMs))}));
  const winner = ranges[0].max < ranges[1].min ? 1 : ranges[1].max < ranges[0].min ? 3 : null;
  return {status:winner ? 'measured' : 'inconclusive',connections:winner ?? fallback,
    reason:winner ? 'repeated_delivery_times_do_not_overlap' : 'delivery_time_ranges_overlap',ranges};
}

/** Bounded pre-transfer exploration on separate, real RTC associations.
 * No retries, sleeps, polling, or timers. One active peer per local session.
 * A silent peer can occupy that bounded slot until cancellation/disconnection;
 * it never blocks file selection. Payloads are counted and discarded on receipt.
 */
export function createTransferScout({self,allowed,send,runtime=globalThis,configuration,
  onReport=()=>{},busy=()=>false,sampleBytes=DEFAULT_BYTES,fallback=3,newId=()=>runtime.crypto.randomUUID()}) {
  if (![self,allowed,send,onReport,busy,newId].every(f => typeof f === 'function') ||
      !Number.isInteger(sampleBytes) || sampleBytes < 3 * 1024 || sampleBytes > MAX_BYTES || sampleBytes % 3 ||
      ![1,3].includes(fallback)) throw TypeError('Invalid transfer scout configuration');
  let active=null,closed=false,suspended=false;
  const attempted=new Set(),results=new Map();
  const now=()=>runtime.performance?.now() ?? performance.now();
  const permitted=peer=>!closed&&!suspended&&typeof peer==='string'&&/^[0-9a-f]{16}$/.test(peer)&&peer!==self()&&allowed(peer);
  const live=s=>active===s&&!closed&&!s.stopped;
  const notify=(s,report)=>onReport({peer:s.peer,id:s.id,scope:'synthetic_transfer_connections',...report});
  function wire(s,body){if(live(s))send(s.peer,{type:'transferScout',version:1,id:s.id,...body});}
  function retire(s,reason,notifyPeer=true) {
    if(!live(s))return;
    if(s.finishing&&['channel_closed','connection_closed','connection_failed'].includes(reason))reason='complete';
    if(notifyPeer)wire(s,{action:'cancel',reason});
    s.stopped=true;active=null;
    s.ack?.reject(Error(reason));s.ack=null;
    for(const lane of s.lanes){
      lane.pc.onicecandidate=null;lane.pc.onconnectionstatechange=null;lane.pc.ondatachannel=null;
      if(lane.dc){lane.dc.onopen=null;lane.dc.onclose=null;lane.dc.onerror=null;lane.dc.onmessage=null;try{lane.dc.close();}catch{}}
      try{lane.pc.close();}catch{}
    }
    if(reason!=='complete') {results.delete(s.peer);notify(s,{phase:'preflight',status:'cancelled',reason,samples:s.samples});}
  }
  function fail(s,error){retire(s,error?.message==='Invalid probe data'?'invalid_probe_data':'probe_failed');}
  function snapshot(s,phase,round) {
    for(const [lane,row] of s.lanes.entries())void scoutConnection(row.pc,{channel:row.dc})
      .then(report=>{if(!closed)notify(s,{phase,lane,round,report});}).catch(()=>{});
  }
  function create(peer,id,initiator,bytes,reverse) {
    const s={peer,id,initiator,bytes,reverse,lanes:[],ready:new Set(),started:false,stopped:false,
      receiving:null,nextReceive:initiator?5:0,samples:[],ack:null,running:false,receivedRounds:0,payload:new Uint8Array(65536)};
    active=s;attempted.add(peer);results.delete(peer);
    runtime.crypto.getRandomValues(s.payload);
    for(let lane=0;lane<LANES;lane++) {
      const pc=new runtime.RTCPeerConnection(configuration??defaultRTCConfiguration()),row={pc,dc:null,ice:[],signals:Promise.resolve(),signalCount:0};s.lanes.push(row);
      pc.onicecandidate=e=>{if(e.candidate)wire(s,{action:'signal',lane,candidate:e.candidate.toJSON?.()??e.candidate});};
      pc.onconnectionstatechange=()=>{if(['failed','closed','disconnected'].includes(pc.connectionState))retire(s,'connection_'+pc.connectionState);};
      pc.ondatachannel=e=>bind(s,lane,e.channel);
    }
    notify(s,{phase:'preflight',status:'started',sampleBytes:bytes,candidates:[1,3],roundsPerDirection:4,
      payloadBudgetBytes:10*bytes,order:order(s),messageCeilingBytes:65536});
    return s;
  }
  const order=s=>s.reverse?[3,1,1,3]:[1,3,3,1];
  function specification(s,round) {
    const phase=round%5,connections=phase===0?3:order(s)[phase-1];
    return {connections,perLane:s.bytes/connections};
  }
  function maybeStart(s) {
    if(!live(s)||!s.initiator||s.started||s.ready.size!==LANES||s.lanes.some(r=>r.dc?.readyState!=='open'))return;
    s.started=true;snapshot(s,'preflight-connected',null);void run(s,0).catch(e=>fail(s,e));
  }
  function bind(s,lane,dc) {
    if(!live(s)||s.lanes[lane].dc||dc.label!=='phantom-distribution-scout'||dc.ordered===false||dc.maxRetransmits!=null||dc.maxPacketLifeTime!=null){try{dc.close();}catch{}return;}
    const row=s.lanes[lane];row.dc=dc;dc.binaryType='arraybuffer';
    const opened=()=>{if(!live(s))return;wire(s,{action:'ready',lane});maybeStart(s);};
    dc.onopen=opened;dc.onclose=()=>retire(s,'channel_closed');dc.onerror=()=>retire(s,'channel_error');
    dc.onmessage=e=>{try{receiveData(s,lane,e.data);}catch(error){fail(s,error);}};
    if(dc.readyState==='open')opened();
  }
  function receiveData(s,lane,data) {
    if(!live(s))return;
    if(typeof data==='string') {
      if(data.length>512)throw Error('Invalid probe data');
      const body=JSON.parse(data);
      if(body.action==='sample') {
        if(s.initiator?s.samples.length!==4:s.started)throw Error('Invalid probe data');
        if(body.round!==s.nextReceive||!Number.isInteger(body.round)||body.round<0||body.round>9)throw Error('Invalid probe data');
        const spec=specification(s,body.round);
        if(lane>=spec.connections||body.bytes!==spec.perLane)throw Error('Invalid probe data');
        s.receiving??={round:body.round,counts:new Map(),spec};
        if(s.receiving.counts.has(lane))throw Error('Invalid probe data');
        s.receiving.counts.set(lane,0);return;
      }
      if(lane!==0)throw Error('Invalid probe data');
      if(body.action==='ack'&&s.ack?.round===body.round){const ack=s.ack;s.ack=null;ack.resolve();return;}
      if(body.action==='turn'&&!s.initiator&&s.nextReceive===5&&!s.started){s.started=true;void run(s,5).catch(e=>fail(s,e));return;}
      if(body.action==='finished'&&s.initiator&&s.nextReceive===10){s.finishing=true;rowSend(s,0,{action:'finishAck'});return;}
      if(body.action==='finishAck'&&!s.initiator&&s.running===false&&s.samples.length===4){wire(s,{action:'complete'});retire(s,'complete',false);return;}
      throw Error('Invalid probe data');
    }
    const receiving=s.receiving;
    if(!(data instanceof ArrayBuffer)||data.byteLength<1||data.byteLength>65536||!receiving?.counts.has(lane))throw Error('Invalid probe data');
    const count=receiving.counts.get(lane)+data.byteLength;
    if(count>receiving.spec.perLane)throw Error('Invalid probe data');
    receiving.counts.set(lane,count);
    if(receiving.counts.size===receiving.spec.connections&&[...receiving.counts.values()].every(n=>n===receiving.spec.perLane)) {
      s.nextReceive++;s.receivedRounds++;s.receiving=null;rowSend(s,0,{action:'ack',round:receiving.round});
    }
  }
  function rowSend(s,lane,body){if(!live(s)||s.lanes[lane].dc?.readyState!=='open')throw Error('Probe channel unavailable');s.lanes[lane].dc.send(JSON.stringify(body));}
  async function run(s,base) {
    s.running=true;
    for(let round=base;round<base+5;round++) {
      if(!live(s)||busy())throw Error('Probe superseded');
      const {connections,perLane}=specification(s,round);
      const ack=new Promise((resolve,reject)=>{s.ack={round,resolve,reject};});
      void ack.catch(()=>{}); // Cancellation may happen while send() itself throws.
      // All payloads fit a byte-bounded round (<=3 MiB total), below the file
      // sender's existing 16 MiB high-water mark. No unbounded enqueue loop.
      const payloads=s.lanes.slice(0,connections).map(row=>{
        const limit=row.pc.sctp?.maxMessageSize;
        const size=Number.isFinite(limit)&&limit>0?Math.min(65536,Math.floor(limit)):65536;
        if(size<1024)throw Error('Probe message limit too small');
        return s.payload.buffer.slice(0,size);
      });
      const messageBytes=payloads.map(p=>p.byteLength);
      const before=s.lanes.map(r=>r.dc.bufferedAmount),startedAt=now();
      for(let lane=0;lane<connections;lane++) {
        const payload=payloads[lane],size=payload.byteLength;rowSend(s,lane,{action:'sample',round,bytes:perLane});
        for(let offset=0;offset<perLane;offset+=size)s.lanes[lane].dc.send(offset+size<=perLane?payload:payload.slice(0,perLane-offset));
      }
      const queued=s.lanes.map(r=>r.dc.bufferedAmount);
      await ack;
      if(!live(s))return;
      const elapsedMs=now()-startedAt;
      const sample={round,connections,bytes:s.bytes,elapsedMs,messageBytes,bufferedBefore:before,bufferedAfterEnqueue:queued,
        bufferedAfterAck:s.lanes.map(r=>r.dc.bufferedAmount),bytesPerSecond:elapsedMs>0?s.bytes*1000/elapsedMs:null,warmup:round%5===0};
      if(!sample.warmup)s.samples.push(sample);
      notify(s,{phase:'preflight-sample',direction:'outgoing',...sample});snapshot(s,'preflight-sample-stats',round);
    }
    s.running=false;
    const selection=selectTransferSetup(s.samples,fallback);
    const result={version:1,id:s.id,scope:'synthetic_transfer_connections',direction:'outgoing',...selection,
      sampleBytes:s.bytes,samples:structuredClone(s.samples),validFor:'current_peer_connection_generation',
      limitations:['short_samples_include_ack_latency','separate_associations_from_file','does_not_measure_disk_or_hashing','does_not_establish_link_capacity']};
    results.set(s.peer,result);notify(s,{phase:'preflight-result',...result});
    if(base===0)rowSend(s,0,{action:'turn'});else rowSend(s,0,{action:'finished'});
  }
  async function signal(s,lane,body) {
    if(!live(s))return;
    const row=s.lanes[lane];
    if(body.description) {
      const d=body.description;
      if(!['offer','answer'].includes(d.type)||typeof d.sdp!=='string'||d.sdp.length>24000||d.type!==(s.initiator?'answer':'offer')||row.pc.remoteDescription)throw Error('Invalid probe signal');
      await row.pc.setRemoteDescription(d);if(!live(s))return;
      for(const candidate of row.ice.splice(0))await row.pc.addIceCandidate(candidate);
      if(!s.initiator){await row.pc.setLocalDescription(await row.pc.createAnswer());wire(s,{action:'signal',lane,description:{type:'answer',sdp:row.pc.localDescription.sdp}});}
    } else if(body.candidate) {
      const c=body.candidate;
      if(typeof c.candidate!=='string'||c.candidate.length>2048||row.ice.length>=32)throw Error('Invalid probe candidate');
      if(row.pc.remoteDescription)await row.pc.addIceCandidate(c);else row.ice.push(c);
    }
  }
  return {
    start(peer) {
      if(!permitted(peer)||self()>peer||busy()||active||attempted.has(peer)||attempted.size>=128)return false;
      const id=newId();if(!validId(id))throw Error('Invalid probe id');
      let s;
      try {
        s=create(peer,id,true,sampleBytes,parseInt(id.replaceAll('-','').slice(-1),16)%2===1);
        wire(s,{action:'init',sampleBytes:s.bytes,reverse:s.reverse});
        for(const [lane,row]of s.lanes.entries()) {
          bind(s,lane,row.pc.createDataChannel('phantom-distribution-scout',{ordered:true}));
          void (async()=>{await row.pc.setLocalDescription(await row.pc.createOffer());wire(s,{action:'signal',lane,description:{type:'offer',sdp:row.pc.localDescription.sdp}});})().catch(e=>fail(s,e));
        }
        return true;
      }catch(e){if(active)retire(active,'setup_failed');return false;}
    },
    receive(peer,body) {
      if(!permitted(peer)||body?.type!=='transferScout'||body.version!==1||!validId(body.id))return false;
      if(body.action==='init') {
        if(self()<peer||active||busy()||attempted.has(peer)||attempted.size>=128||!Number.isInteger(body.sampleBytes)||body.sampleBytes<3072||body.sampleBytes>sampleBytes||body.sampleBytes%3||typeof body.reverse!=='boolean')return false;
        try{create(peer,body.id,false,body.sampleBytes,body.reverse);}catch{if(active)retire(active,'setup_failed');}return true;
      }
      const s=active;if(!s||s.peer!==peer||s.id!==body.id)return false;
      if(body.action==='cancel'){retire(s,'peer_cancelled',false);return true;}
      if(body.action==='complete'&&s.initiator&&s.nextReceive===10){retire(s,'complete',false);return true;}
      if(!Number.isInteger(body.lane)||body.lane<0||body.lane>=LANES)return false;
      if(body.action==='ready'){s.ready.add(body.lane);maybeStart(s);return true;}
      if(body.action==='signal'){const row=s.lanes[body.lane];if(++row.signalCount>64){retire(s,'signal_budget_exceeded');return false;}row.signals=row.signals.then(()=>signal(s,body.lane,body)).catch(e=>fail(s,e));return true;}
      return false;
    },
    get(peer){const result=results.get(peer);return result?structuredClone(result):null;},
    suspend(reason='file_selected'){suspended=true;if(active)retire(active,reason);},
    drop(peer){if(active?.peer===peer)retire(active,'peer_disconnected',false);results.delete(peer);attempted.delete(peer);},
    clear(){if(active)retire(active,'generation_changed',false);results.clear();attempted.clear();suspended=false;},
    close(){if(active)retire(active,'session_closed');closed=true;results.clear();attempted.clear();},
  };
}
