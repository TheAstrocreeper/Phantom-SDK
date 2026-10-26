import {createFileDistribution} from '../src/files.js';
import {waitForDataChannelCapacity} from '../src/data-channel-capacity.js';

const MAGIC=0x52424e31,HEADER=44,MAX_PAYLOAD=65536;
/** RBN1: magic/u32 index/u32 payload length/32-byte file digest/payload. */
export function encodeReubenFrame(file,bytes){
 if(!/^sha256:[0-9a-f]{64}$/.test(file?.id)||!Number.isInteger(file.index)||file.index<0||file.index>0xffffffff||!(bytes instanceof ArrayBuffer)||bytes.byteLength<1||bytes.byteLength>MAX_PAYLOAD)throw new TypeError('Invalid Reuben chunk');
 const frame=new ArrayBuffer(HEADER+bytes.byteLength),view=new DataView(frame),out=new Uint8Array(frame);
 view.setUint32(0,MAGIC);view.setUint32(4,file.index);view.setUint32(8,bytes.byteLength);
 for(let i=0;i<32;i++)out[12+i]=parseInt(file.id.slice(7+i*2,9+i*2),16);
 out.set(new Uint8Array(bytes),HEADER);return frame;
}
export function decodeReubenFrame(frame){
 if(!(frame instanceof ArrayBuffer)||frame.byteLength<=HEADER||frame.byteLength>HEADER+MAX_PAYLOAD)throw new TypeError('Invalid Reuben frame');
 const view=new DataView(frame);
 if(view.getUint32(0)!==MAGIC||view.getUint32(8)!==frame.byteLength-HEADER)throw new TypeError('Invalid Reuben frame header');
 const id='sha256:'+Array.from(new Uint8Array(frame,12,32),b=>b.toString(16).padStart(2,'0')).join('');
 return {file:{id,index:view.getUint32(4)},bytes:frame.slice(HEADER)};
}
/** Experimental adapter over an already admitted, reliable ordered channel.
 * Reuses SDK manifests, hashing, bounded source preparation, writes and resume.
 * Whole-file request uses its rolling request window, without coordinator batches.
 * Caller owns admission, publication authorization and the channel lifecycle.
 */
export function createReubenTransfer({channel,peerId,crypto=globalThis.crypto,chunkStore,allowed=()=>true,canUsePeer=()=>true,emit=()=>{},requestWindow=32,highWaterMark=16*1024*1024,lowWaterMark=4*1024*1024,requestTimeoutMs=15000,maxFileBytes,maxMessageSize=65536,signal}){
 if(!channel?.addEventListener||!channel?.send||! /^[0-9a-f]{16}$/.test(peerId)||channel.ordered===false||channel.maxRetransmits!=null||channel.maxPacketLifeTime!=null)throw new TypeError('Supply an admitted reliable ordered channel and peer');
 if(!Number.isSafeInteger(maxMessageSize)||maxMessageSize<HEADER+1024||!Number.isSafeInteger(highWaterMark)||highWaterMark<Math.min(MAX_PAYLOAD+HEADER,maxMessageSize)||!Number.isSafeInteger(lowWaterMark)||lowWaterMark<0||lowWaterMark>=highWaterMark)throw new TypeError('Invalid Reuben frame or buffer limits');
 let closed=false,service;const listeners=new Set(),controller=new AbortController();
 const fire=(type,payload)=>{for(const fn of listeners)fn({type,payload});};
 const usable=()=>!closed&&channel.readyState==='open'&&canUsePeer(peerId);
 const core={onEvent(fn){listeners.add(fn);return()=>listeners.delete(fn);},command(type,payload){
  if(type==='SEND_DM'){
   if(!usable())throw Error('Reuben channel unavailable');
   channel.send(JSON.stringify({reuben:1,sys:payload.sys,file:payload.file}));return;
  }
  if(type!=='SEND_FILE_BIN_CHUNK')return;
  void (async()=>{let ok=false,error,bufferWaitMs=0;
   try{
    if(!usable()||!allowed(payload.file.id,payload.file.distributionId))throw Error('Reuben unavailable');
    const frame=encodeReubenFrame(payload.file,payload.bytes),at=performance.now();
    if(frame.byteLength>maxMessageSize)throw Error('Reuben frame exceeds negotiated message limit');
    await waitForDataChannelCapacity(channel,{bytes:frame.byteLength,highWaterMark,lowWaterMark,timeoutMs:requestTimeoutMs,signal:controller.signal});
    bufferWaitMs=performance.now()-at;
    if(!usable()||!allowed(payload.file.id,payload.file.distributionId))throw Error('Reuben unavailable');
    channel.send(frame);ok=true;
   }catch(e){error=e.message;}
   fire('FILE_BIN_CHUNK_SENT',{...payload,ok,error,bufferWaitMs});
  })();
 }};
 service=createFileDistribution({core,crypto,chunkStore,allowed,canUsePeer:p=>p===peerId&&usable(),emit:(type,data)=>{
  emit(type,data);
  if(type==='fileComplete'&&usable()){
   const m=service.getManifest(data.id);
   try{channel.send(JSON.stringify({reuben:1,sys:'REUBEN_RECEIPT',file:{id:m.id,hashRoot:m.hashRoot,total:m.total,bytes:m.size}}));}catch(error){emit('reubenReceiptError',{error:error.message});}
  }
 },requestWindow,requestTimeoutMs,...(maxFileBytes===undefined?{}:{maxFileBytes})});
 const receive=event=>{
  if(!usable())return;
  try{
   if(event.data instanceof ArrayBuffer){if(event.data.byteLength>maxMessageSize)throw Error('Reuben frame exceeds negotiated message limit');const chunk=decodeReubenFrame(event.data);fire('FILE_BIN_CHUNK',{from:peerId,...chunk});return;}
   if(typeof event.data!=='string'||event.data.length>65536)throw Error('Invalid Reuben control frame');
   const m=JSON.parse(event.data);
   if(m.reuben===1&&m.sys==='REUBEN_RECEIPT'){
    const manifest=service.getManifest(m.file?.id);
    if(m.file.hashRoot!==manifest.hashRoot||m.file.total!==manifest.total||m.file.bytes!==manifest.size||!allowed(manifest.id,manifest.distributionId))throw Error('Invalid Reuben receipt');
    emit('reubenReceipt',{id:manifest.id,total:manifest.total,bytes:manifest.size});return;
   }
   if(m.reuben!==1||!['FILE_OFFER','PHANTOM_FILE_MANIFEST','PHANTOM_FILE_REQUEST','PHANTOM_FILE_MISSING'].includes(m.sys))throw Error('Invalid Reuben control message');
   fire('INCOMING_MSG',{from:peerId,sys:m.sys,file:m.file});
  }catch(error){emit('reubenProtocolError',{error:error.message});close();}
 };
 function close(){if(closed)return;closed=true;controller.abort();service.close();channel.removeEventListener('message',receive);channel.removeEventListener('close',close);channel.removeEventListener('error',close);signal?.removeEventListener('abort',close);listeners.clear();}
 channel.addEventListener('message',receive);channel.addEventListener('close',close);channel.addEventListener('error',close);signal?.addEventListener('abort',close,{once:true});if(signal?.aborted)close();
 return {publish(file,options={}){const chunkSize=options.chunkSize??Math.min(MAX_PAYLOAD,maxMessageSize-HEADER);if(!Number.isInteger(chunkSize)||chunkSize<1024||chunkSize>Math.min(MAX_PAYLOAD,maxMessageSize-HEADER))throw new TypeError('Chunk exceeds Reuben message limit');return service.publish(file,{...options,chunkSize});},share:service.share,request:service.request,cancel:service.cancel,getManifest:service.getManifest,close};
}
