import {waitForDataChannelCapacity} from './data-channel-capacity.js';
import {fitsMessageLimits,maxMessagePayload} from './message-size-guard.js';
/** Shared file/scout framing and native-queue admission. Does not acknowledge receipt. */
export async function sendFileChannelPayload({dc,pcs,bytes,file,send=data=>dc.send(data),overhead=0,messageBytes=null,fragments=false,queuePolicy,timeoutMs=15000,signal,assertLive=()=>{}}){
 const header=JSON.stringify({kind:'binary',file}),headerBytes=new TextEncoder().encode(header).byteLength;
 const direct=(!messageBytes||bytes.byteLength<=messageBytes)&&fitsMessageLimits({connections:pcs,payloadBytes:bytes.byteLength,headerBytes,wrapperBytes:overhead});
 if(!direct&&!fragments)throw Error('File message exceeds the connection message limit');
 const maxPayload=direct?bytes.byteLength:Math.min(maxMessagePayload(pcs,overhead),messageBytes??Infinity);
 if(!direct&&maxPayload<1024)throw Error('Connection message limit is too small');
 let bufferWaitMs=0;
 for(let offset=0;offset<bytes.byteLength;){
  const length=Math.min(maxPayload,bytes.byteLength-offset),meta=direct?header:JSON.stringify({kind:'binary-fragment',file,offset,length});
  if(!fitsMessageLimits({connections:pcs,payloadBytes:length,headerBytes:new TextEncoder().encode(meta).byteLength,wrapperBytes:overhead,unknownPayloadLimit:direct?65536:65536-overhead}))throw Error('File fragment exceeds the connection message limit');
  const started=performance.now();
  await waitForDataChannelCapacity(dc,{bytes:length,timeoutMs,signal,...(queuePolicy?{highWaterMark:Math.max(length,queuePolicy.highWaterMark),lowWaterMark:Math.min(queuePolicy.lowWaterMark,Math.max(length,queuePolicy.highWaterMark)-length)}:{})});
  bufferWaitMs+=performance.now()-started;assertLive();send(meta);send(direct?bytes:bytes.slice(offset,offset+length));offset+=length;
 }
 return {bufferWaitMs};
}
/** FIFO queue ownership is knowable only for an explicitly reliable ordered DC. */
export function coalesceQueuedFileChunk({dc,striped=false,submittedBytes,queuedChunks,index}){
 if(striped||dc.ordered!==true||dc.maxRetransmits!==null||dc.maxPacketLifeTime!==null)return false;
 const drained=Math.max(0,submittedBytes-dc.bufferedAmount);
 for(const [chunk,end]of queuedChunks)if(end<=drained)queuedChunks.delete(chunk);
 return queuedChunks.has(index);
}
