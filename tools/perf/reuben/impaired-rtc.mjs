import {memoryRTC} from '../../../tests/fixtures/memory-rtc.js';
/** Reliable ordered application-message model. NOT an SCTP congestion simulation.
 * Serializes bytes per direction; adds propagation and deterministic recovery stalls.
 */
export function impairedRTC({oneWayMs=0,mbps=100,recoverEvery=0}={}){
 const timers=new Set(),upgraded=new WeakSet(),state=new WeakMap(),all=[];
 function upgrade(ch){
  if(upgraded.has(ch))return ch;upgraded.add(ch);const events=new EventTarget();
  ch.ordered=true;ch.addEventListener=events.addEventListener.bind(events);ch.removeEventListener=events.removeEventListener.bind(events);ch.dispatchEvent=events.dispatchEvent.bind(events);
  const close=ch.close.bind(ch);ch.close=()=>{if(ch.readyState==='closed')return;close();events.dispatchEvent(new Event('close'));};
  return ch;
 }
 const Base=memoryRTC({onSend(data,ch){
  upgrade(ch);let row=state.get(ch);if(!row){row={ch,freeAt:performance.now(),deliveredAt:0,messages:0,peak:0,emptyAt:null,emptyMs:0,binary:0,recoveries:0,deliveries:[],deliveryTimer:false};state.set(ch,row);all.push(row);}
  const bytes=typeof data==='string'?Buffer.byteLength(data):data.byteLength,now=performance.now(),binary=data instanceof ArrayBuffer;
  if(binary){if(row.emptyAt!==null)row.emptyMs+=now-row.emptyAt;row.emptyAt=null;row.binary++;}
  ch.bufferedAmount+=bytes;row.peak=Math.max(row.peak,ch.bufferedAmount);
  const recovery=binary&&recoverEvery&&row.binary%recoverEvery===0;
  const finish=Math.max(now,row.freeAt)+bytes*8/(mbps*1000)+(recovery?2*oneWayMs+1200*8/(mbps*1000):0);row.freeAt=finish;row.messages++;if(recovery)row.recoveries++;
  const schedule=(fn,at)=>{const t=setTimeout(()=>{timers.delete(t);fn();},Math.max(0,at-performance.now()));timers.add(t);};
  schedule(()=>{const previous=ch.bufferedAmount;ch.bufferedAmount=Math.max(0,previous-bytes);if(!ch.bufferedAmount&&row.binary)row.emptyAt=performance.now();if(previous>(ch.bufferedAmountLowThreshold??0)&&ch.bufferedAmount<=(ch.bufferedAmountLowThreshold??0))ch.dispatchEvent(new Event('bufferedamountlow'));},finish);
  // Preserve ordered delivery even across timers rounded to the same millisecond.
  row.deliveredAt=Math.max(row.deliveredAt,finish+oneWayMs);
  row.deliveries.push({data,at:row.deliveredAt});
  const deliver=()=>{row.deliveryTimer=false;while(row.deliveries.length&&row.deliveries[0].at<=performance.now()){
    const next=row.deliveries.shift();if(ch.other?.readyState!=='open')continue;upgrade(ch.other);ch.other.onmessage?.({data:next.data});ch.other.dispatchEvent(new MessageEvent('message',{data:next.data}));
   }if(row.deliveries.length){row.deliveryTimer=true;schedule(deliver,row.deliveries[0].at);}};
  if(!row.deliveryTimer){row.deliveryTimer=true;schedule(deliver,row.deliveries[0].at);}
  return false;
 }});
 class RTC extends Base {
  createDataChannel(label){return upgrade(super.createDataChannel(label));}
  fire(type,event={}){if(type==='datachannel')upgrade(event.channel);super.fire(type,event);}
 }
 return {RTC,metrics(){return all.filter(r=>r.binary).map(r=>({label:r.ch.label,binaryMessages:r.binary,peakBufferedBytes:r.peak,emptyBufferMs:Math.round(r.emptyMs+(r.emptyAt===null?0:performance.now()-r.emptyAt)),recoveryStalls:r.recoveries}));},close(){for(const t of timers)clearTimeout(t);timers.clear();}};
}
