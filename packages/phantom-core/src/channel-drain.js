const waits=new WeakMap();
// One owned watchdog per blocked channel, cancelled on drain or close.
export function waitForChannelDrain(dc,{schedule=setTimeout,cancel=clearTimeout,timeoutMs=5000}={}){
 if(!dc||dc.readyState!=='open')return Promise.resolve(false);
 dc.bufferedAmountLowThreshold=512*1024;
 if(dc.bufferedAmount<2*1024*1024)return Promise.resolve(true);
 if(waits.has(dc))return waits.get(dc);
 let finish,timer,settled=false;
 const result=new Promise(resolve=>{
  finish=event=>{if(settled)return;settled=true;cancel(timer);for(const type of ['bufferedamountlow','close','error'])dc.removeEventListener(type,finish);waits.delete(dc);resolve(event?.type!=='error'&&dc.readyState==='open'&&dc.bufferedAmount<2*1024*1024);};
  for(const type of ['bufferedamountlow','close','error'])dc.addEventListener(type,finish);
  timer=schedule(finish,timeoutMs);
 });
 waits.set(dc,result);return result;
}
