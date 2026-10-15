/** Local submission is not delivery. Native connection progress cancels fallback. */
export function createSequentialSignaling({setTimeout,clearTimeout,now=Date.now,status=()=>{},graceMs=750,limit=256}) {
 const pending=new Set();let generation=0;
 function remove(row){clearTimeout(row.timer);pending.delete(row);}
 function send({peer,attemptId,signalType,routes,current,complete}){
  const row={generation,peer,attemptId,signalType,at:now(),timer:null,index:0};
  const report=(action,route,reason)=>status({action,peer,attemptId,signalType,route,reason,elapsedMs:now()-row.at,graceMs});
  function advance(){
   clearTimeout(row.timer);row.timer=null;
   if(row.generation!==generation||!current()){remove(row);report('cancelled',null,'attempt_retired');return false;}
   if(row.index>0&&complete()){remove(row);report('cancelled',null,'native_connection_ready');return true;}
   while(row.index<routes.length){
    const route=routes[row.index++];let submitted=false;
    try{submitted=route.send()===true;}catch{}
    report(submitted?'submitted':'skipped',route.name,submitted?'local_submission':'unavailable_or_send_failed');
    if(row.generation!==generation||!current()){remove(row);return false;}
    if(!submitted)continue;
    if(row.index<routes.length){pending.add(row);row.timer=setTimeout(advance,graceMs);report('waiting',route.name,'awaiting_native_connection');}
    else remove(row);
    return true;
   }
   remove(row);return false;
  }
  if(pending.size>=limit){
   // Avoid adding timers or retaining more payloads under signaling pressure.
   if(!current())return false;
   report('fallback',routes.at(-1).name,'pending_limit');
   if(row.generation!==generation||!current())return false;
   try{return routes.at(-1).send()===true;}catch{return false;}
  }
  return advance();
 }
 return {send,clear(){generation++;for(const row of pending){remove(row);status({action:'cancelled',peer:row.peer,attemptId:row.attemptId,signalType:row.signalType,reason:'session_cleared',elapsedMs:now()-row.at});}},get pending(){return pending.size;}};
}
