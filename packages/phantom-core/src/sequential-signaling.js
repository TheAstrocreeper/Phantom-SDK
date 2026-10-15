/** One route and fallback deadline per native attempt; submission is not delivery. */
export function createSequentialSignaling({setTimeout,clearTimeout,now=Date.now,status=()=>{},graceMs=750,limit=256}) {
  const attempts=new Map();
  const pending=()=>[...attempts.values()].reduce((sum,row)=>sum+row.messages.length,0);
  function report(row,message,action,route,reason) {
    status({action,peer:row.peer,attemptId:row.attemptId,signalType:message.signalType,
      route,reason,elapsedMs:now()-row.at,graceMs});
  }
  function cancel(peer,reason='attempt_retired') {
    const row=attempts.get(peer);
    if(!row)return;
    attempts.delete(peer);
    clearTimeout(row.timer);
    for(const message of row.messages)report(row,message,'cancelled',null,reason);
    row.messages.length=0;
  }
  function active(row) {
    if(attempts.get(row.peer)!==row)return false;
    if(!row.current()){cancel(row.peer);return false;}
    return true;
  }
  function submit(row,message) {
    const route=message.routes[row.index];
    let submitted=false,reason='unavailable_or_send_failed';
    try{submitted=route.send()===true;}catch(error){reason=error?.message||reason;}
    report(row,message,submitted?'submitted':'skipped',route.name,submitted?'local_submission':reason);
    return submitted;
  }
  function advance(row) {
    clearTimeout(row.timer);
    row.timer=null;
    if(!active(row))return false;
    if(row.index>=0&&row.complete()){cancel(row.peer,'native_connection_ready');return true;}
    while(++row.index<row.routeCount) {
      let submitted=false;
      for(const message of [...row.messages]) {
        if(!active(row))return false;
        submitted=submit(row,message)||submitted;
      }
      if(!active(row))return false;
      if(row.index===row.routeCount-1) {
        // Keep only the selected route until teardown; release retained payloads.
        row.messages.length=0;
        return submitted;
      }
      if(submitted) {
        row.timer=setTimeout(()=>advance(row),graceMs);
        report(row,row.messages[0],'waiting',row.messages[0].routes[row.index].name,'awaiting_native_connection');
        return active(row);
      }
    }
    return false;
  }
  function send(message) {
    const {peer,attemptId,current,complete,routes}=message;
    let row=attempts.get(peer);
    if(row&&(row.attemptId!==attemptId||!row.current())){cancel(peer);row=null;}
    if(!current())return false;
    if(pending()>=limit||(!row&&attempts.size>=limit)) {
      status({action:'rejected',peer,attemptId,signalType:message.signalType,reason:'pending_limit'});
      return false;
    }
    if(!row) {
      row={peer,attemptId,current,complete,at:now(),timer:null,index:-1,routeCount:routes.length,messages:[message]};
      attempts.set(peer,row);
      return advance(row);
    }
    if(row.index===row.routeCount-1)return submit(row,message);
    row.messages.push(message);
    const submitted=submit(row,message);
    if(!active(row))return false;
    // New ICE uses the attempt's selected route without starting another clock.
    return submitted||advance(row);
  }
  return {send,cancel,clear(){for(const peer of [...attempts.keys()])cancel(peer,'session_cleared');},get pending(){return pending();}};
}
