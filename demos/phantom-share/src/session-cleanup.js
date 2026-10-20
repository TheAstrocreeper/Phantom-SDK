/** Local exit must finish even if the remote authority cannot confirm Leave. */
export async function leaveAndReset({leave,reset,onError=()=>{},timeoutMs=2000}) {
 let timer,confirmed=false;
 try {
  await Promise.race([Promise.resolve().then(leave),new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('Leave confirmation timed out')),timeoutMs);})]);
  confirmed=true;
 }catch(error){onError(error);}
 finally{clearTimeout(timer);reset(confirmed);}
 return confirmed;
}


/** Start every local disposal even if one owner throws or awaits a native ack. */
export async function closeSessionOwners(owners,onError=()=>{}) {
 const pending=owners.map(owner=>{
  try{return Promise.resolve(owner?.close());}
  catch(error){return Promise.reject(error);}
 });
 for(const result of await Promise.allSettled(pending))if(result.status==='rejected'){
  try{onError(result.reason);}catch{}
 }
}
