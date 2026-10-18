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
