/** Observe attempts without logging signed requests, remote errors or credentials. */
export function createMembershipControlRequest({getPeer,pairing,onControlPath=()=>{},now=()=>performance.now()}) {
 const notify=event=>{try{onControlPath(event);}catch{}};
 const attempt=async(path,channel,action,data,options)=>{
  const started=now();notify({path,action,status:'start'});
  try{const value=await channel.request(action,data,options);notify({path,action,status:'success',elapsedMs:now()-started});return value;}
  catch(error){const code=options?.signal?.aborted?'aborted':error?.code==='MEMBERSHIP_REJECTED'?'rejected':/timed out/i.test(error?.message??'')?'timeout':/closed|unavailable/i.test(error?.message??'')?'unavailable':'request_failed';
   notify({path,action,status:'failure',code,elapsedMs:now()-started});throw error;}
 };
 return async(action,data,options)=>{
  const peer=getPeer();
  if(peer?.ready())try{return await attempt('peer',peer,action,data,options);}catch(error){if(options?.signal?.aborted)throw error;}
  return attempt('pairing',pairing,action,data,options);
 };
}
