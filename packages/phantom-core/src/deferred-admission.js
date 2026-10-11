/** Drain only after commit; every request must still own its live transport. */
export function resumeDeferredAdmissions(requests,{hasPending,currentEntry,pipeOpen,apply,onRetired=()=>{}}) {
 if(hasPending())return;
 for(const [member,request] of requests){
  requests.delete(member);
  if(currentEntry(request.key)!==request.entry || !pipeOpen(request.key)){onRetired(request);continue;}
  apply(request.key,request.inner);
  if(hasPending())return;
 }
}
