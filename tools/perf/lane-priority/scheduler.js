// Experimental scheduling model; this is not the Phantom wire protocol.
// A caller supplies connection events. There is no polling or stall timer.
export function createLaneScheduler({lanes=3, rangeCount, maxControl=64}={}) {
  if(!Number.isInteger(lanes)||lanes<1||!Number.isSafeInteger(rangeCount)||rangeCount<0||!Number.isInteger(maxControl)||maxControl<1)throw Error('Invalid scheduling limits');
  const state=Array.from({length:lanes},()=>({available:true,work:null}));
  const ranges=Array.from({length:rangeCount},(_,index)=>({index,status:'pending',generation:0}));
  const pending=ranges.map(r=>r.index),controls=[];let nextControl=0;
  const laneAt=i=>{if(!Number.isInteger(i)||i<0||i>=lanes)throw Error('Unknown lane');return state[i];};
  const owner=()=>state.findIndex(s=>s.available&&!s.work);
  function control(payload){if(controls.length>=maxControl)throw Error('Control queue full');const job={kind:'control',id:++nextControl,payload};controls.push(job);return job.id;}
  function next(index){const lane=laneAt(index);if(!lane.available||lane.work)return null;
    // The first idle available lane owns queued controls; sent data cannot be preempted.
    if(index===owner()&&controls.length){lane.work=controls.shift();return {...lane.work};}
    while(pending.length){const n=pending.shift(),range=ranges[n];if(range.status!=='pending')continue;range.status='active';range.generation++;lane.work={kind:'range',index:n,generation:range.generation};return {...lane.work};}return null;
  }
  function finish(index,token){const lane=laneAt(index),work=lane.work;if(!work||work.kind!==token?.kind)return false;
    if(work.kind==='range'&&(work.index!==token.index||work.generation!==token.generation))return false;
    if(work.kind==='control'&&work.id!==token.id)return false;
    lane.work=null;if(work.kind==='range')ranges[work.index].status='complete';return true;
  }
  function close(index){const lane=laneAt(index);if(!lane.available)return;lane.available=false;const work=lane.work;lane.work=null;
    if(work?.kind==='range'){const range=ranges[work.index];range.status='pending';pending.unshift(work.index);}
    if(work?.kind==='control')controls.unshift(work);
  }
  function snapshot(){return {complete:ranges.filter(r=>r.status==='complete').length,pending:pending.length,controlPending:controls.length,lanes:state.map((lane,i)=>({index:i,...lane,work:lane.work&&{...lane.work}}))};}
  return {control,next,finish,close,snapshot};
}
