/** Checks an isolated authority once per leadership tenure. Never edits a roster. */
export function createLeadershipAdmin({networkId,readOrders=async()=>[],applyOrder=async()=>true,
  retired=()=>false,emit=()=>{},retryMs=500,maxQueued=1024}={}) {
  let leader=null,generation=0,phase='idle',checking=null,retryTimer=null,closed=false;
  const pending=new Map(),waiters=new Set();
  const notify=()=>emit('adminCheck',{phase,peerId:leader,generation});
  function settle(value){for(const done of waiters)done(value);waiters.clear();}
  function observe(context){if(closed||retired())return;const next=context?.role==='syscon1'&&context.peerId===context.leader?context.peerId:null;
    if(next===leader)return;leader=next;generation++;phase=next?'pending':'idle';checking=null;pending.clear();clearTimeout(retryTimer);retryTimer=null;settle(false);notify();if(next)start();
  }
  function start(){if(closed||!leader||checking||retired())return;const token=generation,peerId=leader;phase='checking';notify();
    checking=Promise.resolve().then(()=>readOrders({networkId,peerId})).then(async orders=>{
      if(closed||token!==generation)return;if(!Array.isArray(orders))throw Error('Admin orders must be an array');
      for(const order of orders){if(closed||token!==generation)return;if(!await applyOrder(order))throw Error('Invalid admin order');}
      if(closed||token!==generation)return;
      if(retired()){phase='retired';pending.clear();settle(false);notify();return;}
      phase='approved';notify();const tasks=[...pending.values()];pending.clear();settle(true);
      for(const task of tasks)queueMicrotask(()=>{if(!closed&&token===generation&&!retired()&&phase==='approved')task();});
    }).catch(error=>{if(closed||token!==generation)return;phase='failed';emit('adminCheckError',{peerId,error:error.message});notify();retryTimer=setTimeout(()=>{retryTimer=null;start();},retryMs);}).finally(()=>{if(token===generation)checking=null;});
  }
  return {
    observe,
    authorize(context,key,resume){if(closed||retired())return false;observe(context);if(!leader)return true;if(phase==='approved')return true;
      if(typeof resume==='function'){if(pending.size>=maxQueued&&!pending.has(key)){emit('adminCheckError',{peerId:leader,error:'Deferred duty queue full'});return false;}pending.set(key,resume);}if(!retryTimer)start();return false;
    },
    wait(context){if(closed||retired())return Promise.resolve(false);observe(context);if(!leader)return Promise.resolve(false);if(phase==='approved')return Promise.resolve(true);const promise=new Promise(resolve=>waiters.add(resolve));if(!retryTimer)start();return promise;},
    retire(){phase='retired';pending.clear();clearTimeout(retryTimer);retryTimer=null;settle(false);notify();},
    snapshot:()=>({phase,peerId:leader,generation,queuedDuties:pending.size}),
    close(){if(closed)return;closed=true;generation++;clearTimeout(retryTimer);pending.clear();settle(false);}
  };
}
