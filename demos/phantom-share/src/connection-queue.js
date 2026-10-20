/** One local owner: replacement waits for admission AND active-session teardown. */
export function createConnectionQueue({onState=()=>{}}={}) {
  let tail=Promise.resolve(), pending=0, phase='closed', revision=0;
  const state=(next,owner)=>{if(owner!==revision)return;phase=next;try{onState(next);}catch{}};
  function enqueue(work){
    pending++;
    const result=tail.then(work).finally(()=>{pending--;});
    tail=result.catch(()=>{});return result;
  }
  return {
    get busy(){return pending>0;},
    get phase(){return phase;},
    settled:()=>tail,
    run(work,signal){
      const owner=++revision;
      return enqueue(async()=>{
        state('connecting',owner);
        try {
          if(signal?.aborted)throw Error('Connection cancelled');
          const connection=await work();
          if(signal?.aborted){
            if(connection?.cancel)await connection.cancel();
            else await connection?.close();
            throw Error('Connection cancelled');
          }
          state('active',owner);return connection;
        }catch(error){state('closed',owner);throw error;}
      });
    },
    cleanup(work){
      const owner=++revision;
      state('ending',owner);
      return enqueue(async()=>{
        try{await work();}
        finally{state('closed',owner);}
      });
    },
  };
}
