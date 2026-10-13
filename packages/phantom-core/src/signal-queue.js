// One FIFO per peer pipe. Capacity is checked before retaining a task.
export function createSignalQueue({perKey=64,total=1024}={}) {
 const chains=new Map(),counts=new Map();let pending=0;
 return {
  enqueue(key,task){
   const count=counts.get(key)||0;
   if(count>=perKey||pending>=total)return Promise.resolve(false);
   counts.set(key,count+1);pending++;
   const next=Promise.resolve(chains.get(key)).catch(()=>{}).then(task);
   chains.set(key,next);
   return next.finally(()=>{
    pending--;const left=(counts.get(key)||1)-1;
    if(left)counts.set(key,left);else counts.delete(key);
    if(chains.get(key)===next)chains.delete(key);
   });
  },
  snapshot(){return {queuedSignals:pending,queueLimit:total,perPeerLimit:perKey,activeQueues:counts.size};}
 };
}
