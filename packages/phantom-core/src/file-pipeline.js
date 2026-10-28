/** Bounded asynchronous preparation with ordered dispatch per peer and connection; admission limits remain per peer. */
export function createPreparedSendQueue({concurrency=8,maxPending=64,maxPerPeer=16}={}) {
 let active=0,pending=0,closed=false;const waiting=[],counts=new Map(),tails=new Map();
 function pump(){while(!closed&&active<concurrency&&waiting.length){const task=waiting.shift();active++;Promise.resolve().then(task.prepare).then(value=>task.resolve({value}),error=>task.resolve({error})).finally(()=>{active--;pump();});}}
 return {
  enqueue(peer,prepare,send,scope=peer){
   if(closed||pending>=maxPending||(counts.get(peer)||0)>=maxPerPeer)return null;
   pending++;counts.set(peer,(counts.get(peer)||0)+1);
   const prepared=new Promise(resolve=>waiting.push({prepare,resolve}));pump();
   let scopes=tails.get(peer);if(!scopes){scopes=new Map();tails.set(peer,scopes);}
   const previous=scopes.get(scope)||Promise.resolve();
   const next=previous.catch(()=>{}).then(async()=>{const result=await prepared;if(closed)throw Error('File service closed');if(result.error)throw result.error;return send(result.value);});
   scopes.set(scope,next);
   const release=()=>{pending--;counts.set(peer,(counts.get(peer)||1)-1);if(!counts.get(peer))counts.delete(peer);if(scopes.get(scope)===next)scopes.delete(scope);if(!scopes.size&&tails.get(peer)===scopes)tails.delete(peer);};
   next.then(release,release);return next;
  },
  close(){closed=true;for(const task of waiting.splice(0))task.resolve({error:Error('File service closed')});},
 };
}
/** At most 256 timing samples per stage; summaries describe overlapping work, not wall-time attribution. */
export function createFileTimings(){
 const stages=new Map();
 return {add(stage,ms){if(!Number.isFinite(ms)||ms<0)return;let s=stages.get(stage);if(!s){s={count:0,sum:0,max:0,samples:[]};stages.set(stage,s);}s.count++;s.sum+=ms;s.max=Math.max(s.max,ms);if(s.samples.length<256)s.samples.push(ms);},summary(){return Object.fromEntries([...stages].map(([stage,s])=>{const sorted=[...s.samples].sort((a,b)=>a-b),pick=q=>sorted[Math.min(sorted.length-1,Math.ceil(q*sorted.length)-1)];return[stage,{count:s.count,sum:s.sum,max:s.max,p50:pick(.5),p95:pick(.95),sampleCount:sorted.length}];}));}};
}
