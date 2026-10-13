// Deterministic core timer scheduler. Network callbacks still use real microtasks.
export class ManualClock {
 now=Date.now();next=0;tasks=new Map();
 setTimeout=(fn,ms,...args)=>this.add(fn,ms,0,args);
 setInterval=(fn,ms,...args)=>this.add(fn,ms,Math.max(1,ms),args);
 clearTimeout=id=>this.tasks.delete(id);
 clearInterval=id=>this.tasks.delete(id);
 add(fn,ms,period,args){const id=++this.next;this.tasks.set(id,{fn,args,period,at:this.now+Math.max(1,ms)});return id;}
 async settle(){for(let i=0;i<20;i++)await new Promise(r=>setImmediate(r));}
 async advance(ms){const target=this.now+ms;let observations=0;for(;;){let id,task;for(const [k,v]of this.tasks)if(v.at<=target&&(!task||v.at<task.at)){id=k;task=v;}if(!task)break;if(++observations>10000)throw Error('Unbounded timer activity');this.now=task.at;if(task.period)this.tasks.set(id,{...task,at:task.at+task.period});else this.tasks.delete(id);task.fn(...task.args);await this.settle();}this.now=target;await this.settle();}
}
