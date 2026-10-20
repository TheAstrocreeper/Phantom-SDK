/** Generated attempt counters are monotonic within one sender boot and target.
 * Do not compare counters across boots, peers or legacy opaque attempt IDs.
 */
export function createSysOfferOrder({maxScopes=4096}={}) {
 const highest=new Map();
 function parse(peer,bootId,recipient,attemptId){
  const match=/^(.+):([0-9a-f]{16}):([1-9][0-9]*)$/.exec(String(attemptId||''));
  if(!match||match[1]!==bootId||match[2]!==recipient)return null;
  const counter=Number(match[3]);if(!Number.isSafeInteger(counter))return null;
  return {key:peer+':'+bootId+':'+recipient,counter};
 }
 return {
  allows(peer,bootId,recipient,attemptId){const row=parse(peer,bootId,recipient,attemptId);return !row||row.counter>=(highest.get(row.key)??0);},
  accept(peer,bootId,recipient,attemptId){const row=parse(peer,bootId,recipient,attemptId);if(!row)return;if(!highest.has(row.key)&&highest.size>=maxScopes)highest.delete(highest.keys().next().value);highest.set(row.key,Math.max(highest.get(row.key)??0,row.counter));},
  clear(){highest.clear();}
 };
}
