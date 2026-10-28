/** Deterministic sender clock for protocol tests; not an SCTP/network model.
 * Mirrors the fixture's ordered message sizes to produce monotonic progress
 * timestamps at two explicitly different simulated delivery rates.
 */
export function scoutClock({sampleBytes=98304,maxMessageSize=262144,reverse=false}={}) {
 let round=0,index=0,base=0;
 const order=reverse?[3,1,1,3]:[1,3,3,1];
 function points(){
  const phase=round%9,connections=phase===0?3:phase<=4?order[phase-1]:order.map(c=>4-c)[phase-5];
  const bytes=sampleBytes*(phase>4?4:1),perLane=bytes/connections,size=Math.min(65536,maxMessageSize);
  const result=[0];let received=0,next=bytes/4;
  for(let lane=0;lane<connections;lane++)for(let offset=0;offset<perLane;offset+=size){
   received+=Math.min(size,perLane-offset);
   if(received<bytes&&received>=next){result.push(received/bytes);next=(Math.floor(received/(bytes/4))+1)*bytes/4;}
  }
  result.push(1);
  return {fractions:result,duration:(connections===1?10:30)*(phase>4?4:1)};
 }
 return {
  now(){const {fractions,duration}=points();if(index>=fractions.length)throw Error('Unexpected scout clock call');return base+fractions[index++]*duration;},
  report(report){if(report.phase==='preflight-sample'){base+=points().duration+1;round++;index=0;}},
 };
}
