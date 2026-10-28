/** Deliberately synthetic receiver timings for policy/enforcement tests only.
 * Bytes, message counts and real RTC transport observations remain unchanged.
 * This is not a network benchmark or evidence of a throughput improvement.
 */
export function modelReceiverEvidence(frame,{winner=[0],queueWinner=false}={}){
 if(frame.body?.action!=='receiverEvidence')return frame;
 frame=structuredClone(frame);const r=frame.body.observation;
 const preferred=r.lanes.map(l=>l.lane).join(',')===winner.join(',');
 const reverse=parseInt(frame.body.id.replaceAll('-','').slice(-1),16)%2===1;
 const position=r.round%25-21,tight=reverse?[0,3].includes(position):[1,2].includes(position);
 const duration=r.bytes/4096*(r.stage==='queue'?(queueWinner&&!tight?100:1):(preferred?1:100));
 r.elapsedMs=duration;
 for(const lane of r.lanes){lane.firstAtMs=duration*.1;lane.lastAtMs=duration*.9;lane.completeAtMs=lane.lastAtMs;lane.maxGapMs=duration*.1;lane.largestGaps=[{fromMs:duration*.1,toMs:duration*.2,gapMs:duration*.1,afterBytes:Math.min(1024,lane.bytes-1)}];}
 return frame;
}
