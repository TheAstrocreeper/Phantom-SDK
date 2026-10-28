/** Synthetic monotonic clock for protocol tests, not a network model.
 * Advance every observation; sampling may add clock reads freely. Each outgoing
 * round uses a deliberately distinct rate so scheduler policy can be exercised.
 * Pure selection tests supply explicit timings separately.
 */
export function scoutClock({reverse=false}={}) {
 let round=0,time=0;
 const order=reverse?[3,1,1,3]:[1,3,3,1];
 return {
  now(){const phase=round%9,c=phase===0?3:phase<=4?order[phase-1]:order.map(c=>4-c)[phase-5];time+=c===1?1:100;return time;},
  report(report){if(report.phase==='preflight-sample')round++;},
 };
}
