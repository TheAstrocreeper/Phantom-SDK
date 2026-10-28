/** Synthetic monotonic clock for protocol tests, not a network model.
 * Every observation advances time; a chosen lane set has a deliberately distinct
 * rate to exercise scheduler policy. Pure policy tests supply explicit timings.
 */
export function scoutClock({reverse=false,winner=[0]}={}) {
 let round=0,time=0;
 const candidates=[[0],[1,2],[0,1,2],[1],[0,2],[2],[0,1]],first=reverse?[...candidates].reverse():candidates;
 let screen=[...first,...[...first].reverse()];const counts=[1,2,3,3,2,1];
 return {
  now(){const phase=round%21,lanes=phase===0?[0,1,2]:phase<=14?screen[phase-1]:counts[phase-15]===winner.length?winner:Array.from({length:counts[phase-15]},(_,i)=>i);time+=lanes.join(',')===winner.join(',')?1:100;return time;},
  report(report){if(report.phase==='preflight'&&report.status==='started'&&Array.isArray(report.order))screen=report.order;if(report.phase==='preflight-sample')round++;},
 };
}
