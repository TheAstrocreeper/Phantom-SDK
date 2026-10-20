/** Summarize per-chunk timing without exposing payload or peer details. */
import {readFile} from 'node:fs/promises';
const quantile=(rows,q)=>{const s=[...rows].sort((a,b)=>a-b);return s.length?s[Math.round((s.length-1)*q)]:null;};
for(const file of process.argv.slice(2)) {
 const report=JSON.parse(await readFile(file,'utf8'));
 const rows=report.events.filter(e=>e.type==='verifiedProgress');
 const complete=report.events.findLast(e=>e.type==='complete'&&e.role==='receiver');
 const prepared=report.events.findLast(e=>e.type==='prepared');
 const times=rows.map(e=>e.sessionElapsedMs);
 const gaps=times.slice(1).map((at,i)=>at-times[i]);
 const pings=report.events.filter(e=>e.type==='ping'&&e.sessionElapsedMs>=times[0]&&e.sessionElapsedMs<=times.at(-1)).map(e=>e.roundTripMs);
 const summary=v=>({p50:quantile(v,.5),p95:quantile(v,.95),p99:quantile(v,.99),max:v.length?Math.max(...v):null});
 console.log(JSON.stringify({build:report.build,verifiedChunks:rows.length,complete:!!complete,
  size:complete?.size??prepared?.size??null,preparationMs:prepared?.durationMs??null,
  receiveAndVerifyMs:complete?.durationMs??null,
  bytesPerSecond:complete?.durationMs>0?complete.size/(complete.durationMs/1000):null,
  chunkGapMs:summary(gaps),gapsAbove250Ms:gaps.filter(n=>n>250).length,
  loadedApplicationPingMs:summary(pings),
  lastAggregateVerificationMs:rows.at(-1)?.verificationMs??null,
  missingProgressMetricEvents:rows.filter(e=>!['bytes','verifiedBytes','elapsedMs','verificationMs','inFlight'].every(k=>Number.isFinite(e[k]))).length,
  limitations:'Progress gaps include read/hash, queue, transport and persistence. Application ping is not native network RTT; timestamps do not isolate the bottleneck.'}));
}
