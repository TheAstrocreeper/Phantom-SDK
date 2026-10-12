// Deliberately copy only measurements; never serialize packets, invitations or credentials.
const fields=['size','chunkSize','total','processed','received','index','bytes','verifiedBytes','elapsedMs','verificationMs','readMs','hashMs','inFlight','attempt','durationMs','channels','relaySockets','roundTripMs','state','result','role'];
export function createDiagnostics({build,now=()=>performance.now(),wall=()=>new Date().toISOString(),limit=10000}){
 const startedAt=wall(),origin=now(),events=[];let dropped=0;
 function record(type,data={}){const measurements={};for(const key of fields){const value=data[key];if(typeof value==='number'&&Number.isFinite(value)||typeof value==='string'&&['state','result','role'].includes(key))measurements[key]=value;}events.push({at:wall(),sessionElapsedMs:Math.round(now()-origin),type,...measurements});if(events.length>limit){events.shift();dropped++;}}
 function report(){return {schema:'phantom-diagnostics-v1',build,startedAt,exportedAt:wall(),droppedEvents:dropped,measurementNotes:{throughput:'Receiver-verified bytes over measured elapsed time; not link capacity.',preparation:'Batch read and hash times are measured separately; hash time includes concurrent work.',connection:'Network state and channel counts do not establish the selected ICE route.',privacy:'No file contents, filenames, invitation URLs, peer identities or raw signaling packets.'},events:structuredClone(events)};}
 return {record,report,text:()=>JSON.stringify(report(),null,2)};
}
