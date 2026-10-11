import {normalizeRelays,probeRelays} from './relay-probe.js';
export const RELAY_SOURCES=Object.freeze(['https://api.nostr.watch/v1/online','https://raw.githubusercontent.com/sesseor/nostr-relays-list/main/relays.txt']);
export async function discoverRelays({fetch=globalThis.fetch,limit=24}={}) {
 const candidates=['wss://relay.damus.io','wss://nos.lol'];const discovery=[];
 for(const url of RELAY_SOURCES){try{
  const response=await fetch(url,{signal:AbortSignal.timeout(8000)});if(!response.ok)throw new Error(`HTTP ${response.status}`);
  const body=await response.text();if(body.length>2000000)throw new Error('Directory response too large');
  let rows;try{rows=JSON.parse(body);}catch{rows=body.split(/\s+/);}
  if(!Array.isArray(rows))throw new Error('Expected relay list');
  candidates.push(...rows.filter(v=>typeof v==='string'));discovery.push({url,ok:true});
 }catch(e){discovery.push({url,ok:false,reason:e.message});}}
 return {urls:normalizeRelays(candidates,limit),discovery};
}
export async function findWorkingRelays({onProgress=()=>{},...options}={}) {
 const {urls,discovery}=await discoverRelays(options);onProgress({stage:'testing',total:urls.length});
 const results=await probeRelays(urls,{...options,onResult:result=>onProgress({stage:'result',result})});
 const relays=results.filter(r=>r.passed).sort((a,b)=>a.latencyMs-b.latencyMs).slice(0,4).map(r=>r.url);
 return {checkedAt:new Date().toISOString(),relays,discovery,results};
}
