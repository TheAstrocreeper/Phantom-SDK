import verified from './verified-relays.json' with {type:'json'};
import {normalizeRelays} from './relay-probe.js';
import {findWorkingRelays} from './relay-discovery.js';
/** Refresh on use, not a polling loop; failed scans preserve the last good set. */
export function createRelayManager({storage,now=Date.now,maxAgeMs=86400000,relayFinder=findWorkingRelays,WebSocket=globalThis.WebSocket,onProgress=()=>{}}={}) {
 const key='phantom.verified-relays.v1';let record={...verified,relays:[...verified.relays]},flight;
 function accept(value){return value?.version===1&&Array.isArray(value.relays)&&value.relays.length>0&&value.relays.length<=8&&normalizeRelays(value.relays).length===value.relays.length&&Number.isFinite(Date.parse(value.checkedAt))&&Date.parse(value.checkedAt)<=now()+60000;}
 try{const cached=JSON.parse(storage?.getItem(key)??'null');if(accept(cached)&&Date.parse(cached.checkedAt)>Date.parse(record.checkedAt??0))record=cached;}catch{}
 function refresh(){
  if(flight)return flight;
  flight=(async()=>{try{
   onProgress({stage:'refreshing'});const report=await relayFinder({WebSocket,onProgress});const relays=normalizeRelays(report.relays??[],8);
   if(!relays.length){onProgress({stage:'refresh_failed',reason:'no_passing_relays',retained:record.relays.length});return [...record.relays];}
   record={version:1,checkedAt:new Date(now()).toISOString(),relays};try{storage?.setItem(key,JSON.stringify(record));}catch{}
   return [...record.relays];
  }catch(error){onProgress({stage:'refresh_failed',reason:error.message,retained:record.relays.length});return [...record.relays];}})().finally(()=>{flight=null;});return flight;
 }
 return {get:async()=>{if(!record.relays.length||now()-Date.parse(record.checkedAt??0)>maxAgeMs)return refresh();return [...record.relays];},refresh,snapshot:()=>structuredClone(record)};
}
