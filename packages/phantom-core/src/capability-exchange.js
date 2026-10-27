import {createCapabilityReport} from './connection-planner.js';

/** Event-driven exchange over the caller's authenticated, admitted peer channel. */
export function createCapabilityExchange({send,allowed,localReport,onReport=()=>{}}) {
 if(typeof send!=='function'||typeof allowed!=='function'||typeof localReport!=='function'||typeof onReport!=='function')throw new TypeError('Invalid capability exchange');
 const peers=new Map();let closed=false;
 const permitted=peer=>typeof peer==='string'&&/^[0-9a-f]{16}$/.test(peer)&&allowed(peer)===true;
 const canonical=report=>{
  if(!report||report.version!==1||!Array.isArray(report.protocols))throw new TypeError('Invalid capability report');
  return createCapabilityReport({version:1,messageLimit:report.messageLimit},{protocols:report.protocols,maxChunkBytes:report.maxChunkBytes});
 };
 return {
  request(peer){if(closed||!permitted(peer))throw Error('Peer unavailable');send(peer,{type:'capabilityRequest',version:1});},
  receive(peer,body){
   if(closed||!permitted(peer)||!body||body.version!==1)return false;
   if(body.type==='capabilityRequest'){const report=canonical(localReport());send(peer,{type:'capabilityReport',version:1,report});return true;}
   if(body.type!=='capabilityReport')return false;
   let report;try{report=canonical(body.report);}catch{return false;}
   if(!peers.has(peer)&&peers.size>=128)return false;peers.set(peer,report);onReport(peer,structuredClone(report));return true;
  },
  get(peer){return peers.has(peer)?structuredClone(peers.get(peer)):null;},
  drop(peer){peers.delete(peer);},
  clear(){peers.clear();},
  close(){closed=true;peers.clear();},
 };
}
