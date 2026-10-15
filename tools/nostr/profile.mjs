import {writeFile} from 'node:fs/promises';
import {relayMetadata} from './metadata.mjs';
import {probeRelays,normalizeRelays} from '../../packages/phantom-doorways/src/relay-probe.js';
const args=process.argv.slice(2),urls=normalizeRelays(args.filter(v=>v.startsWith('wss://')));
const idleArg=args.find(v=>v.startsWith('--idle-reconnect='));
const idleReconnectMs=idleArg?Number(idleArg.slice(17)):null;
if(idleReconnectMs!==null&&(!Number.isFinite(idleReconnectMs)||idleReconnectMs<0||idleReconnectMs>30000))throw Error('--idle-reconnect must be milliseconds from 0 to 30000');
const metadataOnly=args.includes('--metadata-only');
const delayArg=args.find(v=>v.startsWith('--stagger='));
const secondPeerDelayMs=delayArg?Number(delayArg.slice(10)):0;
if(!Number.isFinite(secondPeerDelayMs)||secondPeerDelayMs<0||secondPeerDelayMs>=12000)throw Error('--stagger must be milliseconds from 0 to 11999');
const output=args.find(v=>v.startsWith('--out='))?.slice(6);
if(!urls.length){console.error('Usage: node tools/nostr/profile.mjs wss://relay.example/ [--out=report.json]');process.exit(1);}
const metadata=[];
for(const url of urls){const result=await relayMetadata(url);metadata.push({url,...result});console.log(`${url} metadata ${result.available?'available':result.error||result.status}`);}
const results=metadataOnly?[]:await probeRelays(urls,{concurrency:1,timeoutMs:idleReconnectMs===null?12000:idleReconnectMs+25000,secondPeerDelayMs,idleReconnectMs,onResult:r=>{
 const acks=r.timeline.filter(e=>e.type==='publish');
 console.log(`${r.url} ${r.passed?'PASS':'FAIL'} ${r.reason} (${r.latencyMs}ms); discovery ${r.announcementsReceived}/2, delivery ${r.signalsReceived}/2, accepted signals ${r.signalsAccepted}/2`);
 for(const e of acks)console.log(`  peer ${e.peer}: ${e.packetType} ${e.accepted?'accepted':'rejected'} ack=${e.ackMs}ms bytes=${e.eventBytes} ${e.reason||''}`);
}});
const report={checkedAt:new Date().toISOString(),metadata,secondPeerDelayMs,idleReconnectMs,scope:'Two fresh identities, one unique room, signed discovery and bidirectional signaling. No WebRTC or persistence claim.',results};
if(output)await writeFile(output,JSON.stringify(report,null,2)+'\n');
