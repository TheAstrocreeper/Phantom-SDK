import {writeFile} from 'node:fs/promises';
import {probeRelays,normalizeRelays} from '../../packages/phantom-doorways/src/relay-probe.js';
const args=process.argv.slice(2),urls=normalizeRelays(args.filter(v=>v.startsWith('wss://')));
const output=args.find(v=>v.startsWith('--out='))?.slice(6);
if(!urls.length){console.error('Usage: node tools/nostr/profile.mjs wss://relay.example/ [--out=report.json]');process.exit(1);}
const results=await probeRelays(urls,{concurrency:1,timeoutMs:12000,onResult:r=>{
 const acks=r.timeline.filter(e=>e.type==='publish');
 console.log(`${r.url} ${r.passed?'PASS':'FAIL'} ${r.reason} (${r.latencyMs}ms); discovery ${r.announcementsReceived}/2, delivery ${r.signalsReceived}/2, accepted signals ${r.signalsAccepted}/2`);
 for(const e of acks)console.log(`  peer ${e.peer}: ${e.packetType} ${e.accepted?'accepted':'rejected'} ack=${e.ackMs}ms bytes=${e.eventBytes} ${e.reason||''}`);
}});
const report={checkedAt:new Date().toISOString(),scope:'Two fresh identities, one unique room, signed discovery and bidirectional signaling. No WebRTC or persistence claim.',results};
if(output)await writeFile(output,JSON.stringify(report,null,2)+'\n');
