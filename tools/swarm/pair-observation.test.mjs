import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const source=readFileSync(new URL('./contracts-peer.js',import.meta.url),'utf8');
test('transient candidate-pair stats are not recorded as successful relay proof',async()=>{
 let state='in-progress';const rows=[];
 class Native{
  addEventListener(){}
  getConfiguration(){return {iceTransportPolicy:'relay'};}
  async getStats(){return new Map([['transport',{type:'transport',selectedCandidatePairId:'pair'}],['pair',{state,localCandidateId:'local',remoteCandidateId:'remote'}],['local',{candidateType:'relay',relayProtocol:'tcp'}],['remote',{candidateType:'relay'}]]);}
 }
 const c={RTCPeerConnection:Native,native:new Set(),turnOnly:true,log:async(type,data)=>{await Promise.resolve();rows.push({type,data});}};
 vm.createContext(c);vm.runInContext(source.slice(source.indexOf('class RTC extends'),source.indexOf('const chunkStore='))+';globalThis.Observer=RTC;',c);
 const pc=new c.Observer();pc.kind='messaging';await pc.capturePair();
 assert.equal(rows.at(-1).type,'rtc.pendingPair');assert.equal(rows.some(r=>r.type==='rtc.selectedPair'),false);
 state='succeeded';await pc.capturePair();assert.equal(rows.at(-1).type,'rtc.selectedPair');assert.equal(rows.at(-1).data.state,'succeeded');assert.equal(rows.at(-1).data.relayProtocol,'tcp');
});
