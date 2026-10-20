import test from 'node:test';
import assert from 'node:assert/strict';
import {createMembershipAdmission} from '../packages/phantom-core/src/membership-admission.js';
const room='a'.repeat(64);
const policy=extra=>createMembershipAdmission({authorityPublicKey:'b'.repeat(64),authorize:async()=>{},readRevocations:async()=>[],...extra});

test('refresh diagnostics distinguish invalid snapshot and request failure without copying error contents',async()=>{
 for(const [readRevocations,code] of [[async()=>{throw Error('SECRET');},'request_failed'],[async()=>({token:'SECRET'}),'invalid_snapshot']]){
  const rows=[],p=policy({readRevocations,onRefresh:e=>rows.push(e)});
  await assert.rejects(p.refresh(room));
  assert.deepEqual(rows.map(e=>e.status),['start','failure']);assert.equal(rows[1].code,code);
  assert.ok(Number.isFinite(rows[1].elapsedMs));assert.equal(rows[1].freshForMs,0);
  assert.equal(JSON.stringify(rows).includes('SECRET'),false);
 }
});
test('concurrent refresh shares one attempt and ignores observer exceptions',async()=>{
 let release,calls=0;const rows=[];
 const p=policy({readRevocations:()=>{calls++;return new Promise(r=>release=r);},onRefresh:e=>{rows.push(e);throw Error('observer');}});
 const a=p.refresh(room),b=p.refresh(room);await new Promise(r=>setImmediate(r));release([]);
 assert.deepEqual(await Promise.all([a,b]),[true,true]);assert.equal(calls,1);
 assert.deepEqual(rows.map(e=>e.status),['start','success']);assert.ok(rows[1].freshForMs>0);
});
test('hung refresh reports timeout while the original freshness deadline still invalidates membership',async()=>{
 let offline=false;const rows=[],changes=[];
 const p=policy({pollIntervalMs:10,maxAgeMs:40,readRevocations:()=>offline?new Promise(()=>{}):[],onRefresh:e=>rows.push(e)});
 await p.refresh(room);offline=true;const off=p.subscribe({networkId:room,onChange:e=>changes.push(e)});
 try{
  const end=Date.now()+2000;
  while(Date.now()<end&&(!changes.some(e=>e.type==='stale')||!rows.some(e=>e.code==='timeout')))await new Promise(r=>setTimeout(r,5));
  assert.ok(changes.some(e=>e.type==='stale'));assert.ok(rows.some(e=>e.status==='failure'&&e.code==='timeout'));
  const timeout=rows.find(e=>e.code==='timeout');assert.equal(timeout.freshForMs,0);
 }finally{off();}
});
