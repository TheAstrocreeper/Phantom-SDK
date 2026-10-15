import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {spawn} from 'node:child_process';
test('stress preserves both failed launches and exits unsuccessfully',async()=>{
 const output=mkdtempSync(join(tmpdir(),'phantom-stress-'));
 try{
  const exit=await new Promise((resolve,reject)=>{const child=spawn(process.execPath,[new URL('./stress.mjs',import.meta.url).pathname,'--runs','2','--nostr','--relays','wss://relay.example','--peers','3','--bytes','32768','--timeout','1000','--output',output],{env:{...process.env,PHANTOM_CHROMIUM_PATH:join(output,'absent-chromium')},stdio:'ignore'});child.once('error',reject);child.once('exit',resolve);});
  assert.equal(exit,1);const summary=JSON.parse(readFileSync(join(output,'stress-summary.json'),'utf8'));
  assert.equal(summary.status,'failed');assert.equal(summary.results.length,2);
  for(const row of summary.results){assert.equal(row.status,'failed');assert.equal(row.error.stage,'setup');const report=JSON.parse(readFileSync(join(output,row.directory,'summary.json'),'utf8'));assert.equal(report.settings.scenario,'stress');assert.equal(report.settings.transport.mode,'nostr');assert.deepEqual(report.settings.transport.doorways,[{type:'nostr',relays:['wss://relay.example/']}]);assert.ok(report.error.message.includes('absent-chromium'));assert.ok(readFileSync(join(output,row.directory,'browser-stderr.log'),'utf8'));}
  const html=readFileSync(join(output,'report.html'),'utf8');assert.ok(html.includes('run-002/report.html'));assert.ok(html.includes('absent-chromium'));
 }finally{rmSync(output,{recursive:true,force:true});}
});
