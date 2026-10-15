import {spawn} from 'node:child_process';
import {mkdirSync,readFileSync,writeFileSync} from 'node:fs';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
const here=fileURLToPath(new URL('.',import.meta.url)),args=process.argv.slice(2);
const known=new Set(['--runs','--peers','--bytes','--timeout','--output','--headed','--nostr','--relays','--cf','--batch-size','--scenario']);
for(let i=0;i<args.length;i++){if(!known.has(args[i]))throw Error('Unknown stress option: '+args[i]);if(!['--headed','--nostr'].includes(args[i])&&(args[++i]===undefined||args[i].startsWith('--')))throw Error('Missing option value');}
function option(key,fallback){const i=args.indexOf('--'+key);return i<0?fallback:args[i+1];}
const runs=Number(option('runs',3));if(!Number.isInteger(runs)||runs<1||runs>100)throw Error('--runs must be 1–100');
const output=resolve(option('output',join(here,'reports','stress-'+new Date().toISOString().replaceAll(':','-'))));mkdirSync(output,{recursive:true});
const forwarded=args.filter((v,i)=>!['--runs','--output'].includes(v)&&!['--runs','--output'].includes(args[i-1]));
for(const [key,value] of [['peers','8'],['bytes','4194304'],['timeout','180000'],['scenario','stress']])if(!forwarded.includes('--'+key))forwarded.push('--'+key,value);
const summary={build:'swarm-bridge-repeat-v20',status:'running',startedAt:new Date().toISOString(),runs,settings:forwarded,results:[]};let child,interrupted=false;
for(const signal of ['SIGINT','SIGTERM'])process.on(signal,()=>{interrupted=true;child?.kill(signal);});
function save(){writeFileSync(join(output,'stress-summary.json'),JSON.stringify(summary,null,2));const escape=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('"','&quot;');writeFileSync(join(output,'report.html'),`<!doctype html><meta charset="utf-8"><title>Phantom stress</title><style>body{font:16px system-ui;max-width:900px;margin:40px auto;background:#111;color:#eee}a{color:#8cc8ff}td,th{padding:12px;text-align:left}</style><h1>Phantom stress: ${escape(summary.status)}</h1><p>Repeated isolated runs; real RTC on one host. Faults trigger on transfer progress and completion.</p><table><tr><th>Run</th><th>Status</th><th>Duration</th><th>Failure stage</th><th>Error</th></tr>${summary.results.map(r=>`<tr><td><a href="${r.directory}/report.html">${r.iteration}</a></td><td>${escape(r.status)}</td><td>${(r.durationMs/1000).toFixed(1)} s</td><td>${escape(r.error?.stage??'')}</td><td><pre>${escape(r.error?.message??r.error?.error??'')}</pre></td></tr>`).join('')}</table><p><a href="stress-summary.json">Aggregate JSON</a></p>`);}
save();
for(let i=1;i<=runs&&!interrupted;i++){
 const directory='run-'+String(i).padStart(3,'0'),at=Date.now();console.log(`STRESS ${i}/${runs}`);
 const result=await new Promise(resolve=>{child=spawn(process.execPath,[join(here,'launch.mjs'),...forwarded,'--output',join(output,directory)],{stdio:'inherit'});child.once('error',error=>resolve({code:1,error:error.message}));child.once('exit',(code,signal)=>resolve({code:code??1,signal}));});
 let report;try{report=JSON.parse(readFileSync(join(output,directory,'summary.json'),'utf8'));}catch{}
 summary.results.push({iteration:i,directory,status:result.code===0&&report?.status==='passed'?'passed':'failed',durationMs:Date.now()-at,error:report?.error??(result.code!==0?result:undefined),metrics:report?.metrics,bridgeCounts:report?.bridgeCounts,steps:report?.steps.map(({name,status,durationMs,error})=>({name,status,durationMs,error}))});save();
}
summary.status=interrupted?'interrupted':summary.results.length===runs&&summary.results.every(r=>r.status==='passed')?'passed':'failed';summary.finishedAt=new Date().toISOString();save();process.exitCode=summary.status==='passed'?0:1;
console.log('Stress report:',join(output,'report.html'));
