/** Build the same recovered Breach application with two isolated SDK revisions. */
import {execFileSync} from 'node:child_process';
import {mkdtemp, mkdir, readFile, writeFile, copyFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {createRequire} from 'node:module';
const repo=fileURLToPath(new URL('../../',import.meta.url));
const require=createRequire(new URL('../../demos/phantom-share/package.json',import.meta.url));
const {build}=require('esbuild');
const reference='00dfcb1648bedd8b048c93b14a7ce5e2e109fb60';
const current=execFileSync('git',['rev-parse','HEAD'],{cwd:repo,encoding:'utf8'}).trim();
const out=path.resolve(process.argv[2]??path.join(tmpdir(),'phantom-breach-comparison'));
const scratch=await mkdtemp(path.join(tmpdir(),'phantom-breach-source-'));
const git=(args)=>execFileSync('git',args,{cwd:repo,maxBuffer:32*1024*1024});
const sourceRoot=path.join(scratch,'reference');
try {
 await mkdir(sourceRoot,{recursive:true});
 execFileSync('tar',['-x','-C',sourceRoot],{input:git(['archive',reference,'packages','demos']),maxBuffer:32*1024*1024});
 await mkdir(out,{recursive:true});
 const builds=[];
 for(const [label,revision] of [['reference',reference],['current',current]]) {
  const root=path.join(scratch,label+'-sdk');
  await mkdir(root,{recursive:true});
  execFileSync('tar',['-x','-C',root],{input:git(['archive',revision,'packages']),maxBuffer:32*1024*1024});
  // Application bytes are identical; only SDK imports resolve to a different tree.
  const appRoot=path.join(root,'demos/phantom-breach');
  const {cp}=await import('node:fs/promises');
  await cp(path.join(sourceRoot,'demos/phantom-breach'),appRoot,{recursive:true});
  await cp(path.join(sourceRoot,'demos/shared-canvas'),path.join(root,'demos/shared-canvas'),{recursive:true});
  const codes=[];
  for(const file of ['network-engine.js','network-state.js','index.js']) {
   const text=await readFile(path.join(root,'packages/phantom-core/src',file),'utf8');
   for(const m of text.matchAll(/["']([a-z][a-z0-9_]{1,100})["']/g))codes.push(m[1]);
  }
  const target=path.join(out,label);await mkdir(target,{recursive:true});
  await build({entryPoints:[path.join(appRoot,'src/app.js')],bundle:true,format:'esm',target:'es2022',minify:true,
   nodePaths:[path.join(repo,'node_modules'),path.join(repo,'demos/phantom-share/node_modules')],
   define:{__PHANTOM_BUILD__:JSON.stringify('breach-'+label+'-'+revision.slice(0,7)),__PHANTOM_DIAGNOSTIC_CODES__:JSON.stringify([...new Set(codes)])},
   outfile:path.join(target,'app.js')});
  for(const file of ['index.html','style.css'])await copyFile(path.join(appRoot,'src',file),path.join(target,file));
  builds.push({path:label,applicationRevision:reference,sdkRevision:revision});
 }
 await writeFile(path.join(out,'comparison.json'),JSON.stringify({builds,scope:'Identical recovered Breach app; different SDK revisions. Neither is established as the exact previously fast deployment. No Share membership/distribution layer.'},null,2)+'\n');
 for(const file of ['LICENSE','NOTICE','THIRD_PARTY_NOTICES.md'])await copyFile(path.join(repo,file),path.join(out,file));
 await writeFile(path.join(out,'index.html'),`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Breach transfer comparison</title><style>body{max-width:680px;margin:3rem auto;padding:1rem;background:#111820;color:#e4edf4;font:18px/1.5 system-ui}a{color:#b7ed78}</style><h1>Breach transfer comparison</h1><p>Same recovered Breach app, two SDK revisions. Use the same choice on both devices and transfer the same file in the same direction.</p><p><a href="reference/">Reference SDK ${reference.slice(0,7)}</a></p><p><a href="current/">Current SDK ${current.slice(0,7)}</a></p><p>Both use the existing peer channel, 64 KiB chunks and eight requests. Neither uses Share's live membership or distribution scheduler. The reference is preserved source, not a verified copy of the previously fast deployment.</p><p>Keep both pages visible. Each demo exports diagnostics. Compare actual file completion and verification; this is not the raw ramp test.</p>`);
 console.log(JSON.stringify({out,builds}));
} finally {await rm(scratch,{recursive:true,force:true});}
