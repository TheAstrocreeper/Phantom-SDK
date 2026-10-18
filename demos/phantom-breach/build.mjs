import {build} from 'esbuild';
import {mkdir,copyFile,readFile} from 'node:fs/promises';
const root=new URL('./',import.meta.url);const out=new URL('dist/',root);
await mkdir(out,{recursive:true});
const engine=await readFile(new URL('../../packages/phantom-core/src/network-engine.js',root),'utf8');
const codes=[...new Set([...engine.matchAll(/["']([a-z][a-z0-9_]{1,100})["']/g)].map(m=>m[1]))];
for(const file of ['../../packages/phantom-core/src/network-state.js','../../packages/phantom-core/src/index.js']){const text=await readFile(new URL(file,root),'utf8');for(const match of text.matchAll(/["']([a-z][a-z0-9_]{1,100})["']/g))codes.push(match[1]);}
const buildId='breach-window-v1';
await build({define:{__PHANTOM_BUILD__:JSON.stringify(buildId),__PHANTOM_DIAGNOSTIC_CODES__:JSON.stringify([...new Set(codes)])},entryPoints:[new URL('src/app.js',root).pathname],bundle:true,format:'esm',target:'es2022',minify:true,sourcemap:true,outfile:new URL('app.js',out).pathname});
for(const file of ['index.html','style.css'])await copyFile(new URL('src/'+file,root),new URL(file,out));
console.log('Built Phantom Breach with the workspace Phantom SDK.');
