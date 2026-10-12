import {build} from 'esbuild';
import {mkdir,copyFile} from 'node:fs/promises';
const root=new URL('./',import.meta.url);const out=new URL('dist/',root);
await mkdir(out,{recursive:true});
await build({entryPoints:[new URL('src/app.js',root).pathname],bundle:true,format:'esm',target:'es2022',minify:true,sourcemap:true,outfile:new URL('app.js',out).pathname});
for(const file of ['index.html','style.css'])await copyFile(new URL('src/'+file,root),new URL(file,out));
console.log('Built shared canvas with the workspace Phantom SDK.');
