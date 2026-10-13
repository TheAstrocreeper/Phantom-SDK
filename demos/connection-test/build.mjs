import {build} from 'esbuild';
import {mkdir,copyFile} from 'node:fs/promises';
const root=new URL('./',import.meta.url),out=new URL('dist/',root);await mkdir(out,{recursive:true});await build({entryPoints:[new URL('app.js',root).pathname],bundle:true,format:'esm',target:'es2022',sourcemap:true,outfile:new URL('app.js',out).pathname,define:{__CONNECTION_BUILD__:JSON.stringify('main-connection-'+new Date().toISOString()),__MAIN_SHA__:JSON.stringify('007a99affb99bb8055648fc5be3deb3a0408779e')}});await copyFile(new URL('index.html',root),new URL('index.html',out));
