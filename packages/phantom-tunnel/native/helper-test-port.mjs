import {spawn} from 'node:child_process';
import {endianness} from 'node:os';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
export const identity=JSON.parse(readFileSync(new URL('../browser-extension/identity.json',import.meta.url),'utf8'));
export const binary=process.env.PHANTOM_HELPER_BINARY??fileURLToPath(new URL('./target/debug/phantom_helper'+(process.platform==='win32'?'.exe':''),import.meta.url));
export function framed(body){const data=Buffer.from(JSON.stringify(body));const head=Buffer.alloc(4);endianness()==='LE'?head.writeUInt32LE(data.length):head.writeUInt32BE(data.length);return Buffer.concat([head,data]);}
export function helperPort({origin='chrome-extension://'+identity.extensionId+'/'}={}){
 const child=spawn(binary,[origin],{env:{...process.env,PHANTOM_HELPER_BIND:'127.0.0.1',PHANTOM_HELPER_STUN:'none'},stdio:['pipe','pipe','pipe']});
 const listeners=new Set();let buffer=Buffer.alloc(0);const frames=[];
 const done=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',code=>{for(const fn of listeners)fn({event:'unavailable'});resolve(code);});});done.catch(()=>{});
 child.stderr.resume();child.stdin.on('error',()=>{});
 child.stdout.on('data',data=>{
  buffer=Buffer.concat([buffer,data]);
  while(buffer.length>=4){const size=endianness()==='LE'?buffer.readUInt32LE(0):buffer.readUInt32BE(0);if(size>32768){child.kill();return;}if(buffer.length<4+size)break;const frame=JSON.parse(buffer.subarray(4,4+size));buffer=buffer.subarray(4+size);frames.push(frame);for(const fn of listeners)fn(frame);}
 });
 return {child,frames,done,send(body){child.stdin.write(framed(body));},subscribe(fn){listeners.add(fn);return()=>listeners.delete(fn);},close(){child.stdin.end();},kill(){child.kill();}};
}
