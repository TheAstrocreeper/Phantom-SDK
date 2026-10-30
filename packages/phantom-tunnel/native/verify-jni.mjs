import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {join,delimiter} from 'node:path';
import {mkdirSync} from 'node:fs';
const root=fileURLToPath(new URL('.',import.meta.url)),build=join(root,'target','jni'),classes=join(root,'target','java-tests');
const native=join(root,'target','debug',process.platform==='win32'?'phantom_tunnel_native.dll':process.platform==='darwin'?'libphantom_tunnel_native.dylib':'libphantom_tunnel_native.so');
const cmake=process.env.CMAKE??'cmake';
const commands=[
 ['cargo',['build','--locked','--lib','-j','2']],
 [cmake,['-S','jni','-B',build,'-DCMAKE_BUILD_TYPE=Debug','-DPHANTOM_TUNNEL_LIBRARY='+native,...(process.platform==='win32'?['-DPHANTOM_TUNNEL_IMPLIB='+native+'.lib']:[]),...(process.env.PHANTOM_JNI_INCLUDE_ROOT?['-DJAVA_INCLUDE_PATH='+process.env.PHANTOM_JNI_INCLUDE_ROOT,'-DJAVA_INCLUDE_PATH2='+join(process.env.PHANTOM_JNI_INCLUDE_ROOT,process.platform==='darwin'?'darwin':'linux')]:[])]],
 [cmake,['--build',build,'--config','Debug','-j','2']],
 [process.env.JAVAC??'javac',['-d',classes,'jni/java/io/phantom/tunnel/NativeTunnel.java','jni/BindingTest.java']],
 ['java',['-Djava.library.path='+[join(root,'target','debug'),build,join(build,'Debug')].join(delimiter),'-cp',classes,'BindingTest']],
];
mkdirSync(classes,{recursive:true});
for(const [command,args] of commands){const r=spawnSync(command,args,{cwd:root,env:process.env,stdio:'inherit'});if(r.error)throw r.error;if(r.status!==0)process.exit(r.status??1);}
