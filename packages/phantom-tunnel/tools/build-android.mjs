/** Cross-build the shared Rust/C core and JNI shim; does not build an APK. */
import {spawnSync} from 'node:child_process';
import {join,resolve,dirname} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {mkdir,copyFile} from 'node:fs/promises';
export const targets={
 'aarch64-linux-android':{abi:'arm64-v8a',clang:'aarch64-linux-android'},
 'armv7-linux-androideabi':{abi:'armeabi-v7a',clang:'armv7a-linux-androideabi'},
 'x86_64-linux-android':{abi:'x86_64',clang:'x86_64-linux-android'},
 'i686-linux-android':{abi:'x86',clang:'i686-linux-android'},
};
export async function buildAndroid({target='aarch64-linux-android',ndk=process.env.ANDROID_NDK_HOME,api=33}={}) {
 if(!targets[target]||!ndk||!Number.isInteger(api)||api<24||api>100)throw Error('Supply a supported Rust Android target, ANDROID_NDK_HOME and API 24..100');
 const root=fileURLToPath(new URL('../native',import.meta.url)),spec=targets[target];
 const host=process.platform==='darwin'?'darwin-x86_64':process.platform==='win32'?'windows-x86_64':'linux-x86_64';
 const bin=join(ndk,'toolchains','llvm','prebuilt',host,'bin'),suffix=process.platform==='win32'?'.cmd':'';
 const cc=join(bin,spec.clang+api+'-clang'+suffix),cxx=join(bin,spec.clang+api+'-clang++'+suffix),toolchain=join(ndk,'build','cmake','android.toolchain.cmake');
 const env={...process.env,ANDROID_NDK_HOME:ndk,PHANTOM_ANDROID_API:String(api),CMAKE_TOOLCHAIN_FILE:toolchain,[`CARGO_TARGET_${target.toUpperCase().replaceAll('-','_')}_LINKER`]:cc,[`CC_${target.replaceAll('-','_')}`]:cc,[`CXX_${target.replaceAll('-','_')}`]:cxx,[`AR_${target.replaceAll('-','_')}`]:join(bin,process.platform==='win32'?'llvm-ar.exe':'llvm-ar')};
 const output=resolve(root,env.CARGO_TARGET_DIR??'target'),library=join(output,target,'debug','libphantom_tunnel_native.so'),build=join(output,'jni-android',spec.abi),cmake=env.CMAKE??'cmake';
 for(const [cmd,args] of [
  ['cargo',['build','--locked','--lib','--target',target,'-j','2']],
  [cmake,['-S','jni','-B',build,'-DCMAKE_BUILD_TYPE=Debug','-DCMAKE_TOOLCHAIN_FILE='+toolchain,'-DANDROID_ABI='+spec.abi,'-DANDROID_PLATFORM=android-'+api,'-DANDROID_STL=c++_static','-DPHANTOM_TUNNEL_LIBRARY='+library]],
  [cmake,['--build',build,'-j','2']],
 ]){const result=spawnSync(cmd,args,{cwd:root,env,stdio:'inherit'});if(result.error)throw result.error;if(result.status!==0)throw Error('Android build failed: '+cmd);}
 const destination=join(output,'android-jniLibs',spec.abi);await mkdir(destination,{recursive:true});
 await copyFile(library,join(destination,'libphantom_tunnel_native.so'));await copyFile(join(build,'libphantom_tunnel_jni.so'),join(destination,'libphantom_tunnel_jni.so'));
 return destination;
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){const dir=await buildAndroid({target:process.argv[2]??'aarch64-linux-android'});console.log('Android JNI libraries: '+dir);}
