import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const cwd=fileURLToPath(new URL('.',import.meta.url));
const python=process.env.PYTHON??(process.platform==='win32'?'python':'python3');
for(const [command,args] of [
  ['cargo',['test','--locked','-j','2']],
  ['cargo',['build','--locked','-j','2','--lib','--example','native_probe']],
  [process.execPath,['--test','test-processes.mjs']],
  [python,['test_ffi.py','-v']],
]) {
  const result=spawnSync(command,args,{cwd,env:process.env,stdio:'inherit'});
  if(result.error)throw result.error;
  if(result.status!==0)process.exit(result.status??1);
}
