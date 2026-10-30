import {readFile,writeFile,mkdir,copyFile,chmod,access} from 'node:fs/promises';
import {dirname,resolve,join,win32,posix} from 'node:path';
import {homedir} from 'node:os';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {execFileSync} from 'node:child_process';
const identity=JSON.parse(await readFile(new URL('../browser-extension/identity.json',import.meta.url),'utf8'));
export function installationPlan({platform=process.platform,home=homedir(),binary,localAppData=process.env.LOCALAPPDATA,browser='chrome'}) {
  if(!['linux','darwin','win32'].includes(platform)||!['chrome','chromium','edge'].includes(browser)||typeof binary!=='string'||!binary)throw new Error('Unsupported helper installation');
  const p=platform==='win32'?win32:posix;
  const base=platform==='win32'?p.join(localAppData??p.join(home,'AppData','Local'),'PhantomTunnel'):platform==='darwin'?p.join(home,'Library','Application Support','PhantomTunnel'):p.join(home,'.local','share','phantom-tunnel');
  const installed=p.join(base,platform==='win32'?'phantom_helper.exe':'phantom_helper');
  const dir=platform==='win32'?base:platform==='darwin'?p.join(home,'Library','Application Support',browser==='chrome'?'Google/Chrome':browser==='edge'?'Microsoft Edge':'Chromium','NativeMessagingHosts'):p.join(home,'.config',browser==='chrome'?'google-chrome':browser==='edge'?'microsoft-edge':'chromium','NativeMessagingHosts');
  const manifestPath=p.join(dir,identity.hostName+'.json');
  const manifest={name:identity.hostName,description:'Phantom native bulk transport prototype',path:installed,type:'stdio',allowed_origins:['chrome-extension://'+identity.extensionId+'/']};
  const vendor=browser==='chrome'?'Google\\Chrome':browser==='edge'?'Microsoft\\Edge':'Chromium';
  const registry=platform==='win32'?['add','HKCU\\Software\\'+vendor+'\\NativeMessagingHosts\\'+identity.hostName,'/ve','/t','REG_SZ','/d',manifestPath,'/f']:null;
  return {binary,installed,manifestPath,manifest,registry};
}
export async function installHelper(plan,{register=registry=>execFileSync('reg.exe',registry,{stdio:'pipe'})}={}) {
  await access(plan.binary);await mkdir(dirname(plan.installed),{recursive:true,mode:0o700});
  if(resolve(plan.binary)!==resolve(plan.installed))await copyFile(plan.binary,plan.installed);
  if(process.platform!=='win32')await chmod(plan.installed,0o700);
  await mkdir(dirname(plan.manifestPath),{recursive:true,mode:0o700});
  await writeFile(plan.manifestPath,JSON.stringify(plan.manifest,null,2)+'\n',{mode:0o600});
  if(plan.registry)register(plan.registry);
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){
  const args=process.argv.slice(2),value=name=>{const i=args.indexOf(name);return i<0?undefined:args[i+1];};
  const binary=value('--binary');if(!binary)throw Error('Use --binary /path/to/phantom_helper [--browser chrome|chromium|edge] [--install]. Without --install, only a plan is printed.');
  const plan=installationPlan({binary:resolve(binary),browser:value('--browser')??'chrome'});
  if(args.includes('--install')){await installHelper(plan);console.log('Installed Phantom native host: '+plan.manifestPath);}
  else console.log(JSON.stringify(plan,null,2));
}
