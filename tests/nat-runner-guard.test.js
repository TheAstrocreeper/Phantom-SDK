import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {readlinkSync,mkdtempSync,existsSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
const linux=process.platform==='linux';
test('topology helper refuses execution outside the isolated runner before creating output',{skip:!linux},()=>{
 const folder=mkdtempSync(join(tmpdir(),'phantom-guard-')),report=join(folder,'report');
 try{
  const env={...process.env};delete env.PHANTOM_NAT_ISOLATED_NET_NS;delete env.PHANTOM_NAT_PARENT_NET_NS;
  const result=spawnSync('bash',['tools/swarm/nat/topology.sh','direct',report],{env,encoding:'utf8'});
  assert.equal(result.status,2);assert.match(result.stderr,/isolated network namespace required/);assert.equal(existsSync(report),false);
 }finally{rmSync(folder,{recursive:true,force:true});}
});
test('isolated helper refuses the parent network namespace before mounting anything',{skip:!linux},()=>{
 const result=spawnSync('bash',['tools/swarm/nat/isolated.sh','unused'],{encoding:'utf8',env:{...process.env,PHANTOM_NAT_PARENT_NET_NS:readlinkSync('/proc/self/ns/net'),PHANTOM_NAT_PARENT_MNT_NS:readlinkSync('/proc/self/ns/mnt')}});
 assert.equal(result.status,2);assert.match(result.stderr,/Refusing to modify host networking/);
});
