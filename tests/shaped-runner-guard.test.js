import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {readlinkSync} from 'node:fs';
test('shaped topology refuses host namespace before mutations',{skip:process.platform!=='linux'},()=>{
 const result=spawnSync('bash',['tools/perf/shaped/topology.sh','unused'],{encoding:'utf8',env:{...process.env,PHANTOM_SHAPED_PARENT_NET:readlinkSync('/proc/self/ns/net'),PHANTOM_SHAPED_PARENT_MNT:readlinkSync('/proc/self/ns/mnt'),PHANTOM_SHAPED_RTT_MS:'80'}});
 assert.equal(result.status,2);assert.match(result.stderr,/Isolated network namespace required/);
});
