import {readFile} from 'node:fs/promises';
import {buildScoutConnectionPlan} from '../../packages/phantom-core/src/scout-connection-plan.js';
if(process.argv.length<3){console.error('Usage: node tools/perf/scout-plan-report.mjs SUPPORT_LOG.json [...]');process.exitCode=1;}
else for(const path of process.argv.slice(2)){
 const log=JSON.parse(await readFile(path,'utf8'));
 for(const setup of log.scout?.setups??[]){
  if(!setup.result)continue;
  const result={...setup.result,receiverEvidence:setup.receiverEvidence??setup.result.receiverEvidence};
  console.log(JSON.stringify({file:path,build:log.build,scoutId:setup.id,connectionPlan:buildScoutConnectionPlan(result)},null,2));
 }
}
