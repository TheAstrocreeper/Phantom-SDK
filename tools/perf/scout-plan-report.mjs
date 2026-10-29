import {readFile} from 'node:fs/promises';
import {buildScoutConnectionPlan} from '../../packages/phantom-core/src/scout-connection-plan.js';
import {selectTransferSetup,screenTransferCandidates} from '../../packages/phantom-core/src/transfer-scout.js';
const args=process.argv.slice(2),audit=args.includes('--audit'),paths=args.filter(arg=>arg!=='--audit');
if(!paths.length){console.error('Usage: node tools/perf/scout-plan-report.mjs [--audit] SUPPORT_LOG.json [...]');process.exitCode=1;}
else{
 const records=new Map();
 for(const path of paths){const log=JSON.parse(await readFile(path,'utf8'));for(const setup of log.scout?.setups??[]){
  if(!setup.result)continue;const result={...setup.result,receiverEvidence:setup.receiverEvidence??setup.result.receiverEvidence},previous=records.get(setup.id);
  if(previous&&(previous.result.receiverEvidence?.receivedRounds??0)>(result.receiverEvidence?.receivedRounds??0))continue;
  records.set(setup.id,{path,log,setup,result});
 }}
 for(const {path,log,setup,result}of records.values()){
  if(audit){const samples=result.samples??[],screen=samples.filter(s=>s.stage==='screen'),confirm=samples.filter(s=>s.stage==='confirm'),replay=selectTransferSetup(samples);
   console.log(JSON.stringify({file:path,build:log.build,scoutId:setup.id,recordedVersion:result.version,recordedDecision:{status:result.status,reason:result.reason,lanes:result.lanes},screening:{rounds:screen.length,finalists:screenTransferCandidates(screen)},confirmation:{rounds:confirm.length,required:6},postLaneProfilePayloadBytes:(result.queueComparisonPayloadBytes??0)+(result.messageComparisonPayloadBytes??0),replay:{status:replay.status,reason:replay.reason,lanes:replay.lanes},limitation:'missing_saved_rounds_are_not_reconstructed'}));
  }else console.log(JSON.stringify({file:path,build:log.build,scoutId:setup.id,connectionPlan:buildScoutConnectionPlan(result)},null,2));
 }
}
