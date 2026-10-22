#!/usr/bin/env bash
set -uo pipefail
report=${1:-/reports}
mkdir -p "$report"
failed=0
for mode in direct blocked turn-udp turn-tcp; do
  echo "=== $mode ==="
  bash tools/swarm/nat/topology.sh "$mode" "$report/$mode" 2>&1 | tee "$report/$mode.log"
  status=${PIPESTATUS[0]}
  [[ $status -eq 0 ]] || failed=1
  echo "$mode: exit $status" >> "$report/results.txt"
done
node - "$report" <<'JS'
const fs=require('node:fs'),path=require('node:path');
const root=process.argv[2];
const scenarios=['direct','blocked','turn-udp','turn-tcp'].map(name=>{try{return JSON.parse(fs.readFileSync(path.join(root,name,'summary.json')));}catch{return {scenario:name,status:'failed',error:'Runner did not produce a summary; see scenario log.'};}});
fs.writeFileSync(path.join(root,'summary.json'),JSON.stringify({status:scenarios.every(s=>s.status==='passed')?'passed':'failed',scenarios},null,2));
JS
exit "$failed"
