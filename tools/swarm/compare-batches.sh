#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
output="reports/batch-comparison-$(date -u +%Y%m%dT%H%M%SZ)"
# Each arm gets a fresh browser and room. SDK defaults stay unchanged.
# Continue to the second arm if the first fails; preserve a failing exit code.
result=0
bash run.sh --stress --runs 1 --peers 4 --batch-size 8 --output "$output/batch-8" "$@" || result=1
node launch.mjs --stress --runs 1 --peers 4 --batch-size 32 --output "$output/batch-32" "$@" || result=1
echo "Comparison reports: $PWD/$output"
exit "$result"
