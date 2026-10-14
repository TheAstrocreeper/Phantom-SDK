#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "$0")"
if ! command -v node >/dev/null || ! command -v npm >/dev/null; then
  echo 'Node.js 22+ and npm are required.' >&2
  exit 1
fi
node -e 'if(Number(process.versions.node.split(".")[0])<22)process.exit(1)' || { echo 'Node.js 22+ is required.' >&2; exit 1; }
mkdir -p reports
setup_log="reports/setup-$(date -u +%Y%m%dT%H%M%SZ).log"
{
  npm ci --prefix ../..
  npm ci
  npx playwright install chromium --only-shell
} 2>&1 | tee "$setup_log"
node launch.mjs "$@"
