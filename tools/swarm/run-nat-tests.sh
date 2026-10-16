#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/../.."
command -v docker >/dev/null || { echo 'Docker is required. Install Docker Engine, then rerun this command.' >&2; exit 2; }
docker info >/dev/null
report="$PWD/tools/swarm/reports/$(date -u +%Y%m%dT%H%M%SZ)-nat-$$"
mkdir -p "$report"
finish() {
  status=$?
  trap - EXIT
  # A second unprivileged container corrects ownership of root-written output on Linux.
  docker run --rm -v "$report:/reports" --entrypoint chown phantom-nat-tests "$(id -u):$(id -g)" -R /reports >/dev/null 2>&1 || true
  tar -czf "$report.tar.gz" -C "$(dirname "$report")" "$(basename "$report")"
  echo "Results and logs: $report.tar.gz"
  exit "$status"
}
docker build -f tools/swarm/nat/Dockerfile -t phantom-nat-tests .
trap finish EXIT
docker run --rm --init --privileged --network none --shm-size=1g \
  -v "$report:/reports" phantom-nat-tests
