#!/usr/bin/env bash
set -Eeuo pipefail
mount --make-rprivate /
mount -t tmpfs tmpfs /run
mkdir -p /run/netns
ip link set lo up
exec bash tools/swarm/nat/suite.sh "${1:?report directory required}"
