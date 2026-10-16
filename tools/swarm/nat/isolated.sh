#!/usr/bin/env bash
set -Eeuo pipefail
[[ $(readlink /proc/self/ns/net) != "${PHANTOM_NAT_PARENT_NET_NS:?missing parent network namespace}" ]] || { echo 'Refusing to modify host networking' >&2; exit 2; }
[[ $(readlink /proc/self/ns/mnt) != "${PHANTOM_NAT_PARENT_MNT_NS:?missing parent mount namespace}" ]] || { echo 'Refusing to modify host mounts' >&2; exit 2; }
export PHANTOM_NAT_ISOLATED_NET_NS="$(readlink /proc/self/ns/net)"
mount --make-rprivate /
mount -t tmpfs tmpfs /run
mkdir -p /run/netns
ip link set lo up
exec bash tools/swarm/nat/suite.sh "${1:?report directory required}"
