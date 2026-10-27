#!/usr/bin/env bash
# CI entrypoint. No host qdisc or interface is changed.
set -Eeuo pipefail
cd "$(dirname "$0")/../../.."
report="$PWD/tools/perf/delivery-native/download-results-${PHANTOM_SHAPED_LOSS:?loss required}"
mkdir -p "$report"
privilege=(); if [[ $EUID -ne 0 ]]; then privilege=(sudo); fi
trap '"${privilege[@]}" chown -R "$(id -u):$(id -g)" "$report"' EXIT
"${privilege[@]}" env PATH="$PATH:/usr/sbin:/sbin" \
 GITHUB_SHA="${GITHUB_SHA:-$(git rev-parse HEAD)}" \
 PHANTOM_SHAPED_RTT_MS="$PHANTOM_SHAPED_RTT_MS" \
 PHANTOM_SHAPED_LOSS="${PHANTOM_SHAPED_LOSS:?loss required}" \
 PHANTOM_REAL_CHROMIUM="${PHANTOM_CHROMIUM_EXECUTABLE:?Chrome required}" \
 PHANTOM_SHAPED_PARENT_NET="$(readlink /proc/self/ns/net)" \
 PHANTOM_SHAPED_PARENT_MNT="$(readlink /proc/self/ns/mnt)" \
 unshare --mount --net --pid --fork --kill-child=SIGKILL --mount-proc \
 bash tools/perf/delivery-native/download-topology.sh "$report"
