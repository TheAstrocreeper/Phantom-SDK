#!/usr/bin/env bash
set -euo pipefail
exec ip netns exec "${PHANTOM_PEER_NS:?missing peer namespace}" "${PHANTOM_REAL_CHROMIUM:?missing browser}" "$@"
