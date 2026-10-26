#!/usr/bin/env bash
set -Eeuo pipefail
exec ip netns exec "${PHANTOM_SHAPED_PEER:?peer namespace required}" "${PHANTOM_REAL_CHROMIUM:?Chrome required}" "$@"
