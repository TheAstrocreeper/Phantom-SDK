#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/../.."
command -v node >/dev/null || { echo 'Node.js 22 or newer is required.' >&2; exit 2; }
node -e 'if(Number(process.versions.node.split(".")[0])<22)process.exit(1)' || { echo 'Node.js 22 or newer is required.' >&2; exit 2; }
[[ $(uname -s) == Linux ]] || { echo 'This runner requires Linux.' >&2; exit 2; }
command -v apt-get >/dev/null || { echo 'Automatic setup supports Debian, Ubuntu and Mint.' >&2; exit 2; }
# sudo is used only for package setup and the isolated kernel network environment.
privilege=()
if [[ $EUID -ne 0 ]]; then privilege=(sudo); sudo -v; fi
runtime="$PWD/tools/swarm/.nat-runtime"
mkdir -p "$runtime"
missing=()
for spec in ip:iproute2 iptables:iptables socat:socat nc:netcat-openbsd unshare:util-linux; do
  command -v "${spec%%:*}" >/dev/null || missing+=("${spec#*:}")
done
if ((${#missing[@]})); then
  "${privilege[@]}" apt-get update
  "${privilege[@]}" apt-get install -y --no-install-recommends "${missing[@]}"
fi
[[ -d node_modules ]] || npm ci
[[ -d tools/swarm/node_modules ]] || npm ci --prefix tools/swarm
if [[ ! -f "$runtime/browser-ready" ]]; then
  node tools/swarm/node_modules/playwright/cli.js install --with-deps chromium
  touch "$runtime/browser-ready"
fi
browser=$(node --input-type=module -e "import {chromium} from './tools/swarm/node_modules/playwright/index.mjs'; console.log(chromium.executablePath())")
[[ -x "$browser" ]] || { node tools/swarm/node_modules/playwright/cli.js install chromium; }
# Extract Coturn locally: do not install, enable or start a system TURN service.
if ! command -v turnserver >/dev/null && [[ ! -x "$runtime/root/usr/bin/turnserver" ]]; then
  mkdir -p "$runtime/debs/partial" "$runtime/root"
  "${privilege[@]}" apt-get update
  "${privilege[@]}" apt-get -y --download-only --reinstall -o "Dir::Cache::archives=$runtime/debs" install coturn
  for deb in "$runtime"/debs/*.deb; do dpkg-deb -x "$deb" "$runtime/root"; done
fi
library_path=""
for directory in "$runtime"/root/usr/lib/*-linux-gnu "$runtime"/root/lib/*-linux-gnu; do
  [[ ! -d "$directory" ]] || library_path="${library_path:+$library_path:}$directory"
done
report="$PWD/tools/swarm/reports/$(date -u +%Y%m%dT%H%M%SZ)-nat-$$"
mkdir -p "$report"
finish() {
  status=$?
  trap - EXIT
  "${privilege[@]}" chown -R "$(id -u):$(id -g)" "$report"
  tar -czf "$report.tar.gz" -C "$(dirname "$report")" "$(basename "$report")"
  echo "Results and logs: $report.tar.gz"
  exit "$status"
}
trap finish EXIT
# All network, firewall, /run and /proc changes below live in private namespaces.
# A private PID namespace ensures leftover test processes die when the suite exits.
"${privilege[@]}" env PATH="$runtime/root/usr/bin:$PATH:/usr/sbin:/sbin" \
  LD_LIBRARY_PATH="$library_path${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" \
  PHANTOM_CHROMIUM_PATH="$browser" \
  unshare --mount --net --pid --fork --kill-child=SIGKILL --mount-proc \
  bash tools/swarm/nat/isolated.sh "$report"
