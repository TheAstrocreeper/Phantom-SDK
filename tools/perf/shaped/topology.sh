#!/usr/bin/env bash
set -Eeuo pipefail
[[ -n ${PHANTOM_SHAPED_PARENT_NET:-} && $(readlink /proc/self/ns/net) != "$PHANTOM_SHAPED_PARENT_NET" ]] || { echo 'Isolated network namespace required' >&2; exit 2; }
[[ -n ${PHANTOM_SHAPED_PARENT_MNT:-} && $(readlink /proc/self/ns/mnt) != "$PHANTOM_SHAPED_PARENT_MNT" ]] || { echo 'Isolated mount namespace required' >&2; exit 2; }
case ${PHANTOM_SHAPED_RTT_MS:-} in 0|80) ;; *) echo 'Unsupported RTT' >&2; exit 2;; esac
report=${1:?report directory required}
mount --make-rprivate /
mount -t tmpfs tmpfs /run
mkdir -p /run/netns
ip link set lo up
ip link add shaped-wan type bridge
ip address add 10.211.0.1/24 dev shaped-wan
ip link set shaped-wan up
export PHANTOM_SHAPED_NET="$(readlink /proc/self/ns/net)"
for n in 1 2; do
 ip netns add "shaped$n"
 ip link add "shaped-v$n" type veth peer name eth0 netns "shaped$n"
 ip link set "shaped-v$n" master shaped-wan
 ip link set "shaped-v$n" up
 ip -n "shaped$n" address add "10.211.0.$((n+1))/24" dev eth0
 ip -n "shaped$n" link set eth0 up
 ip -n "shaped$n" link set lo up
 ip netns exec "shaped$n" sysctl -qw net.ipv6.conf.all.disable_ipv6=1
 # TCP (page load and signaling) stays outside the impaired UDP band.
 ip netns exec "shaped$n" tc qdisc add dev eth0 root handle 1: prio bands 3 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
 ip netns exec "shaped$n" tc qdisc add dev eth0 parent 1:3 handle 30: netem delay "$((PHANTOM_SHAPED_RTT_MS/2))ms" rate 15600kbit limit 10000
 ip netns exec "shaped$n" tc filter add dev eth0 protocol ip parent 1: prio 1 u32 match ip protocol 17 0xff flowid 1:3
 ip netns exec "shaped$n" socat TCP-LISTEN:8080,bind=127.0.0.1,reuseaddr,fork TCP:10.211.0.1:8080 > "$report/proxy-$n.log" 2>&1 &
done
# The private PID namespace reaps browser and proxy children on every exit.
node tools/perf/shaped/run.mjs "$report"
