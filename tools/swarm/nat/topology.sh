#!/usr/bin/env bash
# Runs inside the runner's private mount, network and PID namespaces.
set -Eeuo pipefail
[[ -n ${PHANTOM_NAT_ISOLATED_NET_NS:-} && $(readlink /proc/self/ns/net) == "$PHANTOM_NAT_ISOLATED_NET_NS" && $(readlink /proc/self/ns/net) != "${PHANTOM_NAT_PARENT_NET_NS:?missing parent namespace}" ]] || { echo 'Use run-nat-tests.sh: isolated network namespace required.' >&2; exit 2; }
mode=${1:?scenario required}
report=${2:?report directory required}
case "$mode" in direct|blocked|turn-udp|turn-tcp) ;; *) exit 2;; esac
mkdir -p "$report"
pids=()
rootSink=false
cleanup() {
  status=$?
  trap - EXIT
  for n in 1 2 3; do
    ip netns exec "pg$n" iptables-save -c > "$report/gateway-$n.rules" 2>&1 || true
    ip netns exec "pp$n" ip -j address > "$report/peer-$n.addresses.json" 2>&1 || true
  done
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  for n in 1 2 3; do
    for ns in "pp$n" "pg$n"; do
      for pid in $(ip netns pids "$ns" 2>/dev/null); do kill "$pid" 2>/dev/null || true; done
      ip netns del "$ns" 2>/dev/null || true
    done
  done
  if [[ $rootSink == true ]]; then
    iptables -D OUTPUT -d 10.0.0.0/8 -j DROP 2>/dev/null || true
    iptables -D FORWARD -d 10.0.0.0/8 -j DROP 2>/dev/null || true
    ip route del 10.0.0.0/8 dev pwan 2>/dev/null || true
  fi
  ip link del pwan 2>/dev/null || true
  exit "$status"
}
# Preflight before mutation; never fall back to loopback and call it NAT.
ip netns add phantom-capability-check || { echo 'Network namespaces unavailable; NAT tests did not run.' >&2; exit 2; }
ip netns del phantom-capability-check
trap cleanup EXIT
sysctl -qw net.ipv4.ip_forward=1
ip link add pwan type bridge
ip address add 198.18.0.1/24 dev pwan
ip link set pwan up
# Route private candidate checks through a silent firewall sink. A blackhole
# route returns EINVAL to local Coturn sends and can close its allocation.
ip route add 10.0.0.0/8 dev pwan
rootSink=true
iptables -A FORWARD -d 10.0.0.0/8 -j DROP
iptables -A OUTPUT -d 10.0.0.0/8 -j DROP
for n in 1 2 3; do
  ip netns add "pp$n"
  ip netns add "pg$n"
  ip link add "pw$n" type veth peer name wan netns "pg$n"
  ip link set "pw$n" master pwan
  ip link set "pw$n" up
  ip link add lan netns "pg$n" type veth peer name eth0 netns "pp$n"
  ip -n "pg$n" address add "198.18.0.$((10+n))/24" dev wan
  ip -n "pg$n" address add "10.201.$n.1/24" dev lan
  ip -n "pp$n" address add "10.201.$n.2/24" dev eth0
  for dev in lo eth0; do ip -n "pp$n" link set "$dev" up; done
  ip -n "pp$n" route add default via "10.201.$n.1"
  for dev in lo wan lan; do ip -n "pg$n" link set "$dev" up; done
  ip -n "pg$n" route add default via 198.18.0.1
  ip netns exec "pg$n" sysctl -qw net.ipv4.ip_forward=1
  ip netns exec "pg$n" sysctl -qw net.ipv6.conf.all.disable_ipv6=1
  ip netns exec "pp$n" sysctl -qw net.ipv6.conf.all.disable_ipv6=1
  ip netns exec "pg$n" iptables -t nat -A POSTROUTING -o wan -j MASQUERADE
  # Unmapped WAN packets must drop before conntrack confirms a local flow.
  # Otherwise a racing inbound check steals the endpoint's preserved NAT port.
  ip netns exec "pg$n" iptables -P INPUT DROP
  ip netns exec "pg$n" iptables -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
  ip netns exec "pg$n" iptables -P FORWARD DROP
  ip netns exec "pg$n" iptables -A FORWARD -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
  if [[ "$mode" == direct ]]; then
    ip netns exec "pg$n" iptables -A FORWARD -i lan -o wan -j ACCEPT
  elif [[ "$mode" == turn-tcp ]]; then
    ip netns exec "pg$n" iptables -A FORWARD -i lan -o wan -d 198.18.0.1 -p tcp -j ACCEPT
  else
    ip netns exec "pg$n" iptables -A FORWARD -i lan -o wan -d 198.18.0.1 -j ACCEPT
  fi
  ip netns exec "pg$n" iptables -A FORWARD -j DROP
  # Browser origin stays localhost (secure context); this proxy carries signaling only.
  ip netns exec "pp$n" socat TCP-LISTEN:8080,bind=127.0.0.1,reuseaddr,fork TCP:198.18.0.1:8080 > "$report/proxy-$n.log" 2>&1 &
  pids+=("$!")
done
# Optional packet evidence in CI while investigating native handshake divergence.
if command -v tcpdump >/dev/null; then
  tcpdump -i pwan -nn -s0 -U -w "$report/wan.pcap" 'udp or icmp' > "$report/capture.log" 2>&1 &
  pids+=("$!")
  for n in 1 2; do
    ip netns exec "pp$n" tcpdump -i eth0 -nn -s0 -U -w "$report/peer-$n.pcap" 'udp or icmp' > "$report/capture-$n.log" 2>&1 &
    pids+=("$!")
  done
fi
# STUN and TURN live on the simulated WAN. Credentials are disposable test credentials.
turnserver -n --verbose --listening-ip=198.18.0.1 --relay-ip=198.18.0.1 \
  --listening-port=3478 --min-port=49160 --max-port=49260 \
  --realm=phantom-nat --lt-cred-mech --user=phantom:disposable-test-password \
  --fingerprint --no-cli --no-tls --no-dtls --no-multicast-peers \
  --userdb="$report/turndb" --pidfile="$report/coturn.pid" --log-file=stdout --simple-log > "$report/coturn.log" 2>&1 &
pids+=("$!")
for attempt in $(seq 1 50); do
  if nc -z 198.18.0.1 3478; then break; fi
  if ! kill -0 "${pids[-1]}" 2>/dev/null; then cat "$report/coturn.log"; exit 1; fi
  sleep 0.1
done
nc -z 198.18.0.1 3478
timeout --signal=TERM --kill-after=15s 240s node tools/swarm/nat/run.mjs "$mode" "$report"
