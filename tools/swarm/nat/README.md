# Headless NAT tests

From the repository root, run:

```sh
bash tools/swarm/run-nat-tests.sh
```

Requires Docker Engine on Linux, or Docker Desktop using Linux containers. The first run builds an image and downloads Chromium. No phone, hosting, invites or manual pairing are involved. The test container is privileged to create network namespaces; run it only from source you trust. It has no external network after the build. No host networking or Docker socket is mounted. All namespaces and firewall changes are confined to that disposable container.

The command exits nonzero if any scenario fails, prints the report location, and creates a `.tar.gz` containing `summary.json`, per-scenario summaries, JSONL SDK/browser events, selected ICE candidate pairs, Coturn logs, and gateway firewall counters.

Each browser process has its own private subnet and Linux MASQUERADE gateway. A separate simulated WAN hosts temporary STUN/TURN and a test discovery/signaling relay. Localhost proxies carry HTTP and signaling to preserve browser secure-context behavior. File and messaging payloads use native WebRTC. IPv6 is disabled to prevent an alternate route bypassing the IPv4 topology.

| Scenario | Network policy | Required outcome |
|---|---|---|
| direct | Outbound traffic allowed through separate NATs; STUN only | Connected reflexive ICE path, no relay |
| blocked | Gateway allows only service traffic; STUN only | Admission timeout and disposed RTC connections |
| turn-udp | Direct peer traffic blocked; TURN available over UDP | Connection contains a relay candidate, UDP allocation evidence |
| turn-tcp | All forwarded UDP blocked; TCP service access allowed | Connection contains a relay candidate, TCP allocation evidence |

A plain native WebRTC control first verifies each connected topology independently of the SDK (STUN/all for direct; relay-only for TURN). The SDK scenarios still use policy `all`. A control failure is reported separately and prevents misleading SDK assertions.

Connected scenarios check three-peer messaging, recovery after an intentional native data channel close, selected-recipient file transfer, IndexedDB persistence after interruption, reload/resume without requesting the stored chunk, SHA-256 equality, and recipient exclusion. ICE policy remains `all` to exercise firewall-driven fallback rather than force relay policy.

This models controlled NAT/firewall behavior. It does not certify actual mobile browsers, carrier NAT, every symmetric NAT implementation, TURN/TLS, or transparent recovery from a live network change. The interruption is a deliberate SDK close after a durable chunk, followed by a reload; it is reported as such.
