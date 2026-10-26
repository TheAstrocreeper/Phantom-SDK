# Current-engine throughput under kernel-shaped UDP

The runner uses the actual `createNetworkDistribution` coordinator with its existing supported `batchSize` and `requestWindow` options. No production defaults change. Browser-side file creation, chunk verification, memory storage and independent content checks use the shared experimental comparison endpoint. This experiment measures sensitivity to latency and application scheduling, not a carrier link's capacity.

Two native Chrome processes run in separate Linux network namespaces on a private bridge. `tc netem` adds half the configured RTT to each UDP egress direction, with a 15,600 kbit/s rate and a 10,000-packet queue. TCP page loading, test control and SDK signaling bypass shaping. A native data-channel echo first checks UDP candidates and observed round-trip delay. Kernel qdisc counters must show shaped bytes with no unexpected queue drops. All topology changes are confined to private network/mount/PID namespaces; helpers reject host execution.

Each 0/80 ms profile screens batch sizes 8, 32 and 64 against request windows 8, 16 and 32. Every setting transfers 8 MiB + 17 bytes twice, reversing order for pass two. The lowest mean non-baseline setting is selected for confirmation, even when baseline wins screening. Selection from a small screen is exploratory and is not proof of a gain.

Confirmation compares that candidate against current's batch 32/window 32 using 128 MiB + 17 bytes, in baseline/candidate/candidate/baseline order. Receiver clocks exclude source preparation and initial connection establishment; every file is independently checked after assembly. Reports retain both endpoint measurements, route categories, RTT, transport rates, queue peaks, writes, calibration and qdisc counters. Intermediate results are written after every case, including failures. No throughput threshold turns a slow case into a test failure; integrity, route, configuration and shaping checks do.

Run the `Current engine shaped throughput` GitHub workflow, or on Linux with Node 24, `tools/swarm` dependencies, `iproute2`, `socat`, and Chrome installed:

```sh
PHANTOM_SHAPED_RTT_MS=80 PHANTOM_CHROMIUM_EXECUTABLE=/usr/bin/google-chrome bash tools/perf/shaped/run.sh
```

The runner needs root or sudo for private namespaces. It does not install dependencies or alter host networking. Full per-profile reports are workflow artifacts. Results cover native browser UDP with kernel-imposed latency/rate; they do not reproduce NAT, cellular scheduling, carrier shaping, loss, Android storage or a 1.2 GiB device transfer.
