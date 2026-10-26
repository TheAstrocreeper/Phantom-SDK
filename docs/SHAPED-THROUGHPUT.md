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

## Measured result

[Native run 40272345001](https://github.com/TheAstrocreeper/Phantom-SDK/actions/runs/40272345001) passed both profiles and verified all 44 transfers. Echo RTT medians were 0.7 ms and 80.9 ms. Both egress queues carried traffic at the configured 15,600 kbit/s rate; all four netem drop counters were zero.

Confirmation means for 128 MiB + 17 bytes (two passes each):

| Added RTT | Baseline batch/window | Candidate batch/window | Baseline | Candidate | Elapsed reduction |
| --- | --- | --- | ---: | ---: | ---: |
| 0 ms | 32/32 | 64/8 | 76.0845 s | 75.8350 s | 0.33% |
| 80 ms | 32/32 | 64/16 | 86.3028 s | 81.0141 s | 6.13% |

At 80 ms, the baseline achieved about 12.44 Mbps of file payload and the candidate about 13.25 Mbps. Both passes reproduced the approximately 5.29-second difference. At zero added delay, the candidate's difference was only about a quarter-second. The two profiles selected different request windows, so this does not validate a single replacement setting across both conditions.

The nine-setting screen also shows that batch 8 is substantially slower at 80 ms (about 8.47 seconds per 8 MiB case versus 5.83 seconds at current 32/32 and 5.49 seconds at 64/16). However, Share already ships 32/32, verified in its `transfer-options.js`; reverting to batch 8 would be a regression.

These controlled conditions did not reproduce the observed approximately 5 Mbps internet path. The measured tuning gain is modest and does not explain a two- or three-fold deficit. This does not rule out interactions with real path loss, jitter, carrier behavior, signaling delay or receiver storage; those conditions were not reproduced. TCP signaling bypasses the impairment in this harness. No production setting changes or device deployment follow from this experiment.

Selected per-case measurements, screen means, confirmation and kernel evidence are retained in [shaped-throughput-results.json](shaped-throughput-results.json). Full paired reports are workflow artifacts. The local related suite passed 48 tests.
