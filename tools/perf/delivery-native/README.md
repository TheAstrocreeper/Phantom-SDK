# Sustained native delivery experiments

## Experimental SDK option

`createNetworkDistribution({..., options: {transferLanes: 3}})` carries one
ordinary file-distribution job over three independent reliable ordered native
WebRTC associations. The default remains `transferLanes: 1`. Both endpoints
must explicitly opt in. This prototype uses a bundled signaling description;
it does not negotiate or fall back with an unmodified peer.

`maxConnections` remains the physical connection budget: with the default
budget of three, a striped endpoint can run one distribution job at a time.
No extra pacing timer, hash bypass, extra chunk request window, or extra batch
is introduced. Nine-byte envelopes restore message order across associations;
receive reordering is bounded to 4 MiB and a 4096-message sequence horizon.
Failure of any component closes all components and returns control to the
existing SDK recovery path. Extra associations consume additional connection
and congestion-control resources; an aggregate gain under artificial loss is
not an increase in physical link capacity.

## Reproduction

Use Linux with private mount/network/PID namespaces and native Chrome:

```sh
PHANTOM_SHAPED_RTT_MS=80 PHANTOM_SHAPED_LOSS=0.1 \
PHANTOM_CHROMIUM_EXECUTABLE=/path/to/google-chrome \
bash tools/perf/delivery-native/run.sh
```

The current workflow runs the full SDK unit suite and two native profiles:
zero loss and 0.1% independent random loss. Both directions receive 40 ms delay
and a 15.6 Mbit/s UDP rate limit. Page/signaling TCP remains unshaped. Host
interfaces/qdiscs are untouched. Loss conditions are controlled experiments,
not a measured model of the phone path.

The final experiment prepares the actual SDK manifest for a virtual 1200 MiB
zero-filled file using bounded slice allocation, then receives and independently
checks the first 32 MiB through normal SDK hashing and storage. It does not
claim full 1200 MiB completion. Rates exclude the first 2 MiB. Each case runs
twice, reversing case order. A 90-second receive deadline is recorded as an
incomplete case, never as a completed transfer. Inspect individual results;
a successful job means the harness/integrity checks ran, not that every case
completed. Native transport queues/ICE RTT/counters and kernel qdisc counters
are retained in artifacts. Manifest offer counters observe native JSON only
and are not comparable when the striped envelope is enabled.

## Evidence before the final large-manifest check

- [SDK versus plain channel, run 40295525316](https://github.com/TheAstrocreeper/Phantom-SDK/actions/runs/40295525316):
  at 0.1% loss SDK 4.55/5.41 Mbps, plain 64 KiB 3.57/4.96 Mbps, plain
  16 KiB 5.14/5.15 Mbps. At 1% loss all were around 1.3–1.4 Mbps and none
  completed 32 MiB before the deadline. Shrinking messages did not remove
  the severe-loss slowdown.
- [Association/stream control, run 40302539464](https://github.com/TheAstrocreeper/Phantom-SDK/actions/runs/40302539464):
  at 0.1% loss one association/one stream 4.63/5.09 Mbps; one association/
  three streams 4.84/4.47 Mbps; three independent associations 12.05/11.62 Mbps.
  Aggregate application send buffering was held at 2 MiB.
- [SDK envelope prototype, run 40303522000](https://github.com/TheAstrocreeper/Phantom-SDK/actions/runs/40303522000):
  complete 32 MiB files passed independent byte verification. At 0.1% loss,
  default SDK 4.074/4.075 Mbps; striped SDK 9.29/8.56 Mbps. Zero-loss SDK
  remained 12.48–12.51 Mbps for both. This precedes the actual SDK option and
  larger manifest check; the final workflow tests those separately.

The original raw-channel comparison is retained in `comparison.mjs` with its
own unchanged fixture; the parallel and SDK experiments are reproducible at
the exact commits in their action runs. The adapter unit tests check actual
cross-association reordering, incomplete setup, malformed/duplicate frames,
receive bounds, and complete teardown after a native component failure.

These results establish a candidate worth testing through the app. They do
not establish the cause of the user's cellular slowdown or promise the same
improvement on that path. No Phantom Share deployment is changed by this PR.
