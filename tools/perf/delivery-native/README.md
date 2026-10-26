# Sustained native delivery comparison

This experiment starts from the v0.1.32 SDK snapshot (`1cccb2d`). It does not
change production file framing, retry behavior, integrity checks, or pacing.

The phone capture showed 83 MiB arriving over about 119 seconds, using one
connection. Only 2.9 seconds were spent between the 41 completed batches.
The remaining slowdown cannot be explained by those batch gaps alone.

Run `PHANTOM_SHAPED_RTT_MS=80 PHANTOM_SHAPED_LOSS=0.1
PHANTOM_CHROMIUM_EXECUTABLE=/path/to/google-chrome bash
tools/perf/delivery-native/run.sh` on a Linux host supporting private mount,
network, and PID namespaces. The GitHub workflow runs loss values 0, 0.1, and
1 percent separately. Existing host interfaces and qdiscs are untouched.

Each profile transfers a complete 32 MiB payload through two actual Chrome
processes. Compare the actual SDK (64 KiB chunks, 32-chunk batches/window)
with plain reliable ordered data channels using 64 KiB and 16 KiB messages.
Raw channels retain a bounded 2 MiB send queue. Each case runs twice, reversing
case order for the second repeat. The receiver verifies all bytes. SDK chunks
are additionally checked through the SDK's normal SHA-256 verification and
independently read from the chunk store after completion.

UDP in both directions receives 40 ms egress delay and a 15.6 Mbit/s rate limit.
Independent random packet loss is injected by the kernel. TCP page loading and
signaling remain unshaped. This is a controlled comparison, **not** an estimate
of cellular capacity or a claim that the user's path has that loss rate.

The report includes received payload timing, 2 MiB progress marks, native
transport counters/ICE RTT, sender queue sizes, retry/manifest/close counters,
completion status, and kernel qdisc statistics. The primary reported payload
rate excludes the first 2 MiB. A 90-second receive deadline records an explicitly
incomplete case; partial progress is not presented as complete throughput.
The job completing successfully only means the experiment ran without a
harness/integrity error. Inspect completion and results for every case.

Interpretation:

- SDK slower than raw 64 KiB: investigate application framing, feedback and
  receiver behavior before blaming the transport.
- Raw 16 KiB consistently faster than raw 64 KiB: smaller wire messages merit
  an SDK implementation experiment, retaining hashes and reliable delivery.
- SDK and raw degrade together: this harness has isolated sensitivity shared
  by reliable WebRTC delivery. It has not identified the phone's network cause.
- All cases remain fast: the actual slowdown has not been reproduced; do not
  promote an unrelated scheduling change as the fix.
