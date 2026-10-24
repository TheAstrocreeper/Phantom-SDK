# Controlled transfer-window experiment

Run `node tools/perf/distribution-window.mjs`. The script uses the real SDK distribution and file pipeline with deterministic memory RTC, imposed one-way message latency, and optional delayed storage writes. Each 8 MiB + 17-byte transfer is checksum-verified. Cases run twice, reversing window order on the second repetition. Source requestWindow remains 8 while the receiver sweeps 8/16/32, matching mixed application versions. Temporary writes remain bounded to four.

This is a scheduling experiment, not a WebRTC/SCTP, Wi-Fi, background-tab or phone throughput benchmark. It does not impose a link bandwidth limit or emulate Android power management. Timing sums overlap and are not additive wall-time attribution. Source buffers, 64 KiB chunks and 32-piece scheduling batches stay unchanged.

Local Node run on 2026-10-24, mean elapsed milliseconds across two repetitions:

| Imposed one-way latency | Imposed write delay | Window 8 | Window 16 | Window 32 |
|---|---|---:|---:|---:|
| 25 ms | 0 ms | 1,406.5 | 994.0 | 797.0 |
| 25 ms | 8 ms | 1,471.5 | 1,129.0 | 1,048.0 |
| 100 ms | 0 ms | 5,447.5 | 3,840.0 | 3,029.0 |
| 100 ms | 8 ms | 5,522.0 | 3,970.5 | 3,293.5 |

All 24 transfers verified, with peak write concurrency four. The larger window reduced elapsed time in these latency-limited cases; it is a candidate application setting, not proof of a device speedup. Retain native browser/Java regression tests, interruption/backpressure coverage and matched physical-device tests before claiming improvement. Increasing the receiver window does not alter the wire chunk size or signed publication policy and does not require the sender to use the same window.
