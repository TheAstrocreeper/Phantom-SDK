# Existing file-request window experiment — 2026-10-29

## Question

Can Phantom's existing request window control sent-but-not-yet-received data
without introducing a separate receipt/acknowledgement protocol?

## Method

Native Chrome 156.0.8078.47 on Ubuntu GitHub Actions. Two separate renderer
contexts share one host and browser network service. Two independent reliable
ordered WebRTC associations, each carrying 32 MiB (64 MiB total each trial).
Fresh associations and file services every trial.

The test imports the existing createScoutFileWorkload driver, which uses the
production createFileDistribution and sendFileChannelPayload paths:
request, block read, per-chunk hash, send, receive, verify and memory-sink write.
Unlike earlier single-batch scouts, each lane requests 128 chunks in four batches.
Each receiver additionally reconstructs the raw arrival data and checks the
whole-lane SHA-256 against the expected zero-filled content.

Only requestWindow changes. Constant settings:
- Chunk size: 262,144 bytes.
- Batch size: 32.
- Source read block: 4,194,304 bytes.
- Per-channel application queue: 1,048,576 high / 524,288 low.
- Existing request timeout 10 s, send timeout 15 s, max attempts 2.
- No new acknowledgement protocol, pacing sleep or recovery polling.
- 45-second outer watchdog bounds test failure only.

The existing global receive-slot count is 32. With two lanes, requestWindow 32
therefore means at most 16 outstanding chunks per lane. Other trials request one
or two per lane. This 16-slot baseline describes this two-lane lab, not the
three-lane app's effective 10-slot split.

Controller elapsed time includes final whole-lane hashing and excludes setup
and manifest preparation. Receive gaps use only receiver-local timestamps.
Selected route IDs must remain stable; discard counters must be present and
monotonic.

## Results

| Trial | Effective chunks per lane | Verified MB/s (decimal, aggregate) | Local send discards | Maximum receive gap (ms) |
|---|---:|---:|---:|---:|
| 1 | 16 | 17.22 | 712 | 848.0 |
| 2 | 1 | 28.15 | 0 | 26.5 |
| 3 | 2 | 36.63 | 0 | 30.1 |
| 4 | 2 | 36.84 | 0 | 27.5 |
| 5 | 1 | 27.58 | 0 | 28.7 |
| 6 | 16 | 17.35 | 747 | 844.4 |

All six transfers completed and verified (384 MiB total). No application file
retries were recorded. That retry counter does not measure SCTP retransmissions.
Source application queues were empty at completion.

The two-slot window delivered roughly 2.1 times the current baseline's aggregate
verified throughput in both observations, with zero local send discards. Its
maximum receive gap was 27.5–30.1 ms versus 844.4–848.0 ms for the baseline.
The one-slot window also eliminated observed discards but was slower than two.

## Decision and limits

The existing request window is sufficient to reproduce the useful control in the
earlier receipt-credit experiment. A new credit wire protocol is unnecessary for
this mechanism. No production default has changed; Phantom Share remains 0.1.77.

The next SDK change should make the window a measured scout candidate and carry
the selected setting into the exact file associations. Do not hardcode two slots
as a universal optimum. These are local observations, not internet or 5G speed
claims. A small fixed window can limit throughput on a high-RTT path; chunk size,
lane count, remote verification/storage cost, and path behavior all affect the
useful amount outstanding. The runner's Chrome patch version also differs from
the user's traced 156.0.8078.73.

The app needs receiver-side enforcement and authenticated agreement on any
selected window; reporting a chosen number without applying it is insufficient.

## Reproduce

```sh
npm ci --prefix tools/swarm
PHANTOM_CHROMIUM_EXECUTABLE="$(command -v google-chrome)" node tools/perf/request-window-browser.mjs
```

[Raw results](../tools/perf/results/request-window-2026-10-29.json).
[Native Chrome CI run](https://github.com/TheAstrocreeper/Phantom-SDK/actions/runs/40893045171).
