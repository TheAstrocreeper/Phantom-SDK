# Two-connection bounded-feed experiment — 2026-10-29

## Question and setup

Does reducing JavaScript message size and queue depth avoid Chromium's local
send discards while retaining verified throughput on two concurrent associations?

Native Chrome 156.0.8078.47, GitHub Actions Ubuntu, two separate renderer
contexts on one host sharing Chrome's network service. This is a local mechanism
test, not an internet/5G capacity benchmark. The user's traced Chrome was
156.0.8078.73; this runner used another patch version of Chrome 156.

Each fresh trial sends 32 MiB on each of two reliable ordered data channels,
64 MiB total. Both reconstructed streams must match SHA-256 of the expected
payload. Controller elapsed time includes delivery and hash verification, and
excludes connection setup. Delivery gaps use only receiver-local timestamps.
Candidate pair IDs must remain stable and discard counters must be available and
monotonic. All four trials completed and verified.

Profiles:
- Current: 256 KiB messages; 1 MiB high / 512 KiB low watermarks per channel.
- Bounded: 16 KiB messages; 128 KiB high / 64 KiB low per channel.

The sender checks bufferedAmount plus the next message before each send and
resumes through bufferedamountlow events. No pacing sleeps or recovery polling.
The 45-second watchdog only bounds test failure.

## Observations

| Trial | Feed | Verified MB/s (decimal) | Local send discards | Maximum receive gap (ms) |
|---|---|---:|---:|---:|
| 1 | current | 20.87 | 702 | 864.4 |
| 2 | bounded | 19.76 | 3245 | 438.2 |
| 3 | bounded | 38.59 | 5193 | 47.5 |
| 4 | current | 20.92 | 707 | 867.8 |

Raw results: [bounded-feed-2026-10-29.json](../tools/perf/results/bounded-feed-2026-10-29.json).

## Decision and limits

Do not promote the bounded profile into the app. It did not prevent exhaustion:
both bounded trials had more local send discards than either current trial.
Bounded delivery gaps were smaller, while aggregate verified throughput varied:
one trial was slightly slower and one substantially faster than current.
Four local trials do not establish sustained internet performance or a generally
better profile. Two variables changed together, so these results also cannot
attribute differences independently to message size versus queue depth.

The bounded sender respected its 128 KiB application watermark in both trials,
yet the lower packet path still reported thousands of discards. Consequently,
bufferedAmount below that watermark is not a guarantee that Chromium's internal
packet send allowance remains available. Discard count alone is also not a
throughput score: the fastest trial had the most discards.

Phantom Share remains on 0.1.77. No production feed settings changed.

Reproduce:
```sh
npm ci --prefix tools/swarm
PHANTOM_CHROMIUM_EXECUTABLE="$(command -v google-chrome)" node tools/perf/bounded-feed-browser.mjs
```
