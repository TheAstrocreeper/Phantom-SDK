# Actual Breach transfer comparison

`node tools/breach-comparison/source-read-browser.mjs` isolates native browser
File-read granularity without WebRTC. It uses a generated 32 MiB file, repeats
eight-worker 64 KiB reads versus 4 MiB blocks, hashes every original chunk and
requires identical hash roots. Its results describe this runtime, not the phone.

The SDK's opt-in publication setting `sourceReadBlockBytes: 4194304` coalesces
adjacent source reads behind one retained/in-flight 4 MiB block per file service.
Default `0` retains per-chunk reads. Each publication has its own cache identity;
individual output buffers are copied and every chunk is rehashed against the
manifest. Loads across different sources serialize to preserve the block bound.
Chunk sizes that do not divide 4 MiB align batches by whole chunks. Sparse access
may amplify reads, so the option is not enabled globally. Stop clears retained
cache and close fences late reads. Breach source-v2 compares this setting on the
sender, with the receiving window held constant. Reports retain native block
read counts and durations in addition to the existing chunk timings.

Run the actual SDK file service over a controlled ordered software wire:

```sh
node tools/breach-comparison/window-benchmark.mjs
```

It transfers and verifies 4 MiB with 64 KiB chunks over an 8 MiB/s wire at
10/200 ms RTT, using windows 1, 8 and 32. It reports queued bytes and gaps
between source binary messages separately from serialization time. The model
includes ordered metadata/raw payload delivery, real asynchronous read/hash
and persistence, but not native SCTP, browser scheduling, radio loss or devices.
Its throughput cannot establish the cause of the uploaded device results.

The SDK now accepts an opt-in `requestWindow` from 1 to 32; the default remains
8, and Share remains at 8. Source admission accommodates 32 requests per peer
under the existing 64-job global bound and eight preparation workers. Receiver
admission remains 32 verification/write/request slots with four active writes.
At 64 KiB this permits 2 MiB rather than 512 KiB in requested payload; native
send buffering remains separately bounded. The blocked-write regression checks
that the larger window does not bypass the receiver's memory/storage bound,
including a source using its default request window. A larger configured window
is not a demonstrated device throughput fix.

`network.requestFile(id, {peers, requestWindow: 32})` overrides the configured
window for that request only, with the same 1–32 validation and receiver bounds.
The restored `tools/phantom-breach` uses this to compare 8/32 over one existing
connection and gives every send a fresh distribution identity to avoid cache
hits. Its README documents the procedure.

Analyze saved receiver reports without another device run:

```sh
node tools/breach-comparison/analyze.mjs /path/to/reference.json /path/to/current.json
```

This reports verified chunk gaps, completion time, loaded application ping and
missing progress metrics. It does not label those gaps as pure network time.

The pipeline refactor dropped `bytes`, `verifiedBytes`, `elapsedMs`,
`verificationMs` and `inFlight` from `fileProgress`. Recovered Breach uses these
fields and formats `verificationMs.toFixed(0)` on every chunk. They are restored
at successful persistence: `bytes` is this chunk, `verifiedBytes` counts newly
persisted bytes in this request (excluding prior cached bytes), `elapsedMs`
starts after the request's initial cache scan, `verificationMs` sums overlapping
hash-through-persistence work including write-queue delay, and `inFlight` counts
pending requests. The aggregate is not wall-time attribution. A regression test
fails before the fix and checks exact bytes, a short final chunk, finite timings
and the application's formatting call. This repairs API compatibility, not a
proven cause of native throughput loss.

Build the recovered Breach application twice, with identical application source:

```sh
node tools/breach-comparison/build.mjs /tmp/phantom-breach-comparison
```

Requires the repository's root dependencies and `tools/phantom-breach` dependencies.
The build reads committed Git objects into temporary isolated directories. It does
not modify Share or replace SDK files. `comparison.json` records both revisions.
Reference is preserved PR #2 source at `00dfcb1`, not a confirmed copy of the
previously fast deployed build. Current is the local committed HEAD.

Both applications run the real SDK: full-file preparation hash, `publishFile`
with 64 KiB chunks, `shareFile`, `requestFile`, per-chunk verification, receiver
progress, and full-file SHA-256 before Save. Both use eight outstanding requests
and the existing authenticated peer channel. Neither configures Share's live
membership authority or its separate distribution connection and scheduler.

Deploy the output as a separate test site. On both devices select the same build,
join using that build's invitation, and send the same file in the same direction.
Keep pages visible and compare completion plus downloaded reports. Do not mix
reference/current endpoints in the first comparison.

Interpretation:

| Result | What it isolates |
| --- | --- |
| Reference fast, current slow | SDK revision difference on Breach's actual path |
| Both fast, Share slow | Share configuration/distribution/membership or its selected path |
| Both slow | Preserved reference does not reproduce the reported fast condition; no universal device-cap conclusion |

These are isolation steps, not proofs of a specific faulty function. Different
RTC connections can select different paths and device/network conditions vary.
The existing deterministic `transfer-path-comparison.mjs` verifies exact bytes
through current direct and Share paths, but bypasses the native engine transport
and cannot establish physical throughput. The raw ramp is a third path and is
not a substitute for either actual application flow.
