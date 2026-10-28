# Transfer scout: measured setup and transport handoff

Opt in with `distribution: {capabilityExchange:true, assignedRanges:true,
transferScout:true, maxConnections:3}`. Add `scoutBeforeSend:true` to wait for
outgoing scouting during single-recipient agreed preparation. Phantom Share
uses both. Both endpoints advertise `phantom-transfer-scout-v3`; v1/v2 peers
keep their compatible file path without v3 scouting or handoff. Native WebRTC
is not involved.

The smaller peer ID starts background exploration over three independent RTC
associations using the file RTC configuration and reliable ordered channels.
Each direction gets a warm-up, four screening deliveries, and four larger
confirmations. Screening uses balanced 1/3/3/1 or 3/1/1/3 order; confirmation
uses the opposite order. Directions run consecutively. Payloads are random
bytes counted and discarded by the receiver. Three bounded intermediate
acknowledgments report cumulative bytes; the final acknowledgment confirms
completion. Sender durations use its monotonic clock, never device clock
subtraction.

If a file is selected before background exploration completes, agreed
preparation with `scoutBeforeSend:true` replaces it with one outgoing pass
owned by the intended sender, regardless of peer ID. The other direction
is not measured first. Preparation continues after explicit completion,
native failure, peer refusal, cancellation, disconnect or session close.
`network.skipTransferScout()` cancels that wait and continues with the
compatible file setup; Share exposes **Send without waiting** during this
step. A silent peer cannot be detected without a timeout: the user must skip
or leave. There is no new timeout, polling loop or retry queue. Opt-in callers
must provide cancellation access. Unsupported peers are never awaited.

The base sample defaults to 768 KiB; each confirmation carries 3 MiB. Background
exploration is capped at 31.5 MiB of payload, and a replacement outgoing pass
at 15.75 MiB, plus signaling/framing. At most one replacement is attempted per
peer and generation. `scoutSampleBytes` configures the base between 3 KiB and
3 MiB, divisible by three; budgets scale with it. Each association's probe
queue is bounded at 768 KiB, resuming on native drain events below 384 KiB.
Active lanes pump concurrently. These queue bounds do not tune the file
sender's watermarks.

A short-run winner is provisional. Both repeated timings must beat both of
the alternative at each size, agree between sizes, and also win in the later
confirmation delivery windows. Missing or overlapping evidence explicitly
keeps the fallback of three connections. These are acknowledged-delivery
rates, not NIC timestamps, link capacity or steady-state guarantees. Probe
payload omits file reading, hashing, framing and storage.

The receiver retains each lane's byte/message count, first/last arrival,
completion time, maximum gap and four largest gap intervals on a common local
clock for that round. Sender samples retain per-lane queue wait count,
cumulative/maximum queue wait, payload submitted and enqueue completion.
Completed/cancelled lifecycle reports retain incoming summaries; cancellation
also captures unfinished enqueue/progress/queue and receiver state. Partial
observations never override the fallback. Browser stats remain local. No extra
traffic is required for these summaries.

## Reuse of the measured associations

After the completion handshake, both endpoints retain the three associations.
File preparation does not destroy a completed pool. Assigned file jobs claim
lanes from that pool and create file channels on the **same SCTP/DTLS/ICE
associations**, preserving the routes that were measured. Existing file
readiness, framing, verification and coordinator leases remain in use.

The source sends the scout ID and lane over the authenticated control path.
The receiver only binds it to an authorized coordinator job with the matching
peer, scout pool and assigned range. Each lane can be claimed once. A pool is
bound to one asset at its first claim, and cannot be reused by another asset.
Unexpected incoming file channels are bounded to one per retained association
until authenticated binding. A missing/failed pool rejects the handoff through
the existing job failure path; it is never silently logged as a successful
handoff to a different connection. Logs retain distinct claimed and bound
`preflight-handoff` events with job IDs.

Unused associations remain bounded to the pool of three and are released on
peer loss, exclusion, generation change, session close or explicit pool
replacement. Claimed associations belong to file jobs and follow their normal
cleanup. Once consumed, a pool is not presented as an available measured setup
for a different file. Later sends may therefore use the compatible fallback;
there is no automatic re-probing loop.

Only outgoing evidence selects outgoing setup. The source's selected limit
travels with its authenticated inventory for that asset and recipient. The
coordinator caps assigned jobs for that exact pair, including when a third
peer coordinates. `preflight-selected` and `preflight-applied` distinguish a
request from coordinator acknowledgment; handoff events establish transport
reuse separately. Old/unconfirmed coordinators retain their previous limits.
Multi-recipient sends keep the compatible path.

`network.getTransferScout(peer)` and `network.getTransferPlan(peer).setup`
expose an available outgoing setup. Retained raw reports stay in Share's
existing export, with completed/cancelled summaries and bounded handoff records
preserved independently of raw report rotation. Nothing is uploaded.

Validation: `node --test tests/transfer-scout*.test.js` covers policy, quotas,
sender intent, cancellation, one-time lane/asset binding, cleanup and scheduler
enforcement with an independent coordinator. The deterministic transport fixture
is a protocol model, not a performance model. `tools/perf/transfer-scout-browser.mjs`
checks real Chromium association identity through both background and outgoing
scouts, a byte-for-byte 4 MiB file, cleanup and cancellation. Loopback checks
plumbing; improvement on the user's network remains unproven.
