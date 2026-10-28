# Transfer scout: measured setup before a file

Opt in with `distribution: {capabilityExchange:true, assignedRanges:true,
transferScout:true, maxConnections:3}`. Capability exchange must be requested
on the authenticated peer path, as Phantom Share already does on readiness.
Both endpoints must advertise `phantom-transfer-scout-v2`; older peers continue
using the previous file path without this probe. The v2 byte schedule is never
sent to a v1 scout. Native WebRTC is not involved.

The smaller peer ID owns one bounded exploration. It opens three independent
RTC associations with the same RTC configuration as file jobs. Each direction
gets a warm-up, four screening deliveries, and four larger confirmations.
Screening uses balanced 1/3/3/1 or 3/1/1/3 connection order; confirmation uses
the opposite order. Directions run consecutively. Payloads are pseudorandom
bytes counted and discarded by the receiver. Three bounded intermediate
acknowledgments, when message boundaries permit them, report cumulative bytes;
a final acknowledgment confirms completion. All elapsed times use the sender's
monotonic clock. No clocks are compared between devices.

The initial sample defaults to 768 KiB; each confirmation carries four times
that payload, or 3 MiB. Both directions together are capped at 31.5 MiB of
payload plus signaling/framing overhead. `scoutSampleBytes` configures the base
sample between 3 KiB and 3 MiB, divisible by three; the larger stage and total
budget scale from that base. Each association's application queue is bounded
at 768 KiB, resuming from native drain events below 384 KiB. Larger candidates
pump all active associations concurrently. No retries or new timers are added.
A silent native queue remains cancellable through the existing lifecycle.

The scout records message sizes, delivery progress, queue peaks and bounded
local browser stats. Each sender sample also retains per-lane submitted payload,
queue wait count, cumulative/maximum queue wait, and enqueue completion time.
The receiver independently retains each lane's byte/message count, first/last
arrival, completion time, maximum gap and four largest gap intervals. These
arrival times share the receiver's monotonic clock, so intervals across lanes
of the same round can be compared. They are JavaScript delivery observations,
not packet timestamps or proof of network loss. Sender queue times use the
sender's clock and must not be subtracted from receiver times.

Completion and cancellation lifecycle reports retain the bounded incoming
round summaries, even if raw stats reports rotate out of Share's export.
Cancellation also captures unfinished outgoing enqueue/progress/queue state
and unfinished incoming lane state. Partial observations never qualify as a
completed measurement or override the fallback. This adds no probe payload,
control frames, timers, or automatic retries. A short-run winner is only provisional. To replace the
fallback, both repeated timings must beat both timings of the alternative at
each payload size, the winner must agree between sizes, and the later portion
of both confirmations must also beat the alternative. Missing, contradictory
or overlapping evidence is explicitly inconclusive and keeps three. A later
portion begins at the first recorded progress point between 50% and 75% of the
payload and ends at completion; these are acknowledged-delivery rates, not NIC
packet timestamps. Reports retain both stages and the reason for the decision.

This is a bounded multiscale experiment, not a statistical guarantee or proof
of steady-state throughput. It includes acknowledgment and browser scheduling
latency. It omits file reading, hashing, file framing, storage and batching.
Its per-association queue cap is a probe resource bound, not a tuning change to
the file sender's watermarks. Probe associations are separate from file
associations and may select different routes. The probe does not establish
link capacity or prove that its choice wins for a full large file. Existing
actual-file logs remain necessary for checking that predictive gap.

Only outgoing evidence chooses outgoing transfers. Share captures the current
selection when preparing a file for one agreed recipient. The selected limit
travels with that source's authenticated inventory for that asset and receiver;
the coordinator caps simultaneous assigned jobs for that exact pair. A third
device can coordinate without receiving the file. The coordinator acknowledges
the policy, producing a distinct `preflight-applied` log event. An older or
unconfirmed coordinator retains the previous setup; logs never call that
an applied measured choice. Multi-recipient sends retain the previous setup.

Scouting does not block sending: selecting a file cancels outstanding scouting
on both peers. An incomplete scout falls back to the existing setup. The same
cancellation happens when a file job arrives. Disconnect, exclusion, membership
removal, leadership change and session close release probe resources and
invalidate the applicable observation. At most one peer can be scouted locally
at a time; no automatic retry queue is created. A silent peer may occupy that
bounded slot until file selection, disconnect or close; it cannot hold a file
send hostage. Samples are not reused across sessions and are not uploaded.

`network.getTransferScout(peer)` exposes the outgoing result, including the
coordinator support state. `network.getTransferPlan(peer).setup` includes it
without changing the already negotiated chunk plan. Existing
`distributionScout` events carry the full local measurements and explicit
selected/applied phases into Share's existing log export. Raw browser stats
stay local; only the bounded selection policy goes to the coordinator.

Validation: `node --test tests/transfer-scout*.test.js` checks quotas,
cancellation, old-peer fallback, and actual scheduler enforcement including an
independent coordinator. Deterministic transport tests are protocol tests, not
network-performance evidence. `tools/perf/transfer-scout-browser.mjs` exercises
real Chromium associations and verifies a subsequent 4 MiB file byte-for-byte.
It checks plumbing, cancellation and cleanup on loopback; WAN performance still
requires the user's actual path.
