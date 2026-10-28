# Transfer scout: measured setup before a file

Opt in with `distribution: {capabilityExchange:true, assignedRanges:true,
transferScout:true, maxConnections:3}`. Capability exchange must be requested
on the authenticated peer path, as Phantom Share already does on readiness.
Both endpoints must advertise `phantom-transfer-scout-v1`; older peers continue
using the previous file path. Native WebRTC is not involved.

The smaller peer ID owns one bounded exploration. It opens three independent
RTC associations with the same RTC configuration as file jobs. Each direction
gets one warm-up and four equal-byte deliveries in balanced 1/3/3/1 or 3/1/1/3
order. Directions run consecutively. Payloads are pseudorandom bytes counted
and discarded by the receiver, with completion acknowledgments on the probe's
first data channel. Elapsed time uses only the sending endpoint's monotonic
clock. No clocks are compared between devices. Default round size is 768 KiB;
ten deliveries total, including warm-ups, use at most 7.5 MiB of payload plus
signaling/framing overhead. `scoutSampleBytes` can configure 3 KiB–3 MiB per
round, divisible by three. No retries or new timers are added.

The scout records per-round duration, actual message sizes, queue observations,
and bounded local browser stats. It selects a connection count only when both
observed delivery durations for one candidate beat both observations of the
other. Otherwise the result explicitly says inconclusive and keeps three.
Two repetitions are a screening measurement, not a statistical guarantee.
Short synthetic deliveries include acknowledgment latency and omit disk,
hashing, file framing and batch coordination. Probe associations are separate
from subsequent file associations and can select different routes. This is
not a measurement of the link's capacity or proof of sustained file speed.

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
