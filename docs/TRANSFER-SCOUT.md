# Transfer scout: measured setup and transport handoff

Opt in with `distribution: {capabilityExchange:true, assignedRanges:true,
transferScout:true, maxConnections:3}`. Add `scoutBeforeSend:true` to wait for
outgoing scouting during single-recipient agreed preparation. Phantom Share
uses both. Both endpoints advertise `phantom-transfer-scout-v5`; v1/v2/v3/v4 peers
keep their compatible file path without v5 scouting or handoff. Native WebRTC
is not involved.

The smaller peer ID starts background exploration over three independent RTC
associations using the file RTC configuration and reliable ordered channels.
Each direction allows a warm-up, up to fourteen screening deliveries, and up to six larger
confirmations. Screening covers all seven lane sets: each singleton, each pair,
and all three. A balanced order followed by its reverse repeats every candidate
with equal payload. Each lane participates equally across the full screen.

Screening nominates one finalist per connection count, ranked by its slower
repetition, then its faster repetition, with lane identity breaking exact ties.
A nomination is exploration, not a confirmed selection. The sender announces
these three lane sets, and waits for an explicit acknowledgment before starting
larger samples. Confirmation compares them in 1/2/3/3/2/1 count order, twice per
finalist. This keeps repeated 3 MiB confirmations without testing all seven at
that size. It can miss a candidate that screens poorly but improves at scale;
the result is not an exhaustive steady-state optimum.

Directions run consecutively. Random bytes are counted and discarded by the
receiver. Bounded intermediate acknowledgments report cumulative bytes; a final
acknowledgment confirms completion. Sender durations use its monotonic clock, never device clock
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
exploration is capped at 58.5 MiB of payload, and a replacement outgoing pass
at 29.25 MiB, plus signaling/framing. At most one replacement is attempted per
peer and generation. `scoutSampleBytes` configures the base between 3 KiB and
3 MiB, divisible by six; budgets scale with it. Each association's probe
queue is bounded at 768 KiB, resuming on native drain events below 384 KiB.
Active lanes pump concurrently. These queue bounds do not tune the file
sender's watermarks.

A selected lane set must have both screening timings beat both repetitions of
all other screened sets. The same exact set must win both larger repetitions
against the other finalists, and win the late-delivery comparison. Overlapping,
missing or contradictory evidence explicitly retains the fallback of three,
lanes `[0,1,2]`. A tie-breaking finalist nomination never becomes a measured
winner by itself. Reports retain every lane set, the finalists, ranges, winning
lane IDs and fallback reason.

All candidates at a stage send the same total payload. Sample acknowledgments
use the first lane in that candidate, so excluding lane 0 does not force its
sample completion to wait on lane 0's transport. Finalist agreement and lifecycle
control still use lane 0 between timed samples. These are acknowledged-delivery
rates, not NIC timestamps, link capacity or steady-state guarantees. Probe
payload omits file reading, hashing, framing and storage.

## Stop when more samples cannot improve the policy

After every completed delivery, the scout checks the existing selection rule.
Each candidate's observed minimum can only decrease, and its maximum can only
increase. A candidate whose maximum already overlaps another candidate's minimum
cannot later become the strictly separated winner. Unseen candidates remain
possible. If none remain possible, stop. If only the fallback can still qualify,
stop without trying to certify the already-default setup. A still-viable smaller
profile keeps its full confirmation budget.

During confirmation, stop if observed total-time or late-delivery envelopes
already prevent the screened winner from qualifying, or required late-delivery
progress is missing. These stops do not relax the rule or promote incomplete
samples to measured evidence. The result explicitly remains inconclusive, with
reason, possible lane sets and observed ranges retained.

An acknowledged `endDirection` message advances the receiver's bounded round
cursor to the normal direction boundary. The existing turn/completion handshakes
then retain the same associations for file use. No timer, polling or retry is
added. Cancellation releases this acknowledgment wait too. v5 peers alone use
this shortened schedule; older peers retain the compatible file path.

Results record `stoppedEarly`, completed/unrun sample counts,
`directionPayloadBytes`, and `directionPayloadAvoidedBytes`. These are counts,
not predictions of saved elapsed time or link capacity. Maximum budgets above
remain caps; actual traffic can be much smaller. The browser checks both a full
pair confirmation and an early fallback, followed by verified file handoff.

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
the selected lane IDs from that pool and create file channels on the **same SCTP/DTLS/ICE
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

Only outgoing evidence selects outgoing setup. The source's selected count and lane IDs
travel with its authenticated inventory for that asset and recipient. The
coordinator caps assigned jobs for that exact pair, including when a third
peer coordinates. `preflight-selected` and `preflight-applied` distinguish a
request from coordinator acknowledgment; handoff events establish transport
reuse separately. Lane-aware policy is advertised separately from the previous scheduling
capability. Old/unconfirmed coordinators retain their previous limits and never
acknowledge a two-lane selection as applied.
Multi-recipient sends keep the compatible path.

`network.getTransferScout(peer)` and `network.getTransferPlan(peer).setup`
expose an available outgoing setup. Retained raw reports stay in Share's
existing export, with completed/cancelled summaries and bounded handoff records
preserved independently of raw report rotation. Nothing is uploaded.

Validation: `node --test tests/transfer-scout*.test.js` covers policy, quotas,
sender intent, all seven candidates, two-lane enforcement, cancellation,
one-time lane/asset binding, cleanup and scheduler
enforcement with an independent coordinator. The deterministic transport fixture
is a protocol model, not a performance model. `tools/perf/transfer-scout-browser.mjs`
checks real Chromium association identity through both background and outgoing
scouts, a byte-for-byte 4 MiB file, cleanup and cancellation. Loopback checks
plumbing; improvement on the user's network remains unproven.

The Chromium runner also checks a forced pair with a synthetic selection clock.
That case verifies exact `[1,2]` association binding and the two-job cap over real
RTC, not a measured performance advantage. Other browser cases use native clocks.

## Source pipeline (.55)

The scout and its selection rules are unchanged. File preparation remains capped
at eight active tasks, 32 admitted chunks per peer and 64 overall. Dispatch
ordering is scoped to the actual source data channel, while those admission
caps remain shared per peer. Sources without a channel-scope adapter retain
the previous peer ordering. A prepared send carries its original channel identity;
a retired channel cannot silently migrate that send to a replacement job.

The source reader still retains or loads at most one block of at most 4 MiB.
It batches same-turn demand with a microtask, preserving full block read-ahead
for a single local range. When queued demand spans different blocks or sources,
it reads the queued span within the first block rather than always fetching
4 MiB. This reduces repeated byte reads under range interleaving without adding
cache capacity. More, smaller native read calls can cost extra on sources with
fixed per-call latency; a WAN improvement is not established by byte counts.
Hash verification and file framing remain in use. No timers or retries are added.

The controlled 24 MiB reproduction originally read 104 MiB in 26 calls. With the
revised reader and connection scopes it read 27.75 MiB in 38 calls; sequential
access stayed at 24 MiB in six calls. This is a scheduling/byte-amplification
check, not a phone throughput measurement. Regression tests verify independent
connection dispatch, aggregate admission caps, byte-correct interleaved chunks
and the unchanged single-read memory bound. Chromium checks exercise this reader
on the existing real-association and checksum transfer paths.

## Actual file timeline (.56)

`distribution.fileTimeline:true` enables local `distributionTimeline` events.
Share enables it and adds a separate `fileTimeline` section to the existing
Support & logs export. There is no new page, probe payload, wire message,
network setting, scheduler change or timer. SDK users leave it off by default.

Each chunk is keyed by the actual job UUID, batch number and chunk index, under
the authorized asset ID. These identifiers survive export without each device's
diagnostic aliasing. Source stages record request reception, preparation start,
read/hash completion and submission start/completion. Receiver stages record
request submission, binary delivery to the file handler, successful verification
and storage start/completion. Retries retain up to four timestamps per stage.
Submission completion means handed to the channel, not acknowledged remotely.

Queue observations come from the existing distribution maintenance tick. They
include `bufferedAmount`, cumulative submitted bytes and the existing derived
drain count. The count includes protocol headers/control bytes; drain is not a
remote-delivery acknowledgment. A zero queue does not prove the native transport
has no data in flight. Delayed/throttled ticks leave sampling gaps visible in
the recorded local timestamps; no replacement sampling timer is introduced.

Times are monotonic within the endpoint's performance clock origin, retained
with job metadata. Join source and receiver records by job/batch/index, then
compare durations within each clock. Never subtract raw timestamps between
devices or origins or claim one-way latency from them. These observations do
not expose SCTP congestion windows or distinguish all causes of network stalls.

Share retains the latest two assets, at most 65,536 chunk records, 32,768 queue
samples and 1,024 jobs per asset. Queue storage is a ring; stage/chunk/job loss
and asset eviction are explicit counters. Routine diagnostics rotation cannot
remove this section. It records metadata only, not file contents or addresses.
Persistence is attempted once all recorded jobs for an asset close, when the
page becomes hidden, and on pagehide. No per-chunk JSON serialization occurs.
Session-storage errors are reported and the in-memory export stays available;
this does not promise recovery after a tab/process is destroyed.

Tests check local stage ordering, byte integrity, bounded retention, reload and
visible storage failures. Chromium checks verify matching stable source/receiver
chunk identities in both SDK events and Share's retained timeline after a
checksum-matching transfer. This validates observation plumbing, not WAN cause.
