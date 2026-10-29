# Worm selector audit: v8

## Finding from saved runs

Seven unique v7 scouts from October 28 were replayed after deduplicating by scout ID. Six stopped during screening; one stopped after three larger trials. None has all six larger confirmation trials. The old overlap test was mathematically correct for the old qualification rule, but conflicted with screening's intended role: nominating finalists for larger independent trials. The scout then performed 36 MiB of message-size comparisons on default lanes.

| Export time, UTC | Lane samples | Larger trials | Stop reason |
| --- | ---: | ---: | --- |
| 13:24 | 9 | 0 | screening overlap |
| 14:45 | 17 | 3 | larger trial contradicted screening winner |
| 15:12 | 11 | 0 | screening overlap |
| 15:40 | 8 | 0 | screening overlap |
| 15:55 | 14 | 0 | screening overlap |
| 16:19 | 10 | 0 | screening overlap |
| 17:09 | 7 | 0 | only default could satisfy old rule |

Replay with `node tools/perf/scout-plan-report.mjs --audit SUPPORT_LOG.json [MORE_LOGS.json ...]`. Missing saved trials are never reconstructed; no faster configuration can honestly be recovered from this incomplete evidence.

## Changed behavior

- Screening nominates the best repeated exact lane set for each connection count. Overlap does not veto larger trials.
- All six larger trials run in balanced order, twice each for one, two and three connections. A measured speed winner requires separated repeated larger sender envelopes and compatible late delivery, then independent receiver rate and stall confirmation.
- Complete evidence may support a separately labeled **bounded_tradeoff**. Against the default exact lane set, the candidate's worst sender rate, worst sender late rate and worst receiver rate must each remain within the rate-loss budget of the reference's best repeated rates. Its worst receiver gap cannot exceed the reference's worst repeated gap. Fewer connections break eligible ties. This is not statistical equivalence, global optimality, or a link-capacity claim.
- The explicit risk budget defaults to 10%; SDK distribution option `scoutLaneRateTradeoff` accepts 0 through 0.1 to tighten it. This is a policy bound, not a learned network constant.
- Optional message/queue tuning runs only with a qualified lane plan at that point. Otherwise it is skipped and the decision reason and avoided bytes are recorded. Later receiver evidence can qualify lanes without claiming that skipped tuning occurred.
- Missing, malformed, partial, materially slower or worse-stalling evidence retains usable defaults. Fallback reports retain available receiver envelopes and alternative rejection reasons.
- The exact selected lanes reach coordinator acknowledgement and retained file-channel handoff. Bounded tradeoffs remain distinct from measured winners in application reports.

With a 768 KiB probe unit, an unresolved v8 direction uses 29.25 MiB for warmup, screening and larger confirmations, omitting 36 MiB of optional tuning. The maximum stays 65.25 MiB per direction. The last saved v7 run used 42 MiB but skipped all larger trials. These are byte budgets, not wall-clock or throughput improvements.

## Controls and verification

| Environment | Control | Evidence |
| --- | --- | --- |
| Browser | exact lanes, application message ceiling, application queue bounds | negotiated limits, repeated trials, coordinator acknowledgement, actual retained handoffs |
| Android wrapper | optional owned UDP receive-buffer increase | native before/after readback and closing drop counters; mapped candidates resolved to host candidates |
| Linux PC diagnostic | external peer-scoped send-buffer override | capture readback; unavailable to browser JavaScript |

Connection plans preserve verified local native preparation readbacks supplied by the hook. Remote native state stays unknown. Preparation drop counters are not closing counters. No recovery timers, polling, device-name profiles or claims of persisted learning are added.

## Compatibility and checks

Skipping the optional tuning schedule requires `phantom-transfer-scout-v8` at both endpoints. Normal file-transfer compatibility is separate. Tests cover complete evidence, screening overlap, changed larger winners, bounded-tradeoff acceptance and independent vetoes, skip cancellation, coordinator enforcement and retained ownership. Chrome file-transfer tests use real RTC associations; policy timing models are explicitly synthetic and are not throughput benchmarks.
