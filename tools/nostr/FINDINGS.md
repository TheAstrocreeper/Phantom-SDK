# Nostr exploration: first findings

These are small observations from 2026-10-15, not reliability rankings or measured relay limits. The JSON reports in `results/` contain relative event timelines and exact relay responses.

| Check | Observation | SDK implication |
| --- | --- | --- |
| Basic signed exchange | Damus, nostr.mom, Snort and Primal accepted and delivered both signals | All four were viable at the sampled moment; acceptance and delivery need separate counters. |
| Repeat on Damus | Discovery accepted, then signal rejected as `rate-limited: you are noting too much` | Stop aggressive writes; temporary throttling should not permanently disable a relay. Exact throttle window and scope remain unknown. |
| Late subscriber on Snort | Opens 4361 ms apart; both discovered and exchanged signals | A bounded announcement/reply exchange can support late discovery without depending on stored ephemeral events. No live no-reply control was run. |
| Snort application idle | Delivery succeeded after 15 seconds without application writes | This interval worked; no general idle-timeout claim. WebSocket control keepalives were not measured. |
| Snort explicit reconnect | Reconnect, discovery and final exchange completed in 4319 ms | Fresh discovery after reconnect works with the probe exchange. Network changes and involuntary outages remain untested. |
| Primal 8 KiB padding | Exact payloads arrived; 9445-byte signed events; ACKs 156/209 ms | One modest payload size works; latency comparisons need controlled repeated samples. |
| nostr.mom 16 KiB padding | Exact payloads arrived; 17637-byte signed events, 17647-byte EVENT messages; ACKs 230/240 ms | Our bounded probe cap works on this relay in this sample. This is not a discovered maximum. |
| NIP-11 metadata | Different declared message/subscription limits and software | Treat declared limits as hints and measured outcomes as separate evidence. |

Implemented from the findings: probe late-discovery reply, publication timelines and message sizing, rejection classification, bounded local rate-limit cooldown, preserved read/ACK processing, AUTH challenge terminology, and explicit idle/reconnect/payload modes. Probe behavior is not a claim that all production discovery recovery paths have been changed.

Still unknown: relay throttle quotas/cooldowns and IP/key scope; payload limits and throughput; long idle periods; involuntary reconnect and network switching; behavior under loss; compatibility across more relay implementations. NIP-66 monitoring can supply candidates but is not yet integrated or trusted as a substitute for verification. Direct-peer → bridge → relay ordering is implemented and validated at `979b0e1`, with a bounded local grace interval and route telemetry.

## CI follow-up discovered during exploration

[Run 38693321768](https://github.com/TheAstrocreeper/Phantom-SDK/actions/runs/38693321768), head `b6e59c8`: unit tests, browser baseline and forced native successor passed, but Cloudflare repeat 3/5 timed out during the new transfer after blackout leader recovery. The other four repeats completed their stages. The failed run recorded ten failed distribution jobs targeting the same receiver, alternating between two sources, roughly every 15 seconds. For the first failed job, the leader logged the source offer/ICE but the retained event stream did not show the receiver processing that job or opening its channel. This narrows investigation to job/signaling delivery or handling; it does not prove a root cause or implicate Nostr (the failing scenario uses Cloudflare and peer bridges).

Artifacts were downloaded for inspection. Do not treat earlier green runs or passing Nostr probes as resolution of this intermittent transfer failure. This prompted the SYS collision investigation below.

### SYS channel collision follow-up

The failed receiver's native stats include two open `six7-sys` channels on one RTC connection, with different stream IDs and asymmetric traffic. The prior `ondatachannel` handler always replaced the selected channel with the last arrival. If both endpoints created a channel during role transitions, each could select the other's channel; strict distribution/admin receive guards would then discard frames arriving on the unselected channel.

A production-core regression reproduces the selection defect and fails before the fix. It simulates either endpoint creating stream 0 or stream 1, introduces the crossed channel, checks both choose stream 0, verifies distribution send/receive on that channel and rejects stale-channel messages. The fix chooses the lower known SCTP stream ID at open, retires the other channel, and guards stale connection callbacks. Unknown IDs do not cause an arbitrary replacement. Open callbacks are idempotent.

This is a concrete reproduced defect and a mitigation consistent with the native evidence. A deterministic fake-channel test alone does not establish the complete cause of the historical native timeout. Native validation at `47031c7` then passed the baseline, forced successor case and all five Cloudflare repeats (1280 jobs, zero recorded job or bridge failures). Sequential routing at `979b0e1` passed the same suite and counts again. These results support the fix without proving universal reliability.

## Native live smoke and rejected publication paths

[Live run 38751392827](https://github.com/TheAstrocreeper/Phantom-SDK/actions/runs/38751392827) passed all seven baseline stages using the merged SDK from PR #6: four initial Chromium peers, 64 KiB, 58.074 seconds, five transfer jobs and zero job failures. Both publisher/leader departure recovery and late-peer verified downloads passed. The loopback server carried zero signals. This is one single-host observation, without NAT/loss emulation.

Socket telemetry included 250 negative ACKs. Matching peer/event IDs to active-adapter publication callbacks attributes 76 `banned:` replies and 154 trust-policy replies to active SDK paths. The remaining 20 negative ACKs were not matched to active callbacks; these include probe traffic, and are not all asserted to be probe-only. The old classifier treated both observed policy replies as event failures, allowing subsequent writes on those same active paths.

The compatibility fix recognizes `banned:` as session-scoped and maps the exact observed trust-policy sentence to `restricted`. Existing session write blocking, read processing and alternative relay selection handle the result. New regressions fail on the old implementation and pass with the fix, including preserved reads and reconnect write blocking. Fresh live validation is recorded in PR #7. Counts across independent live runs are observations, not a controlled throughput or relay-ranking comparison.
