# Scout the file workload through the file service

For peers advertising `phantom-file-workload-v1`, agreed single-recipient sends with assigned ranges and coordinator lane support use a connection-only preflight. No synthetic screening/message/queue rounds run in that path. Legacy/exploration APIs remain compatible and retain their existing probes.

Once the three owned associations are ready, the sender measures an isolated synthetic file through `createFileDistribution`, using the same request scheduler, bounded read/hash/send preparation queue, retries, hash verification, and channel payload sender as a user file. The channel sender is shared code, including message-size checks, fragmentation, native queue admission, and cancellation. Reliable queued retry ownership is shared too.

The workload uses the actual agreed chunk size, source read block setting, configured batch size, receiver's local request window/retry limits, and effective queue/message policy. It measures at most one batch per participating lane, rounded down for small files. With Share's 32-chunk batches, 256 KiB chunks, and three lanes, the payload budget is 24 MiB: 8 MiB per lane. Tiny files that cannot supply one agreed chunk per lane skip this measurement.

The synthetic file lives only in an isolated file-service instance. It is never published into the user's distribution inventory or written to Downloads. A bounded verified memory sink discards content after hashing; native Android storage performance remains outside this measurement. Each endpoint retains its local evidence, and the source receives the receiver's report over the authenticated control connection.

The result records the workload settings, source submissions, coalesced retries, peak queued bytes at submission, receiver arrival gaps, completion/failure, and the receiver's actual request settings. Raw clocks are never subtracted across endpoints. Completion means this bounded file workload arrived and verified, not that a lane layout is fastest, long transfers are proven, or link capacity is known. Failures preserve partial evidence. No new polling or recovery timers are introduced; the production file service retains its existing cancellation and bounded request/send deadlines.

No new lane ranking or queue tuning policy is inferred from one trial. Unknown or failed measurements do not manufacture a winning profile; ordinary file-transfer fallback remains available. New peers do not use a low-queue synthetic result as evidence for a different production feeding profile.

Validation covers connection-only setup without synthetic payloads, actual 256 KiB chunks and 4 MiB source reads, a complete real file-service workload on retained associations, failed delivery with partial evidence, cancellation before readiness, ordinary file-transfer regressions, and native Chrome/app file transfers.

## Verified subset selection (workload protocol v2)

A failed workload is no longer handed to the coordinator as a usable fallback association. Receiver rows explicitly record per-lane verification after the real file service completes that lane's range. A repair candidate must have received and verified the entire batch, with zero queued bytes at both endpoints. Arrival counts by themselves, unavailable verification, and stale protocol reports cannot nominate a survivor.

For a nonempty strict subset, both endpoints detach the rejected association's pool-wide failure handlers before either closes it. A prepare/commit/ack exchange retires its native resources and queued synthetic data without releasing the survivors. The subset is then measured again using the baseline queue and message policy, on the same remaining associations. Only successful repeated verification updates the connection plan and coordinator limit. Explicit handoff guards refuse rejected or unverified lanes at either endpoint.

Attempts shrink monotonically and are bounded to three. Trials, rejections and final selection remain in the export. A fully demonstrated failure with no verified subset blocks agreed preparation. Unsupported/missing evidence stays unavailable and can use fresh ordinary connections; it never reuses a failed retained association. Cancellation/generation teardown settles a pending restriction barrier without new polling or recovery timers.

This selects a working bounded configuration. It does not prove that configuration is fastest or that a long internet transfer cannot stall later. Protocol v2 is separately advertised, so a v1 peer is not asked to perform an unsupported retirement exchange.

## Stop a retrying qualification when a verified alternative exists

The isolated workload forwards the production file service's `fileRetry` event. When a different lane has already completed and verified its entire batch, the receiver ends that trial immediately, before the retry is sent. The failed report preserves the retry's lane, chunk index, attempt, receiver-relative time, and verified alternatives. Existing queue/verification checks still determine whether any alternative can be nominated, and the retirement barrier and repeated subset verification remain mandatory.

A retry with no verified alternative does not end the trial early. Ordinary user-file retry settings are unchanged. This removes the second request-deadline wait from qualifying failures; it adds no timer and makes no claim that a retry proves a broken network or the root cause of slow delivery.

## Qualification belongs to a retained association set

An explicit next-file preparation can start a new outgoing qualification after the previous file has released its connections. The old consumed pool is retired at both endpoints, and its peer-demand guard is cleared. File selection does not resume background exploration; only a new send intent can begin another pass. The receiver accepts that fresh one-way intent when it has no active file links, including after a previous send in the opposite direction.

The new pass creates a new scout ID and new associations; only those newly verified sockets are handed to the next file. A bounded retired-ID history rejects delayed init packets from disposed passes. Existing active/busy checks still prevent a new pass from interrupting a file in progress. No periodic rescouting or recovery timer is added.

Regression coverage sends two distinct files in one direction and a third in reverse, verifies their contents, checks a separate qualification and owned handoff for each, and releases their file sockets. The same sequence runs through native Chrome with an independent coordinator.
