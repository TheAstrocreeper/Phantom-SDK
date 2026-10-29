# Scout the file workload through the file service

For peers advertising `phantom-file-workload-v1`, agreed single-recipient sends with assigned ranges and coordinator lane support use a connection-only preflight. No synthetic screening/message/queue rounds run in that path. Legacy/exploration APIs remain compatible and retain their existing probes.

Once the three owned associations are ready, the sender measures an isolated synthetic file through `createFileDistribution`, using the same request scheduler, bounded read/hash/send preparation queue, retries, hash verification, and channel payload sender as a user file. The channel sender is shared code, including message-size checks, fragmentation, native queue admission, and cancellation. Reliable queued retry ownership is shared too.

The workload uses the actual agreed chunk size, source read block setting, configured batch size, receiver's local request window/retry limits, and effective queue/message policy. It measures at most one batch per participating lane, rounded down for small files. With Share's 32-chunk batches, 256 KiB chunks, and three lanes, the payload budget is 24 MiB: 8 MiB per lane. Tiny files that cannot supply one agreed chunk per lane skip this measurement.

The synthetic file lives only in an isolated file-service instance. It is never published into the user's distribution inventory or written to Downloads. A bounded verified memory sink discards content after hashing; native Android storage performance remains outside this measurement. Each endpoint retains its local evidence, and the source receives the receiver's report over the authenticated control connection.

The result records the workload settings, source submissions, coalesced retries, peak queued bytes at submission, receiver arrival gaps, completion/failure, and the receiver's actual request settings. Raw clocks are never subtracted across endpoints. Completion means this bounded file workload arrived and verified, not that a lane layout is fastest, long transfers are proven, or link capacity is known. Failures preserve partial evidence. No new polling or recovery timers are introduced; the production file service retains its existing cancellation and bounded request/send deadlines.

No new lane ranking or queue tuning policy is inferred from one trial. Unknown or failed measurements do not manufacture a winning profile; ordinary file-transfer fallback remains available. New peers do not use a low-queue synthetic result as evidence for a different production feeding profile.

Validation covers connection-only setup without synthetic payloads, actual 256 KiB chunks and 4 MiB source reads, a complete real file-service workload on retained associations, failed delivery with partial evidence, cancellation before readiness, ordinary file-transfer regressions, and native Chrome/app file transfers.
