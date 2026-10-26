# Progress-aware transfer recovery

The paired Android/PC path capture showed windows repeatedly requested at ten seconds and transfer associations closing while the sender still had native queued bytes. A fixed 15-second batch deadline included both large-manifest delivery and file transfer. Retrying a live queued window amplified the slow path; a receiver without a completed batch was also sent the full manifest again on recovery.

The coordinator now treats the timeout as an idle budget. Valid new manifest pages, newly requested indexes, verified committed chunks and native payload-queue drain renew progress. Payload submission endpoints and total channel submissions distinguish a real drain from readiness chatter. Coarse authenticated progress reports (at most once a second under normal defaults) renew the leader's job and both endpoint leases. Sequence, batch, endpoint and current-leader checks reject stale/replayed reports. Progress is included in coordinator mirroring; membership loss, revocation and terminal transport failure still dispose jobs. Readiness probes alone do not renew the idle budget.

The file service postpones a pending request's retry while that source continues delivering verified chunks. It retains per-chunk hashing and storage timeouts, bounded 32-slot admission, four writes, retry limits and cancellation. Once the source becomes quiet, a missing chunk still retries and ultimately fails/resumes as before. The existing ready handshake advertises a cached, fully verified manifest. It skips manifest replay on recovery without declaring any chunks complete, granting ownership or bypassing hash checks. Old peers omit/ignore the optional field and keep the previous fallback.

Native evidence: https://github.com/TheAstrocreeper/Phantom-SDK/actions/runs/40281781470. Two separate Chrome processes/network namespaces, 80 ms added RTT, kernel UDP limit 1,200 kbit/s, no configured loss and no netem queue drops. The fixture prepares the actual manifest for a virtual 1,200 MiB zero-filled file with 64 KiB chunks. It measures the first verified 32-chunk batch; it does not transfer or claim completion of the entire 1,200 MiB file.

| Version | Observation | Manifest offers | Chunk retries | Connections closed with native queued bytes |
| --- | --- | ---: | ---: | --- |
| Unchanged main | No completed batch after 35 s; 6 chunks verified | 2 | 52 | Two, approximately 1.05 MB and 0.97 MB queued |
| Patched | First 2 MiB batch verified in 26.087 s | 1 | 0 | None |

Receiver-stored bytes in the patched batch were independently checked. This demonstrates removal of application retry/restart amplification under the controlled slow path. It does not prove the cause of the original low wire rate or the final phone-path throughput.

The new deterministic slow-window and manifest-recovery tests fail on unchanged main and pass after the fix. A dead payload stream still expires. A separate-coordinator regression verifies that progress reaches a leader outside the transfer pair without granting it file chunks. Existing recipient authorization, cancellation, resume and window tests remain required. Run the native regression with the `Native slow-progress regression` workflow; full paired results and kernel counters are retained as workflow artifacts.
