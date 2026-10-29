# Scout and file queue ownership audit

Evidence: paired Phantom Share v0.1.66 exports at 2026-10-29T02:43Z.
This is an observed application queue failure mode, not a proven cause of SCTP/network stalls.

## Observations

- Public srflx/UDP path; PC sends, Android receives. All 20 lane samples completed.
- Lane selection remained fallback [0,1,2]; all three retained associations were claimed and coordinator acknowledged that selection.
- Lane tests used a 786,432-byte queue ceiling and 3,145,728-byte total confirmation payloads. The unresolved lane result skipped the optional message comparison. No production-queue qualification was performed.
- File: 83,187,441 bytes, 318 unique chunks, 53.0939 seconds first-to-last receiver arrival.
- First two jobs delivered 160 and 126 unique chunks over 10.113 and 10.823 seconds. Third job delivered 21 unique chunks over 47.526 seconds, failed, and 11 remaining chunks were delivered on a fresh job. Its timeline contains 32 requested chunk identities, not 32 successful deliveries.
- Receiver's longest aggregate arrival gap was 17,600.3 ms, entirely within that third job's tail.
- PC samples on that job remained at 7,813,394 buffered bytes and 4,787,247 drained bytes from local 82,362.3 through 90,362.3 ms. Later peak was 8,600,924 bytes.
- Third job submitted 51 payloads for 32 unique chunk identities. Repeated receiver requests were served on the same reliable ordered association while originals remained in its local queue. A successful send submission acknowledges local admission, not receiver delivery.
- Android reported 2,097,152-byte effective receive buffers and zero receive-drop increments on the three retained sockets. These facts do not establish PC socket state or wire loss.

## Narrow correction

For a non-striped reliable ordered file association, remember the local submitted-byte endpoint of each whole chunk. On a retry, remove records whose endpoint has drained according to submittedBytes - bufferedAmount. Coalesce a duplicate only while its original endpoint remains queued on that exact association. Reconsider normally after local drain; a replacement association has its own ledger.

No new timers, retry intervals, retransmission protocol, buffer settings, or claims of successful delivery. Unordered, partially reliable, and aggregate striped transports retain their previous behavior because they cannot use the same FIFO ownership inference. The map is bounded by queued chunks and pruned on subsequent submissions; association teardown releases it.

This removes retry amplification. It does not explain the initial transport stall or make the scout's short, low-queue tests a sustained production qualification. The next qualification change must measure the actual file feeding envelope (chunk bytes, outstanding requests, per-lane batch bytes and queue policy), rather than describe a longer arbitrary sample as representative.
