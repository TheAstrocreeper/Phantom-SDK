# Phantom Breach

## Source-read comparison

Set the receiving device to **32 requests** for both runs. On the sender select
**64 KiB reads (baseline)**, send the video and wait for verification. Then select
**4 MiB reads (batched)** and send the same file again without leaving/reloading.
Download both reports. The source setting is local to the sender; the request
window is local to the receiver. Logs record the actual source block setting and
actual native block reads. This is opt-in: SDK source reads still default to the
original per-chunk mode. The new reader holds at most one 4 MiB block per service
and serializes loads across sources, coalesces nearby chunk requests, copies
individual send buffers, and retains every original chunk hash check. Sparse
requests may read more bytes than needed, so this is not a universal default.
Each new publication has a separate cache key. Stop clears retained data; close
rejects active/queued work and fences late native reads.

The native-browser local read benchmark (`tools/breach-comparison/source-read-browser.mjs`)
verified identical hashes for 32 MiB. Eight concurrent 64 KiB reads took about
198–201 ms, while 4 MiB reads took 58–63 ms in this runtime. This does not
reproduce the phone's per-read latency or establish device transfer improvement.

## Same-connection request-window comparison

This demo is recovered from `00dfcb1` and adapted to the current workspace SDK.
Build marker: `breach-source-v2`. It exercises the actual direct peer file API,
without Share's membership authority or distribution scheduler.

1. Connect the two devices once. On the **receiving** device select **8 requests**.
2. Send a file from the other device and wait for **File verified** / **Peer verified**.
3. On the receiver select **32 requests**, then send the same file again from
   the sender. Do not leave or reload either page between runs.
4. Download reports from both devices after both runs. For an order check, a
   third send with 8 requests can use the same connection.

The selector takes effect per request, locks while a transfer is active, and
does not rebuild or reconnect the SDK session. Each send uses a new
`distributionId`, so an identical file gets a distinct manifest identity and
must cross the network again rather than completing from the previous cache.
Per-chunk and full-file integrity checks remain required. Reports label the
window and run and retain bounded read/hash/send/buffer/receive/write timing
summaries without filenames or payloads. Native-buffer wait includes local
await scheduling and is not pure wire time. Both settings use 64 KiB chunks.
At 32 requests the existing 32-slot receiver and four-write bound remains.

This is a controlled device comparison, not a claimed throughput fix. Keep the
file, direction and connection the same. The recovered reference-vs-current
build tool is a separate comparison; this package has one app and one SDK.

Run `npm test --prefix demos/phantom-breach` and
`npm run test:browser --prefix demos/phantom-breach` for diagnostics and a browser
fixture that checks fresh IDs, both window settings, one session, busy locking,
verification and export. The browser fixture is not native throughput evidence.

[Open the demo](https://phantom-breach.pages.dev/)

A real Phantom SDK application: connect two screens, send a user-selected file through their WebRTC peer channel, and verify received chunks plus the full-file SHA-256. The doorway stays available for bootstrap and recovery; there is no signaling-disconnect step.

## Build

From the repository root, using Node 22 or newer:

```sh
npm install
npm install --prefix demos/phantom-breach
npm run build --prefix demos/phantom-breach
```

Host `demos/phantom-breach/dist` over HTTPS. For a Cloudflare Pages direct upload, deploy `index.html`, `style.css`, and `app.js`. Invitations use the current page URL. Keep the canvas and transfer projects separate.

## Use

1. Create a link on the laptop and scan its QR on the phone. Startup activity stays visible.
2. Once the peer channel is ready, the invitation disappears. Either device can select and send a file up to 100 MiB.
3. The received bytes must pass per-chunk verification and a full-file SHA-256 comparison. Only then is Save available. Sender progress follows receiver reports.
4. If the sender sleeps or a request exhausts its retries, keep both pages open, wake the sender, and press Resume missing pieces on the receiver. The SDK rechecks its cached pieces and requests only those missing. Cache is in memory; a reload loses it.

The app requests screen wake lock on both active endpoints when supported and reacquires it when the page becomes visible. Browsers can deny or release this request. This does not guarantee background execution; keep the pages visible.

## Transfer loop and measurements

The SDK file service defaults to `requestWindow: 8` (configurable 1–32) and `maxAttemptsPerPeer: 2` (1–3). A bounded window replaces one-request-per-peer scheduling. Timeouts can retry that peer; missing or corrupt responses exhaust that peer for that piece. Source preparation overlaps up to eight read/hash jobs with bounded admission and ordered dispatch; core WebRTC send-buffer backpressure and ordered metadata/binary pairing are retained. Binary payloads are ArrayBuffers, not encoded strings.

Verification and cache storage finish before `fileProgress` is emitted. Progress includes verified byte count, elapsed milliseconds, aggregate chunk verification/storage milliseconds, and requests in flight. This timing is not CPU profiling and may overlap when checks run concurrently.

The receiver's rate is average newly verified bytes per second from the start of its current request, including stalls. Sender rate uses receiver reports and elapsed time since the offer (or resume notification); it is not NIC throughput. Preparation time and final full-file hashing are logged separately. Cached pieces do not count as newly transferred bytes on resume. UI acknowledgments are batched every 100 ms or 32 pieces, capped at 64 indexes, and matrix redraws are coalesced to animation frames. Actual request/verification events drive the visualization.

The screen shows real verification: a matching peer-announced hash establishes file consistency with that announcement, not trust in file contents. It never executes a received file.

## Validation and limits

Automated tests exercise bounded pipeline scheduling, dropped-response retry, cached resume requesting only missing pieces, and exact bytes reconstructed through the real SDK file service on simulated transports. A controlled 20 ms request/response-latency fixture measured 677 ms with window 1 versus 94 ms with window 8 for 32 KiB. This is a fixture result, not a browser or 5G throughput guarantee.

Earlier single-request builds were user-tested across home Wi-Fi and phone 5G: a 33,123,443-byte video completed with 506/506 verified pieces and matching full-file hash. Another transfer with signaling closed reached 498/499 pieces before request exhaustion; phone screen timeout was suspected, not established. The pipelined build requires a new real-device measurement with the same file and devices before claiming a real speed improvement.

This is open admission with Nostr discovery, not the restricted Cloudflare Worker. Cloudflare serves the static page. No file bucket is used, no TURN service is configured, and connectivity is not guaranteed on every network. No private file or authority credential is embedded in the build. Large-file hashing and reassembly use browser memory. Desktop layout is compact; phones and short screens may scroll to keep controls usable. SDK diagnostics are available in the inspector.

## Interface update

Compact steel/charcoal and amber console with system Arial typography. At desktop widths of at least 1,000 px and heights of at least 700 px, a viewport grid places the transfer workflow on one screen; the SDK inspector opens as an overlay. Smaller screens retain natural scrolling for access to controls. Connection startup shows an active button spinner, elapsed waiting time, and actual network/doorway statuses. The transfer trace is a verified-progress ring, not a piece grid or simulated traffic animation. Peer-offline state clears stale latency and changes the connection heading. Revised layout needs browser confirmation after deployment.

## Recovery status and diagnostics

Loss of an established peer channel shows recovery instructions while keeping the room open. Channel return restores the active transfer message or prompts the receiver to resume missing pieces. A recovered channel does not itself claim that a file transfer completed. Copy, download, and share report controls are visible outside the inspector. Reports record channel loss/return, browser online/offline/visibility events, and allowlisted SDK reasons/actions without raw packets or identities. Test network switching on the actual phone and laptop; automated fixtures cannot establish real NAT connectivity.

### Detailed recovery reports

Reports now include the latest recovery context and a bounded history of context changes: system attempt state/fault/retry count/age, doorway deadline phase, committed and pending membership counts, terminal-exit count, and each chat channel's RTC/ICE/signaling state and fault. Peer keys, handles, attempt IDs, raw audit traces, packets, and free-form errors are omitted. Build-time extraction supplies a vocabulary of static SDK diagnostic codes; unknown text is labeled unrecognized.

A two-peer simulated Nostr outage with both RTC paths failed and signaling withheld for 25 seconds produced terminal replacement events after the ten-second replacement window and divergent solo membership. This is a candidate mechanism for the user-observed network-switch failure, not a confirmed native-browser diagnosis. Recovery policy is unchanged in this diagnostic update.

### Network-switch recovery control

Discovery silence no longer commits departure. Failed attempts wait for peer availability; the existing designated SYS opener consumes a new provider availability generation and retries. A bounded reciprocal announcement refreshes the reverse signaling route. Connection readiness still requires SDP and SYS_CONTROL proof. Manual-clock tests cover an offline phone returning after the PC exhausts its retries, including a return during an in-flight attempt, and verify another file after recovery. Runtime resource counters are included in reports. Native phone/laptop confirmation remains outstanding.
