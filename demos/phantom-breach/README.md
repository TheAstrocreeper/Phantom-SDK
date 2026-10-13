# Phantom Breach

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

The SDK file service defaults to `requestWindow: 8` (configurable 1–8) and `maxAttemptsPerPeer: 2` (1–3). A bounded window replaces one-request-per-peer scheduling. Timeouts can retry that peer; missing or corrupt responses exhaust that peer for that piece. Sender queues remain serialized and bounded; core WebRTC send-buffer backpressure and ordered metadata/binary pairing are retained. Binary payloads are ArrayBuffers, not encoded strings.

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
