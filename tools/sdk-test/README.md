# Barebones SDK test

No styling, service endpoint, reconnect loop or mocked SDK. Memory storage only; reload ends this peer and discards its chunks. The page runs the public browser entry with automatic messaging and signed, selected-recipient distribution. A fresh content administrator is created for each new network.

## Run the ready-built copy

Serve the ZIP's top-level directory with `python3 -m http.server 8000`, then open `http://localhost:8000`. For another device, host these static files on HTTPS. Opening index.html as a file is unsupported.

1. Optionally enter your own WebSocket doorway and RTC configuration. Blank doorway uses SDK Nostr discovery; blank RTC uses SDK defaults. Nothing points at a private Cloudflare worker. ws is intended for loopback testing; HTTPS pages require wss.
2. Click Create. Copy the raw invitation to the other page's Invitation field and click Join. Each independent page creates its own identity. WebSocket invites carry their supplied doorway URL; RTC credentials must be configured locally on every page and are not in invites.
3. Inspect actual SDK state and messaging readiness. Enter a peer ID and send text. Submission is recorded separately from the receiver's message event; the app adds no retry or readiness queue.
4. Select recipient checkboxes, choose a file, and distribute it. A receiver's download link is made from bytes returned by getDistributedFile. Its SHA-256 and size are recorded; compare them to file.published in the sender report. The manifest ID is not the whole-file SHA-256.
5. Download JSON reports from each peer. Record snapshot to capture current state. Leave/cancel retains diagnostics, and reports still download afterward. The latest 1,000 application events are retained with a dropped count; the SDK's bounded RTC history is included in the latest snapshot. Reports contain messages, file names, peer IDs and network addresses; review before sharing. RTC configuration and invitations are not added to reports.

There is no automatic pass indicator or claim that public relay discovery, NAT traversal, or TURN will succeed. Failures remain visible. Persistent reload/resume and forced connection loss remain covered by the separate native contract harness; this page does not implement storage or fault injection.

## Inspect and rebuild

The ZIP includes the SDK and test source under source/. From that directory:

```
npm ci
npm ci --prefix tools/swarm
node tools/sdk-test/build.mjs
```

Serve `tools/sdk-test/dist/`. build.json and every report record the revision, dirty state and source hashes. The build bundles the real SDK; it does not fetch an SDK from a CDN.

## Verify the page itself

With Chromium installed for Playwright (`cd tools/swarm && npx playwright install chromium`), run `node tools/sdk-test/check.mjs` from the source root. PHANTOM_CHROMIUM_PATH can point to another installed Chromium.

The check operates the actual page controls with three isolated browser contexts and native WebRTC over the existing loopback test doorway. It downloads and hashes the received file, checks receiver exclusion, downloads reports from all peers, and checks report download after Leave. This establishes single-host behavior, not real NAT or firewall reachability.
