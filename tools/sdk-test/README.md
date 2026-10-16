# Simple SDK test page

Create or join a session. See connection status and peer readiness. Select a peer, send messages or a file, and download logs.

Build from the repo root:

```
npm ci
npm ci --prefix tools/swarm
node tools/sdk-test/build.mjs
```

Host `tools/sdk-test/dist/` over HTTPS to use a phone. The creating page displays a QR; scan and tap Join on the phone. QR generation is local. A join link can also be copied. A PC-only run can use `python3 -m http.server 8000` in dist, then `http://localhost:8000`. Localhost URLs cannot be opened on another device, so their QR is hidden. Opening index.html as a file or using insecure LAN HTTP is unsupported.

Connection settings are collapsed by default. Blank doorway uses SDK Nostr discovery. Supply your own wss doorway if desired. Optional RTC configuration is local to each page and is not carried in invitations. The harness has no prefilled private endpoint.

The selected peer is used for both messaging and files. A sole ready peer is selected automatically. The page that creates the session holds the publication signing key; joined pages receive files. File downloads use verified local SDK bytes and log size/SHA-256. Submission of a message is not a delivery acknowledgment; the received message appears on the other page.

Download logs is always available, including after failure or disconnect. On the creating page it first collects reports from currently ready peers, then downloads one JSON file with local and companion evidence. Missing reports are identified; a broken connection cannot transport its own logs. Diagnostics in the same tab are retained in bounded sessionStorage across reloads. Reopen the same companion link in that tab, join, and download logs on the PC to collect prior evidence. Closing the tab, clearing storage or changing origin can lose those logs. This does not restore SDK identity, keys or file bytes. Individual local exports remain available on joining pages.

Logs contain peer IDs, messages, file names and network addresses. Invitations, RTC settings and private identity/admin seeds are not intentionally recorded. The Diagnostics section contains raw state and recent events. Clear previous retained logs removes the prior-tab journal. SDK networking and files remain memory-only.

The native browser check uses three isolated contexts, including a 390px mobile viewport. It verifies two-way messages, selected-recipient file download/hash, exclusion, PC collection of companion logs, exports after disconnect, and collection of a retained error after reload/rejoin. It saves desktop/mobile screenshots and diagnostics on failure. It does not establish physical phone radio, camera, NAT/firewall or TURN/TLS behavior. Existing swarm workflows cover automated recovery and multi-peer scenarios.

Run `node --test tools/sdk-test/harness.test.mjs`, then `node tools/sdk-test/check.mjs` with Playwright Chromium installed. The existing sdk-page workflow runs these checks automatically. SDK source is unchanged.
