# Solo PC/mobile SDK tests

The controller runs checks and downloads combined reports. The companion joins by scanning a QR on the PC, taps Join, and keeps its page in the foreground. The public SDK handles networking; the harness adds test commands and report acknowledgments over SDK messaging. No application reconnect loop or SDK changes.

## Run

Build with `npm ci`, `npm ci --prefix tools/swarm`, then `node tools/sdk-test/build.mjs`. Host `tools/sdk-test/dist/` over HTTPS for phone use. A PC-only check can use `python3 -m http.server 8000` in that directory and `http://localhost:8000`. LAN HTTP, `0.0.0.0`, and file URLs cannot run this crypto-dependent test. Deploy only the dist directory; it contains bundled dependencies and notices. QR generation happens locally without a third-party QR service. A localhost QR is intentionally hidden because it would point at the phone itself.

1. On the PC, optionally configure a WebSocket doorway and local RTC configuration, then Create. Blank doorway uses Nostr discovery; blank RTC uses SDK defaults. The harness contains no prefilled private endpoint. HTTPS requires wss. RTC settings/credentials are never embedded in the QR and must be configured on each participating page if needed.
2. Scan the QR with the phone camera and open the link. Tap Join. The mobile link preloads the invitation and doorway; these remain in the URL fragment and are not added to reports or diagnostic storage. Treat a join link as network access information.
3. On the PC, Run baseline sends an acknowledged test message and a generated 256 KiB file. The receiver reads actual local bytes and verifies their SHA-256 and size, then the PC collects its report. Every run uses a distinct distribution ID so an earlier local copy cannot satisfy a repeat transfer.
4. For a network-switch check, click Start network-switch check on the PC. Once armed, switch Wi-Fi/cellular on the phone and return to its page without reloading. Click Verify after switching on the PC. It waits up to 90 seconds for an acknowledged message and collects the companion report. A pass requires an observed loss of messaging readiness or unanswered probe. Without that evidence, the result is INCONCLUSIVE. This does not prove which route was used, distinguish browser suspension from a network interruption, or verify file transfer after recovery.
5. Download combined report on the PC. Collect reports refreshes every ready companion. Failed collection is queued and retried on a later ready event; the button also retries manually. Reports include collection times and pending peer IDs. A network that remains broken cannot transport its own logs.

## Retained failure diagnostics

Each tab retains a bounded diagnostic journal in sessionStorage (up to 350,000 characters). It saves current state, up to 250 recent events, test results, and native RTC history when it fits. Large histories are trimmed; storage failures are shown. Reloading the same tab restores its prior journal for inclusion in its next report. Rejoin using the same companion URL, then Collect reports on the PC. The SDK creates a fresh peer on reload; it does not restore identity, admin keys, file bytes, or network state. An old peer may remain listed as pending while the new peer supplies the retained evidence. Closing the tab, clearing browser data, changing origin, or storage restrictions can lose the journal. Native full reports remain available as a manual fallback.

Reports travel as requested, acknowledged 6,000-character parts, with a maximum 1.5 million characters and bounded report caches/command deadlines. They contain SDK events, messages, names, network addresses, build hashes, current state, and retained diagnostics. Clear retained diagnostics removes this tab's saved evidence. Do not enter secrets in chat messages. RTC settings, invitations and identity/admin seeds are not intentionally recorded by the harness.

## Manual inspection and limitations

Developer state and event log are collapsible. Manual messaging, selected-recipient publishing from the creating page, received-file downloads and individual reports remain available. Report export must preserve creator publication controls. Network/file storage is memory-only. Reload discards files; this page is not the durable-store demo.

Baseline success establishes those specific checks on the participating clients. Neither a desktop mobile viewport nor loopback CI establishes real mobile radio, NAT, firewall, TURN/TLS or device permission behavior. Use the existing swarm and TURN workflows for automated multi-peer, publisher-departure and durable restoration coverage instead of manually operating extra devices.

## Verification

`node --test tools/sdk-test/harness.test.mjs` checks journal persistence/quota handling and local QR encoding. With Playwright Chromium installed, run `node tools/sdk-test/build.mjs` and `node tools/sdk-test/check.mjs`. The existing sdk-page workflow installs Chromium, runs both, and uploads reports even on failure. Browser checks exercise native loopback RTC with three contexts, including a 390px mobile viewport, selected/excluded recipients, actual file/report downloads, the acknowledged baseline, inconclusive recovery without a fault, recovery after closing a native messaging channel, and retained evidence after reload/rejoin. These are harness checks, not new production connectivity claims.
