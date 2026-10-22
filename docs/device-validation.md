# SDK integration and device validation — October 2026

A private application exercises the public SDK on a Linux desktop and an Android Pixel 7.
The developer reports hands-on use in Chrome and Firefox, as well as the native
Android WebView app. This is evidence from real devices, not a claim that every
OS/browser/network combination was exhaustively tested.

## Reusable SDK changes

- Fence stale Nostr discovery incarnations and older SYS offers so a previous
  connection attempt cannot replace a newer one after Leave/rejoin.
- Release membership claims abandoned during failed admission. Authenticate
  returning identities independently when reusing a still-active grant; do not
  extend membership or resurrect revoked grants.
- Preserve terminal recovery states and expose a failed state that permits a
  fresh Create/Join. Fence late callbacks after cancellation or closure.
- Attempt every owned teardown even if another resource throws while closing.
- Supply shared default RTC/STUN configuration for control and distribution
  connections, preserving explicit caller configuration, including empty config.
- Offer six-character PAKE pairing through the existing doorway, alongside a
  high-entropy QR/link path. Both enroll into the same single-use membership.
  The current default invitation lifetime is four minutes, with five code
  attempts. This is a configurable default, not a protocol security guarantee.

The branch also includes the preceding verified file-pipeline, buffering,
request-window and bounded source-read work, with their documented opt-in modes.

## Hands-on observations reported by the developer

- Repeated Remove/invite and Leave/fresh-join flows, including transfers after
  rejoining. Canceling an accidentally created empty session permits Join.
- Three clients, including a client joining an existing network, with recipient
  exclusions exercised.
- File transfers on the physical desktop/phone path, including cellular use.
- Android background transfer and screen timeout followed by return to the app;
  the session stayed connected in the reported successful run.
- Network switches end the affected session and permit a fresh connection;
  cleanup improvements were followed by successful reconnection tests.
- Expired pairing codes are rejected; a current code can join.
- The private application UI was checked on desktop/mobile and in portrait/landscape.

## Validation and limits

The final functional revision passed 259 SDK tests and 59 Share tests. The
four-minute lifetime was checked in automated tests and bundled into both web
and Android assets; the developer has not deployed that final expiry change.

Physical runs guided fixes but do not establish universal throughput, NAT or
firewall coverage. Automatic Wi-Fi/cellular session migration remains deferred;
a network change terminates the session rather than promising seamless resume.
Some browser fixtures use controlled transports and do not replace device tests.
The current execution environment cannot run native browser/WebRTC visual
checks or rebuild the full Android app; prior developer device tests and the
current source/build checks must be distinguished.

Android wake locks, foreground service ownership, native network-change signals,
system insets and save permissions are app responsibilities. Share's UI,
recipient controls and diagnostics presentation are application integration,
not additional SDK requirements.

The application integration test and app-specific assets now live in the private
application snapshot; this repository contains only the reusable SDK and public demos.
