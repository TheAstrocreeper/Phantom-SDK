# Phantom Breach

An experimental two-device demo using the actual Phantom SDK. Create a room, scan its QR code, close signaling on both devices, then transfer a file chosen by the user. The receiver verifies each SDK chunk and independently compares a full-file SHA-256 hash before offering the file for download.

## Build and deploy

From the repository root, with Node 22 or newer:

```sh
npm install
npm install --prefix demos/phantom-breach
npm run build --prefix demos/phantom-breach
```

Serve `demos/phantom-breach/dist` over HTTPS. All invitations are generated from the current page URL; there is no hard-coded host. For Cloudflare Pages, upload the three runtime assets (`index.html`, `style.css`, `app.js`) as a new project. Keep the canvas project separate.

## Run the live test

1. On the laptop, click **Deploy network**. Scan the QR with the phone.
2. Wait for an open peer data channel and measured round-trip replies.
3. On either device, click **Cut signaling**. The devices exchange request/ready/commit messages through their data channel. Each calls `network.disconnectDiscovery()`, closes its monitored WebSockets, and reports isolation to its peer.
4. Confirm both screens show zero relay sockets and the peer data channel stays open. If the cut fails, file injection stays disabled.
5. On the phone, select a file and click **Inject payload**. Start with a small file, then test larger files up to 100 MiB.
6. Confirm **Verified: PASS** on the receiver and **Peer verified: PASS** on the sender. Save the received file and inspect it yourself. These are separate local and peer-reported results.
7. Wait at least a minute after the cut and repeat with a different file to look for delayed membership/recovery problems. Closing a peer page should cause the channel count to fall; there is no artificial completion animation.

## What is real

- Network creation, admission state, peer invitation, messaging, file offers, chunks, and verification run through workspace SDK source.
- `relay-monitor.js` counts actual browser WebSockets and forbids every future constructor after the cut. `disconnectDiscovery()` closes the core doorway and permanently prevents that core from re-opening it. This operation does not call network close or intentionally tear down existing RTC channels.
- Latency is application round-trip time measured with `performance.now()`, including both devices' event loops. It is not ICE latency.
- Matrix cells illuminate only for `fileProgress` emitted after the SDK verifies and stores a chunk. Sender cells reflect receiver acknowledgments, rather than pretending a successful send is successful delivery. More than 1,500 chunks are grouped into cells; the exact piece count is shown separately. At 64 KiB per piece, 100 MiB is 1,600 pieces.
- Full-file hashes are computed from the selected Blob and the reconstructed received Blob. A peer-supplied hash proves consistency with that peer's announcement, not independent authenticity or trust in file contents.

## Scope and validation

This initial version uses **open admission and Nostr discovery**, matching the working canvas setup. It does not use the Cloudflare Worker relay template, developer credentials, or a server connection challenge. Anyone possessing an invitation can join. Cloudflare can host the static app, but does not receive the file payload through this application's transfer path.

Automated checks cover real-core discovery closure/no reopening, socket closure/constructor blocking, and the actual file service transferring verified bytes on a simulated data-channel transport. These checks do **not** prove browser WebRTC survival after the cut. That real laptop/phone test is pending; the demo is marked experimental until it passes. There is no TURN service or guarantee for every network.

No relay server is shut down: only these clients' signaling sockets are closed. Losing or renegotiating an RTC channel may require a new room. The app keeps files in memory, limits file size to 100 MiB, and automatically requests the received file after injection. Nothing executes the file. Large-file hashing and reassembly may be expensive on mobile.
