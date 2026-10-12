# Shared canvas

A small, real application of the Phantom SDK: create a room, scan its QR code on a phone, and draw from either screen. The network inspector reports actual SDK state, peers, open data channels, and application message counts.

## Build

From the repository root:

```sh
npm install
npm install --prefix demos/shared-canvas
npm run build --prefix demos/shared-canvas
```

Serve `demos/shared-canvas/dist` over HTTPS on a host reachable by both devices. A localhost URL on your laptop is not reachable from your phone. The QR encodes that hosted page plus a compressed, validated SDK invitation in its query string (so sign-in redirects can retain it); older clients can use the ordinary invite fragment format. QR generation is local.

This example imports the workspace SDK source. `src/session.js` shows room creation/joining, peer invitation/acceptance, messaging, event subscription, and cleanup. `src/model.js` supplies bounded drawing operations, duplicate detection, deterministic clear ordering, and late-join synchronization. `src/app.js` binds pointer/touch input and the UI to those functions.

## Network policy

This is an **open invitation demo**. Anyone with the link can join and edit, including clearing the canvas. It does not advertise private authorization. The app uses the SDK's default Nostr discovery and actual WebRTC channels; relays carry signaling, not the drawing operations. No canvas storage service is used.

Use `sdkOptions` in `openCanvas` to select a WebSocket/custom doorway or supply the documented authorization hooks for a restricted application. Both participants must use the same configuration. Direct connection success depends on the devices' networks. The demo does not add a TURN service or guarantee cross-network NAT traversal.

## Application behavior

Strokes are normalized to a fixed canvas aspect ratio and sent in small batches. Existing drawings sync when a peer's data channel becomes ready. Simultaneous clears use a logical clock and stable tie-break order; old strokes cannot revive a cleared canvas. The board has a 4,000-segment limit; clear it to continue. Nothing is saved after all peers and pages leave.

The inspector counts application messages queued through `network.send` and messages actually received. These are not bandwidth or delivery-acknowledgment measurements. Connection state comes from the SDK and its snapshot, not a simulated network animation.

`npm test` includes convergence, malformed-operation, and invite round-trip checks. The application is designed for contemporary browsers with WebRTC, Pointer Events, secure contexts, and clipboard access; copying the displayed link remains available when clipboard permission is denied.

The native-WebRTC integration attempt on the implementation host produced no ICE candidates and could not establish a peer connection. It is not recorded as a passing end-to-end drawing test. A subsequent user-run laptop/phone check confirmed an active peer data channel and canvas messages received. This confirms that two-device session; it does not establish connectivity on every network.
