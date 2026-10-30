# SDK control bridge and desktop probe binding

The SDK now exposes a dedicated `sendBulkControl(peer, frame)` /
`onBulkControl(listener)` API on the joined network and low-level core.
`createBulkProbeControl` coordinates native probes through that API. There is
no chat-message encoding, doorway/relay delivery, deferred send queue, or
application retry loop for bulk setup.

## Peer and credential boundaries

The engine sends and accepts frames only on the current admitted/accepted
peer's open, `chat_hello`-verified WebRTC channel, with the normal membership,
block/revocation and `canUsePeer` checks. Temporary SYS/bootstrap channels are
not used. The native offer is checked against the actual channel peer, local
identity and session; JSON identities do not replace channel authentication.

Frames are bounded/canonicalized; the initial coordinator supports only the
finite `probe` purpose and a bounded byte count, not arbitrary commands, socket
destinations, file paths or shell execution. Native offers are credential-bearing
and go only to dedicated listeners: they return before chat handling, general
SDK events or RTC debug logging. Status callbacks contain only peer/session,
state and normalized reason codes. Native stdout is parsed privately and native
stderr is not forwarded into page diagnostics.

No recipient allocates a native worker until its explicit `allowIncoming`
callback approves that peer and workload. The default declines all incoming
work. Limit approval to the intended authenticated test peer and size.

Each coordinator has at most eight sessions and one session per peer. Incoming
requests are tracked before asynchronous approval/preparation, duplicates cannot
replace credentials, and recently closed session IDs have a bounded replay
cache. Simultaneous competing requests fail explicitly; they do not silently
retry. A one-shot setup deadline ends a stalled attempt. There are no automatic
recovery/retry timers.

Membership/peer-state changes, network close/retirement, explicit drop and the
setup deadline abort pending work and close the native owner. Late prepare or
connect results are fenced against the exact session and disposed instead of
being attached to a replacement. An incoming `complete` control packet never
substitutes for the native worker's local completion/receipt.

## Desktop reference adapter

`createNativeProbeAdapter` starts the built `native_probe` executable directly
with argument arrays (`shell:false`); the executable path, bind policy and STUN
server are trusted local configuration, not peer-controlled. All probe payload
generation, native stream I/O and hashing happen in that process. JavaScript
handles only bounded JSON offers and receipts. Abort closes the worker.

Imports:

```js
import {createBulkProbeControl} from '@phantom-local/tunnel/control';
import {createNativeProbeAdapter} from '@phantom-local/tunnel/node-probe';
```

Supply a joined SDK network, a configured desktop adapter and an explicit incoming
approval policy to the coordinator. Once the SDK reports the peer ready,
`await control.open(peer, {bytes: 32 * 1024 * 1024})` returns a native probe handle;
`await handle.result` supplies the native hash-verified byte count/timing receipt.
`control.dropPeer(peer)` or `control.close()` tears down the owner. `snapshot()`
and `onState` deliberately exclude offers and ICE credentials.

The Node adapter is a desktop host/reference binding. **It cannot be imported
into an ordinary browser page.** An authenticated installed-helper bridge between
the page and native host remains to be built, along with mobile/platform wrappers
around the C ABI. No privileged localhost HTTP/WebSocket service has been exposed
by this commit. There is no change to Share or its normal file-transfer path.

## Validation and limits

`node --test tests/bulk-control.test.js` exercises the real SDK engine/facade
with its paired in-memory RTC test transport: consent/ready gating, denial,
private event routing, malformed identities/credentials, delayed preparation,
revocation, approval cancellation, deadlines, replay and competing requests.

The native verification script additionally runs `test-control-integration.mjs`:
two admitted SDK instances use that control API to launch separate native
workers and verify a 32 MiB transfer over real loopback ICE/QUIC UDP sockets.
Only control is represented by the SDK's memory RTC fixture; bulk traffic is
not mocked. The receipt/hash and absence of offer material in general events
are checked. The desktop worker's cancellation path is also exercised.

This verifies the integration contract, not an actual browser/WebRTC network
run, deployed helper installation, other OS builds or the PC ↔ phone 5G path.
It establishes no internet throughput improvement. Use a fresh native session
for an actual connectivity test when both platform bindings are ready.
