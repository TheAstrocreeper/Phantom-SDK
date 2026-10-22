# Local connections (experimental)

The `@phantom-local/doorways/local` export provides the reusable local bootstrap
and encrypted doorway used by Phantom Share. It has no Android, UI, Node server,
mDNS, public relay, or file-saving dependency. Existing online pairing stays compatible.

## Host responsibilities

A host application supplies an ordered duplex transport with `send(string)`,
`listen(receive, fail)` returning an unsubscribe function, and optional `close()`.
Messages must arrive once and in order; loss, duplication, or reordering fails the
channel closed. The adapter adds identity-signed, sequenced AES-GCM envelopes,
with bounded 64-message queues, 64 KiB envelopes, and at most 16 room contexts.
It supports two devices, not general LAN multicast or multi-peer discovery.
The host learns the guest identity from its first authenticated envelope. The
joining client pins the host key. Anyone with the complete bootstrap secret can
attempt that first identity claim; release it only through authenticated pairing.

```js
import {createLocalDoorways} from '@phantom-local/doorways/local';

const hub = await createLocalDoorways({
  identity,        // SDK identity with publicKey and signPeerProof
  secret,          // 32 random bytes, lowercase hex; never a PIN-derived key
  hostKey,         // host identity public key, lowercase hex
  host: isHost,
  transport,       // application-supplied native socket or local HTTP adapter
  onError,
});
// Pass () => hub.doorway() as the membership pairing doorwayFactory.
// Use the same adapter for the main network's LAN doorway descriptor.
// Configure every participating SDK instance with rtcConfiguration: {iceServers: []}.
// Do not configure public fallback doorways in the local network invitation.
// Close the hub when its owner session ends.
```

`hub.doorway()` is an event-driven adapter: membership pairing does not start its
legacy reconciliation interval. Request deadlines remain bounded. No new periodic
retry loop is introduced. An event wait in the transport must block until a message
or end event; it must not return an immediate empty response in a busy loop.

`browserLocalTransport({endpoint, token}, {fetch})` implements the Android host's
existing `/send` and `/receive` HTTP protocol. It accepts only canonical RFC1918
IPv4 HTTP endpoints on ports 1024–65535 and a 32-character uppercase hex capability.
It omits credentials/referrers, refuses redirects, bounds responses and aborts its
wait on close. The capability authenticates HTTP routing, not message contents;
HTTP exposes routing metadata and capabilities to LAN observers. Use the separate
high-entropy envelope secret. Secure-origin browser permission/mixed-content rules
can still prevent local HTTP access. This adapter does not start a listener.

## Short local pairing

`createPinPairingHost` and `createPinPairingClient` take an injected SPAKE2 provider
(`start`, `msg`, `finish`), both identity public keys as appropriate, a 48-character
lowercase hex room ID, and an exact four-digit string PIN (including leading zeroes).
The host also takes a bounded opaque invitation string. The encrypted result is
bound to both identities, room, and handshake nonces; it is not an SDK membership
credential by itself. The application validates its bootstrap payload before use.

```js
import {createPinPairingHost, createPinPairingClient}
  from '@phantom-local/doorways/local';
const host = createPinPairingHost({room, pin, hostPublicKey, invitation, pake});
const client = createPinPairingClient({room, pin, hostPublicKey, peerPublicKey, pake});
try {
  const challenge = host.begin(peerPublicKey, client.hello);
  const answer = await client.answer(challenge);
  const sealed = await host.finish(peerPublicKey, client.hello, answer);
  const bootstrap = await client.open(sealed);
} finally { host.close(); client.close(); }
```

The application carries those public handshake messages over local HTTP/WebSocket
or another transport. Never send the raw PIN or use PBKDF2 challenge proof as a
replacement for PAKE. The default lifetime is four minutes, checked on actions.
At most three distinct handshake starts are allowed globally per PIN invitation,
including parallel clients; `maxAttempts` cannot increase that budget. One valid
finish wins. Attackers can exhaust the budget, requiring a fresh invitation.
No IP-based security assumption is made.

`createLanPairingHost` / `createLanPairingClient` retain the separate existing LAN
transcript with a private IPv4 route and five-symbol password. The default budget
there is five attempts. `encodeLanPairingCode`, `parseLanPairingCode`, and
`newLanPairingPassword` encode the private route and password as a 16-symbol code.
Its CRC detects typos, not attacks. The code must be kept secret. Neither format
performs browser LAN scanning or guarantees that a `.local` hostname resolves.

`createMembershipPairingHost({inviteTtlMs, ...})` lets applications choose their
membership invitation lifetime without changing legacy SDK defaults. Share sets
240000 explicitly; the authority's own maximum lifetime still applies.

## Browser build integration and validation

`@phantom-local/doorways/browser-pake-glue` exports `browserPakeGlue(original)` for
the pinned spake2-wasm 0.2.0 ESM glue. It removes the dynamic global-object lookup
while preserving the WASM import ABI, and refuses unfamiliar glue formats. Serve
the adapted glue and WASM locally. This helper does not initialize WASM, configure
CSP, provide certificates, or make an HTTP page a secure context.

Coverage includes real SPAKE2 and CSP execution, transcript binding, concurrent
single-winner admission, budget/expiry/cancellation, route parsing, ciphertext
replay rejection, and SDK membership enrollment/leave. Share retains its complete
HTTPS → PIN → membership → bidirectional file integration test with remote fetches
blocked and empty ICE configuration. Memory RTC tests do not prove physical LAN
or browser compatibility. Connected-LAN PC/TV testing succeeded; a physical
WAN-disconnected test, mDNS resolution, and background behavior remain pending.
