# Phantom bulk transport — experimental

The [native core](native/README.md) now implements ICE + QUIC with a C ABI and
Linux integration tests. The [SDK control bridge and desktop probe binding](CONTROL.md)
now connect it to scoped peer control. An installed browser helper and other
platform wrappers remain pending. The initial Node TLS reference below uses a
different wire protocol.

## Initial Node TLS reference binding

A separate bulk plane negotiated over Phantom's existing authenticated WebRTC
control connection. Messaging, roster, discovery and admission remain unchanged.
No existing app transfer path imports this package.

## Implemented

`src/node-tls.js` is the first executable **reference binding**, using the OS TCP
stack through Node TLS 1.3. It provides one reliable duplex byte stream per
one-use offer. Payloads do not pass through RTCDataChannel, JSON, base64, or an
Android JavaScript bridge. Node stream backpressure governs payload buffering.

This Node reference is separate from the native core. It runs on Node-capable
platforms; only Linux has been exercised here. Android, iOS and embedded native
bindings remain to be implemented against the wire contract below. No Node
runtime is proposed as an Android requirement.

## Adapter contract

- Listener: create transport credentials, bind an explicit endpoint, issue an
  offer bound to the two Phantom peers, and deliver an authenticated duplex
  stream to its consumer.
- Connector: accept an offer **only from the expected authenticated Phantom
  peer**, establish the carrier and verify its credential before sending a
  session credential or handing bytes to the application.
- Both: explicit cancellation and close; bounded handshake resources; no retry
  or recovery timers. Handshake/offer deadlines terminate attempts, never retry.
- Native applications embed a platform binding. A browser uses a separately
  installed helper through a future authenticated local bridge. There is no
  browser helper endpoint in this prototype.
- Reliable streams are implemented. Datagram capability must be advertised
  separately by a future carrier; this TLS binding does not support it.

The contract permits TCP/TLS, the adjacent native QUIC binding, and a later TUN/TAP front
end. A virtual interface is a consumer of the transport, not required for a
bulk stream. Picking TLS for this reference binding does not settle the final
internet carrier.

## Wire contract: phantom-bulk-tls-v1

Offers contain `protocol`, 128-bit hex `sessionId`, `from`, `to`, `host`, `port`,
SHA-256 certificate `fingerprint` (uppercase colon-delimited), and a 256-bit hex
`token`. **Offers are secrets. Do not put them in diagnostic logs, URLs or public
signaling.** Deliver through an authenticated confidential control channel.
Never accept a page-supplied offer as authenticated solely because it names a
Phantom peer. Integration with that control channel is still pending.

The listener generates fresh session credentials. Its offer expires against its
own monotonic clock; hosts need not synchronize clocks. `close()` revokes it and
tears down admitted and pending sockets. A failed credential attempt does not
consume an offer. Successful admission consumes it atomically; replays lose.
A consumed offer is not resurrected after disconnection.

1. Establish TLS 1.3. The connector verifies the exact DER certificate SHA-256
   fingerprint from the trusted offer before sending the token. This uses peer
   certificate pinning, not public CA/hostname authentication. The reference
   binding disables CA validation solely to perform this explicit pin check.
2. Connector sends a 4-byte unsigned big-endian length, followed by UTF-8 JSON:
   `{protocol, sessionId, from, to, token}`. Length must be 1–2048 bytes.
3. Listener checks expiry, unused session, protocol, both peer bindings, and the
   token using a constant-time comparison. It responds with the same framing:
   `{protocol, sessionId, accepted:true}`. Rejection closes the connection.
4. Subsequent bytes are an opaque duplex application stream; no chunk requests
   or file-specific protocol is imposed. Half-close behavior and a receipt are
   application concerns. Stream consumers handle errors and aborts after
   admission; the connector's AbortSignal covers establishment only.

## Node TLS connectivity limits — explicitly unresolved

The current binding requires a **reachable explicit TCP endpoint**. It implements
no STUN/ICE, hole punching, port mapping, relay or automatic endpoint selection.
Exchanging an offer over WebRTC does not establish reachability for this socket.
No existing browser/ICE-owned socket is hijacked or repurposed. No internet or
5G connection and no throughput improvement have been demonstrated.

Before asking users to test, implement native endpoint candidates/traversal and
bind offers to the actual admitted peer lifecycle. Prove connectivity and revoke
on peer removal, leave and shutdown. Do not ship an unauthenticated localhost
helper or send arbitrary page-selected destinations through a privileged service.

## Verification

Requires Node 22+ and `openssl` for temporary test certificates:

```sh
npm --prefix packages/phantom-tunnel test
```

The tests use real TLS sockets: sustained 16 MiB with a slow receiver, exact hash
and reverse receipt, certificate mismatch, wrong credential, replay/concurrent
claim, peer mismatch, cancellation, expired offer, oversized handshake, and a
silent endpoint deadline. Temporary private keys are generated outside the repo
and deleted. These are local correctness tests, not an internet speed benchmark.
