# Native Phantom bulk carrier — experimental

This component implements an independent peer-to-peer bulk plane using libjuice
for UDP ICE and Quinn for QUIC/TLS 1.3. It exposes reliable bidirectional streams
and unreliable datagrams. WebRTC remains the intended authenticated control
channel; no file bytes are carried through RTCDataChannel or a JS/native bridge.

The core is Rust plus a small C shim to libjuice. `phantom_tunnel.h` exposes the
same implementation through a blocking C ABI, so platform wrappers need not be
written in Rust. Rust `rlib`, C static library and shared library builds are
configured. **Only Linux has been built and tested here.** Windows, macOS,
Android and iOS builds/bindings are not yet verified. This is not an installed
page helper, VPN, TUN/TAP interface or finished Share integration.

## Connection setup and authentication

1. Gather host candidates and optional STUN server-reflexive candidates from the
   **same UDP socket that will carry QUIC**. No port copied from WebRTC, no socket
   hijacking, no assumption that the existing ICE mapping applies to this socket.
2. Exchange a bounded offer over the existing authenticated, confidential Phantom
   peer channel. The offer contains protocol/session IDs, both peer names, an
   ephemeral certificate and the new ICE description. ICE descriptions contain
   secret credentials: never include offers in ordinary logs or public relays.
3. Both endpoints validate the expected peer names, session and protocol, then
   libjuice performs its ICE connectivity checks, role resolution and nomination.
   No TURN server or bulk relay is configured or exposed by this adapter.
4. Quinn sends QUIC packets through that nominated ICE path. TLS requires mutual
   certificate authentication, trusting only the certificate in the expected
   peer's offer. A reliable setup stream confirms the session and both names
   before the application receives a usable tunnel.
5. The prepared object is consumed once. Each session admits one QUIC connection;
   additional incoming connections are disabled after establishment. A new
   session needs a new prepared object, ephemeral certificate and fresh random
   128-bit session ID from the control layer.

The C/Rust caller MUST bind the offer to the real authenticated peer. Matching
textual peer names alone does not establish identity. The current probe manually
routes offers; the SDK control-plane integration is still to be implemented.

## APIs and ownership

Rust: `PreparedTunnel::gather(Options)` → `offer()` → consuming
`establish(remote_offer, initiator)` → `Tunnel`. The tunnel exposes Quinn streams
and datagrams through `connection`, plus selected network addresses, local packet
queue drop counts, and explicit close. Keep `Tunnel` alive while using its
streams; dropping it closes the session. An embedding runtime can cancel setup
by dropping the establish future. There is no application recovery/retry loop.

C: prepare → obtain/forward offer → connect/accept → open/accept streams →
read/write/finish, or send/receive datagrams → close/free. Read the ownership
contract in `phantom_tunnel.h`. Call blocking functions on platform worker
threads, never UI or Tokio threads. C close cancels in-progress establishment
and active I/O. Do not free handles concurrently with any call using them.
Streams retain their session/runtime until freed. A failed/partial timed-out
write resets that stream so retrying a whole buffer cannot silently duplicate
bytes. Zero-byte successful read means EOF. C datagrams are limited to 1100 bytes.

The C status function returns credential-free JSON with state, carrier, selected
network addresses and local packet-drop counts. C errors are operation-level
codes; platform wrappers must label the operation that failed.

QUIC stream and connection windows bound flow control (4 MiB/8 MiB); the adapter
uses a bounded 1024-packet receive queue and 256 KiB datagram queues. Oversized
or overflowing UDP arrivals are dropped and counted. libjuice exposes no socket
writable notification; an EAGAIN send is treated as counted local UDP loss for
QUIC to recover, rather than implementing a polling/spinning retry loop.

Quinn sees stable logical addresses for its one-peer abstract socket; these are
not actual localhost network sockets. Use `selected_addresses()` / C status for
the real ICE path. MTU probing is disabled and QUIC uses 1200-byte packets for
this initial adapter. ICE/QUIC retain their standard protocol retransmission,
consent and congestion-control timers; no Phantom retry timer has been added.

The helper disables libjuice's process-global text logging once before starting
agents, so credentials cannot leak through upstream stdout diagnostics. Co-hosting
another libjuice user in the same process needs a logging ownership review.

## Build and verification

Requirements: stable Rust, C/C++ build tools, CMake 3.16+, Git, Node 22+ and
Python 3. The Node TLS baseline tests additionally need `openssl`.

Use an unmodified libjuice checkout at this pinned revision (the build verifies
HEAD and tracked-file cleanliness). No dependency source is fetched by build.rs:

```sh
git clone https://github.com/paullouisageneau/libjuice.git /path/to/libjuice
git -C /path/to/libjuice checkout cb523763255f241442da1fbdd72c498f33a54b35
export PHANTOM_LIBJUICE_SOURCE=/path/to/libjuice
npm --prefix packages/phantom-tunnel run test:native
```

That command uses Cargo.lock and runs:

- Five Rust integration tests: 16 MiB hash-verified stream, reverse streams and
  datagrams; peer/session mismatch; untrusted certificate; unreachable deadline;
  invalid scope/deadline validation.
- A separate two-process probe: JSON control offers are forwarded through the
  controller, while 32 MiB travels over native QUIC UDP sockets and is verified
  at both ends. Only setup/receipt events pass through the process pipes.
- Four C ABI tests through Python ctypes: 4 MiB hash-verified transfer and return
  receipt/datagram; cancellation during connection; consumed handles and buffer
  validation; STUN-only connection through the filtering fixture.

The final test runs a local STUN responder and test-only address mapping/filter
simulator. Host candidates are removed from the exchanged offers. Each peer
must send outbound checks to open its own inbound filter; at least one early
packet must be filtered, and the selected remote address must be an advertised
mapping. QUIC then carries a verified datagram through that selected path.
This exercises the actual ICE path, **not a complete model of router/CGNAT
behavior**. The fixture is not a production relay or fallback.

The tests explicitly bind loopback because this execution environment denies
interface enumeration. Automatic host enumeration, external STUN, real router
NAT/5G and non-Linux platform builds remain unverified. No real-world speed
improvement has been established. Local throughput is not a substitute.

For Rust lint checks:

```sh
cargo clippy --locked --all-targets --manifest-path packages/phantom-tunnel/native/Cargo.toml -- -D warnings
```

## Probe executable

`cargo build --locked --example native_probe` produces a small native worker
with a bounded JSON offer exchange on stdin/stdout. Its arguments are documented
in the example's usage error. Optional STUN and an explicit bind address allow a
controller to supply endpoint policy. The controller must authenticate remote
offers, enforce its own overall deadline and kill the process on peer removal,
leave or shutdown. Do not expose its stdin or a privileged socket API directly
to an untrusted page. The probe is a fixed-byte measurement tool, not a general
helper service or automatically integrated benchmark in Share.

## Remaining integration

Add platform wrappers/build validation around the C API, then a scoped SDK
control message/peer-lifecycle bridge and an authenticated local desktop helper.
Bulk data must stay in native streams/native file handles. Bind tunnel closure
to peer removal, leave and shutdown before exposing it to Share. Test direct
connectivity on the actual PC ↔ phone 5G path before judging performance. A
failed direct path must remain an explicit failure, with no hidden bulk relay
and no transfer benchmark silently falling back to WebRTC.
