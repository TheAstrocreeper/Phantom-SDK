# Short pairing prototype

Status: experimental local integration in Phantom Share. Nothing has been
deployed. SDK callers without an injected PAKE implementation retain long P1
pairing. This work has not undergone an independent security audit.

The prototype generates exactly six Crockford Base32 characters, for example
`7KH9T2`, without a required separator. Entry ignores whitespace/hyphens and
case, maps O to 0 and I/L to 1, and rejects other invalid characters. The
alphabet omits ambiguous glyphs; each generated character is sampled uniformly.

The first two characters select a public routing bucket (1,024 possibilities).
The remaining four are the PAKE secret (32^4 = 1,048,576 possibilities, 20 bits).
Changing any secret character leaves the relay topic unchanged. Hashing the
whole code into the topic would expose an offline secret verifier and is avoided.
The secret never appears in a public verifier, relay topic or transmitted field.
This split fits the six-character typing limit; it is not six characters of
secret entropy. Five independent guesses against one invitation have probability
at most 5 / 1,048,576 (about 1 in 209,715), assuming uniform generation and the
underlying PAKE's guarantees. Limits reset with a new invitation, so this is not
a lifetime security bound. An observer can exhaust the budget and deny pairing.

No additional Cloudflare lookup service is proposed. Discovery and handshake
messages would use the existing Nostr relays; those messages still incur relay
traffic and availability limits. An experimental doorway adapter now implements this exchange; public-relay and
real-browser validation remain outstanding.

Each exchange uses fresh SPAKE2 state through pinned spake2-wasm 0.2.0, a Rust
implementation compiled to WASM. The package is a development dependency only.
No independent audit of this package was established during this investigation.

The PAKE application context binds the protocol version, locator, both device
public keys, a client nonce and a host nonce. HKDF separates client-confirmation
and invitation-encryption keys. The host verifies HMAC confirmation before
AES-GCM encrypting the existing long invitation. The client authenticates that
reply and checks its embedded identities. Existing identity-bound membership
admission can then redeem that invitation through the current path.

The host admits at most five handshake starts per invitation across identities,
expires after four minutes, and permits one successful consumer. Retransmitted
hellos reuse the challenge and budget entry. Finalization is serialized per
handshake and has a single winning consumer across peers. Close releases unused
WASM state and fences late completion. A failed handshake consumes a budget
entry. Secret storage is subject to normal JavaScript/WASM memory limitations.

Validation: sixteen automated tests exercise the actual WASM primitive,
matching/different secrets, transcript/identity binding, modified ciphertext,
replay, expiry, global budget, retransmission, concurrent consumers, and the
package's browser-oriented glue with Web Crypto in a Node execution harness.
That harness is not a real-browser or physical-device test.

## Experimental relay channel

`short-pairing-channel.js` runs the exchange over the existing doorway interface.
It requires identity-authenticated discovery and signals, as provided by the
Nostr doorway. It binds responses to a requested peer and checks the sender's
current boot ID. It does not create another service or use Cloudflare APIs.

The client tries at most five distinct host identities per channel, including
wrong-code candidates in the same bucket. The budget is shared across discovery
updates; a host identity never gets another attempt just by re-announcing. Each
candidate has independent PAKE state. The host keeps the global five-start limit.
RPC IDs and hello nonces survive one-second retries. Bounded response caching
makes duplicate requests reuse the same result without another PAKE attempt.
There are at most sixteen simultaneous incoming handlers, twenty cached host
RPC results, and a fifteen-second default client deadline. Cancel, close and
expiry reject pending requests, dispose PAKE state, remove abort listeners and
stop retry timers. A successful client closes its bootstrap channel immediately;
the consumed host stays open until close/expiry to resend a dropped final reply.

Six channel tests cover same-bucket hosts with different secrets, dropped
replies and duplicate messages, hostile candidate limits, abort, expiry, and
an exchange through the actual Nostr doorway adapter with an in-memory relay
bus. Those tests exercise routing and state management, not live relay sockets
or Nostr signature validation (the bus stands in for verified relay events).

## Remaining before production

The local Share build now loads bundled native-ESM glue and WASM from its own
static host. The emitted loader was exercised in Node with file-backed fetch,
including concurrent initialization and an actual SPAKE2 exchange. Real-browser
execution remains unverified: Playwright's Chromium binary is absent and its
download failed in this environment. Public-relay validation and independent
review of the complete protocol/dependency remain outstanding.

Optional PAKE injection enables short-code membership bootstrap. `newInvite()`
returns the short `code` and separate high-entropy `linkCode`; both lead to the
same authority token with a four-minute lifetime. A successful bootstrap marks the
short code used immediately, even before membership redemption. The demo replaces
it so a later admission failure does not leave a consumed code looking usable.
QR/link redemption also closes the short channel. Active invitations from one
host reserve distinct routing buckets to avoid same-identity doorway conflicts.
The membership integration test covers enrollment, rejection of another identity
using the corresponding link, release, and fresh membership via a new link.
Existing SDK callers without PAKE injection retain their original behavior.

A hostile relay or many hostile host identities can crowd the five-candidate
budget and deny pairing. The client must surface failure rather than silently
retrying indefinitely; the user can generate another invitation. A routing
bucket is never a unique device identifier.

Full six-character collisions are possible too. PAKE authenticates possession of
the code, not a previously known device identity. This experimental adapter
accepts the first successfully authenticated invitation; it cannot distinguish
two hosts that happen to share the entire code. Production policy for that case
and the aggregate guessing risk across many simultaneous invitations still need
review. QR/link pairing remains primary and carries a high-entropy invitation.
