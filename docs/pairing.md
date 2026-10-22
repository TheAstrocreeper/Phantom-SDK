# Pairing codes and membership enrollment

A short human-entered code cannot reconstruct an arbitrary existing room ID,
admin public key, and unique claim token. The SDK instead derives temporary
bootstrap coordinates from a random seed, then exchanges those details with the
inviter over an existing doorway. No separate code-lookup backend is added.

`createPairingCode()` produces `P1` plus 28 Base32 characters: 128 random bits
encoded into 26 characters, followed by a two-character typing checksum. Hyphens
separate four-character groups. Parsing is case-insensitive, ignores whitespace
and hyphens, and normalizes commonly confused characters. The checksum detects
common typing errors; it is not authentication and does not guarantee detecting
every possible typo. Codes are bearer secrets: sharing one conveys the ability
to attempt the invitation's single-use claim.

Domain-separated hashes derive the bootstrap room ID and an AES-256-GCM key.
The public room ID does not reveal the code's encryption key. Each encrypted
message uses a fresh 96-bit nonce and binds its bootstrap room as additional
authenticated data. Message size, age, pending requests, and concurrent processing
are bounded. Requests are retried with the same request ID; completed responses
are cached so transport duplicates do not repeat an operation.

The code authenticates the bootstrap exchange through possession of its secret;
it is not an independent certificate of the inviter's real-world identity.
Doorways must authenticate discovery identities and packet authorship. The
included Nostr doorway verifies the signed identity binding before routing.
Custom doorway factories must provide an equivalent contract. A code holder
can attempt to impersonate an inviter to another holder of the same code;
protect codes like invitation links and obtain them from the intended inviter.

## Public APIs

- Core: `createPairingCode`, `parsePairingCode`, `pairingCoordinates`.
- `@phantom-local/doorways/pairing-channel`: `openPairingChannel`, a bounded
  encrypted request/reply channel over an injected doorway.
- `@phantom-local/doorways/pairing`: `createMembershipPairingHost`,
  `connectMembershipPairing`, `verifyMembershipPeerRequest`.
- `@phantom-local/doorways/membership-store`: `createBrowserMembershipStore`,
  an IndexedDB authority store. Async updates/signing occur outside IDB's live
  transaction; a revision comparison atomically commits only the state read.
  Conflicts retry with fresh state; failed updates never commit partial mutations.

A membership pairing host is given an existing room invite, its matching
membership authority, a saved peer signing identity, and a doorway factory.
`newInvite()` creates a unique single-use authority token and code and serves
that code's bootstrap topic. Requests prove ownership of the identity authenticated
by the doorway. The first successful redemption binds the code to that full
identity. The authority atomically rejects competing claims.

`connectMembershipPairing` returns the room invite, an admission policy suitable
for `createBrowserPhantom`, a `newInvite` request method, and `close`. Obtaining
bootstrap context alone does not redeem a code; the SDK admission checkpoint's
`authorize` performs the signed claim before main-network discovery starts.
The reply pins the admission authority to the key carried by the room invitation.
An arbitrary application should additionally enforce its own trusted authority
configuration if it requires one beyond possession of the invitation code.

The same bootstrap channel then carries authenticated membership status, signed
Leave, and optional delegation of fresh invitations. `canIssueInvite` defaults
to deny. The Share application explicitly lets current members request a fresh
invitation; it does not expose Remove to ordinary members. Its owner calls the
host's privileged `removePeer` method. Current membership is rechecked before a
member can request another invitation.

Signed revocation notifications reach connected members over those channels;
periodic current snapshots and freshness leases handle missed notifications.
Normal reconnect with the same identity remains possible while its original
membership is active. Leave/Remove invalidates it. Fresh enrollment creates a new
generation, and old code context / old tokens cannot restore the former one.
Unused topics close on invitation expiry. Revoked-member topics remain briefly
available to deliver final status before releasing their signaling resources.

The inviter must remain reachable. This exchange adds signaling traffic and
state at the inviter; it does not make discovery free of dependencies or provide
offline LAN bootstrap. The initial Share implementation uses the shipped default
Nostr relay preset on both ends. Different deployments must agree on that preset
or use another shared, authenticated doorway factory. Keep transport endpoints
and access policy configuration outside the human-readable code.

## Validation

Repository tests cover codec round-trips and checksum rejection, encryption and
tampering, real signed membership claims through a paired doorway fixture,
identity-bound single use, signed Leave, and fresh enrollment after revocation.
The Share browser smoke uses a local Nostr relay fixture with the real browser
SDK and Web Crypto. Browser enrollment and desktop/mobile layout passed here.
Native RTC did not become ready in this environment, including with the original
first build; the browser harness retains the full RTC check for a suitable runtime.
Complete SDK pairing through owner-authorized publication and verified distribution
passes with the deterministic RTC fixture. Public-relay and separate-device network
behavior still require field validation.
