# Membership authority (optional SDK primitive)

`createMembershipAuthority` adds single-use invitation accounting and revocable,
identity-bound membership. It is separate from content publication/revocation and
network retirement, but uses the same admin signing identity. Existing open
networks, v1 admission grants, and invite formats remain compatible.

The browser SDK can enforce signed revocations on established connections through
`createMembershipAdmission`. Share UI wiring, authority request transport, durable
backend selection, and compact pairing codes remain application/integration work.

## Authority and persistence

```js
import {createMembershipAuthority} from '@phantom-local/core';

const authority = await createMembershipAuthority({
  networkId,
  adminIdentity, // private signer stays at the authority
  store,        // durable atomic transaction implementation, below
  verifyPeerRequest, // verifies the exact request against the peer's full public key
});

const invitation = await authority.issueInvite();
const credential = await authority.redeem({token: invitation.token, request});
```

The privileged authority object must remain private. `issueInvite` and `remove`
are local admin methods, not public unauthenticated endpoints. Peer-facing
`redeem`, `reconnect`, and `leave` require an application-supplied
`verifyPeerRequest` that returns exactly `true` only after authenticating the
request. A caller-provided public key is not authentication.

The verifier receives `{networkId, action, inviteId?, membershipId?, request}`.
The peer signature must bind the network ID, action, and invite digest for
redemption, or membership ID for leave. It must prove ownership of
`request.peerPublicKey`, enforce freshness, and reject tampering. The existing
`signPeerProof` / `verifyPeerProof` helpers can implement this contract; the
membership tests demonstrate this with actual peer signatures. They are not
implicitly applied by the authority. Do not transmit bearer tokens through public
relay announcements. Request transport must protect tokens in transit.

The store implements:

```js
await store.transaction(scope, async storedState => {
  // storedState is undefined for a new authority.
  return {state: nextState, result: operationResult};
});
```

Transactions must serialize across every writer to the same scope, isolate
uncommitted mutations, roll back on failure, and durably commit before resolving
with `result`. A successful read-modify-write over `localStorage` is not an atomic
transaction. Multiple authority instances require the same transactional backend.
`createMemoryMembershipStore()` is available for tests and ephemeral sessions;
it does not survive process termination. Authority private-key custody and
persistence are application responsibilities. Treat stored state as trusted
security data and protect it against rollback or unauthorized modification.

## Lifecycle

- `issueInvite({ttlMs, membershipTtlMs})` returns a network ID, random bearer token,
  and invitation expiry. Defaults are five minutes and 24 hours respectively.
  Only the SHA-256 digest is retained in authority state.
- `redeem({token, request})` atomically binds that invitation to one full peer
  public key and a new random membership ID. Concurrent claims have one winner.
  The returned Ed25519-signed v2 admission credential binds network, identity,
  membership ID, and expiry. The membership ID is public, not a secret.
- Retrying redemption by the same identity returns the same grant only while
  that original membership is active. This recovers a lost response without
  allowing a second device to claim the invite.
- `reconnect({request})` returns the current active membership grant. Persisting
  the peer's private identity is necessary to reconnect as that member.
- `leave({request})` requires the member's signed request, including its current
  membership ID, and revokes that membership.
- `remove(peerPublicKey, membershipId)` is the privileged equivalent. Requiring
  the generation prevents a stale removal from revoking a later membership.
- Both operations permanently invalidate access through the consumed invite.
  A fresh invite creates a new membership ID. Old grants and old leave requests
  cannot affect that new membership.
- `verify({networkId, peerPublicKey, credential})` checks signature, scope, expiry,
  and authoritative current membership state. Merely verifying a grant signature
  with `createAdmissionVerifier` does **not** check revocation. Legacy v1 grants
  remain valid in the generic verifier but are rejected by this authority.

## Browser integration and live enforcement

```js
import {createMembershipAdmission} from '@phantom-local/core';

const admission = createMembershipAdmission({
  authorityPublicKey, // fixed trusted key; never accept a key asserted by a joiner
  authorize: obtainOrReconnectMembership,
  readRevocations: ({networkId, signal}) => fetchCurrentRevocations(networkId, signal),
  // Optional fast path; polling still recovers missed notifications.
  subscribeRevocations: ({networkId, onRevocation}) => subscribe(networkId, onRevocation),
  leave: ({request}) => submitSignedLeaveRequest(request),
});
const sdk = createBrowserPhantom({identity: savedPeerIdentity, admission, /* doorways, etc. */});
const network = await sdk.join(invite, {name: 'Device'});
await network.refreshMembership(); // explicit refresh, in addition to polling
await network.leave();             // signed self-revocation, then local shutdown
```

These callback names are application functions, not included backend endpoints.
`authorize` receives the existing admission context, including network ID, full
peer public key, operation, and abort signal. It must obtain a membership grant
from the authority, using a single-use token for first entry or a signed reconnect
request for an existing member. Creating a room requires bootstrapping the
creator's membership too. An invite containing only room coordinates is not a
membership credential.

`readRevocations` must return a current, complete array of authority-signed
revocations for the requested network over trusted transport. An old signed
snapshot is not evidence that no later revocations exist. Authenticate the
service/channel supplying the current snapshot. Polling defaults to five seconds;
current-state knowledge has a 15-second freshness lease. Snapshot requests time
out after the smaller of five seconds or the freshness lease and receive an
abort signal. Source failure cannot extend the lease. The authority's
`getRevocations()` and `onRevocation()` provide these callbacks when it is local;
remote transports carry the same signed records. Every record is independently
verified against the configured authority key and bound to a network, full peer
identity, and membership generation. Forged or cross-network records cannot
invalidate a member. Revocations are retained even if a later snapshot omits them.

Once a valid revocation reaches a peer, its cached admission is invalidated,
chat and SYS connections to that member are closed, pending direct transfers
from that peer are stopped or reassigned to other sources, and distribution jobs
and channels involving it are stopped. Incoming and outgoing frames check
membership independently of teardown, including asynchronous binary processing
and backpressure. Old channel callbacks and queued source reads cannot resume
under a later membership generation. Current members can continue through the
existing roster recovery logic; a fresh grant admits the previously removed
identity again.

The revoked device closes its own SDK network when it learns of the revocation.
Membership expiry and a stale authority freshness lease also close an opted-in
membership session. `membershipLost` is emitted before shutdown;
`membershipChanged` reports verified remote membership changes. Notifications
are fast when delivered, but this is not an instantaneous global cutoff: another
peer may continue accepting traffic until it receives the record or exhausts its
freshness lease. Browser timer suspension can delay cleanup; per-frame membership
checks also reject expired/stale access when execution resumes.

This live expiry lifecycle is opt-in through `createMembershipAdmission`.
Legacy admission policies are not automatically converted to membership policies.
Normal `network.close()` releases local resources without surrendering membership;
use `network.leave()` to revoke it. A failed leave submission does not claim that
membership was revoked. The browser SDK signs leave requests with the peer identity;
the authority must still validate the exact signed request.

Authority failover requires transactional state and private signer custody;
elected network leadership alone does not confer admin authority. No
code-to-invite lookup service is required by these primitives, but an available
membership authority is needed to issue/redeem grants and refresh current state.

Single-use claim tokens and compact room coordinates are distinct. A reusable
room seed alone cannot enforce single-use admission. Link, QR, and future pairing
codes must carry or securely establish the same invitation claim. Encoding the
claim for human entry is a separate change.
