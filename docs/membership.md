# Membership authority (optional SDK primitive)

`createMembershipAuthority` adds single-use invitation accounting and revocable,
identity-bound membership. It is separate from content publication/revocation and
network retirement, but uses the same admin signing identity. Existing open
networks, v1 admission grants, and invite formats remain compatible.

This is the authority primitive, not a turnkey browser membership protocol.
Share integration, authority request transport, active-connection teardown,
revocation propagation, and compact pairing codes are not implemented here.
Do not describe this addition alone as enforcing removal on established sessions.

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

## Integration boundary

The existing admission policy can obtain a grant with `authorize` and call the
authority's `verify` method in its `verify` callback, through authenticated
application-owned transport if the authority is remote. No code-to-invite lookup
service is required by these primitives. They do require an available membership
authority to issue/redeem grants and check current state.

Existing admission checkpoints cache approved identities, and established RTC
channels outlive an admission check. Therefore wiring `authority.verify` into
admission protects new checks but is insufficient for immediate Remove/Leave
on live channels. A complete integration must distribute trustworthy membership
changes, invalidate admission caches, stop transfers, and close revoked peers'
existing channels. It must fail closed when current membership cannot be
established. Authority failover requires transactional state and private signer
custody; elected network leadership alone does not confer admin authority.

Single-use claim tokens and compact room coordinates are distinct. A reusable
room seed alone cannot enforce single-use admission. Link, QR, and future pairing
codes must carry or securely establish the same invitation claim. Encoding the
claim for human entry is a separate change.
