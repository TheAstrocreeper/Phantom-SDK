# Network admission

Configure `admission` on `createBrowserPhantom` to restrict network membership. The application decides whether a login, purchase, allowlist, or admin approval qualifies. Phantom calls `authorize` before relay discovery, socket creation, announcement, or signaling. No credential means no connection attempt.

Every receiving peer calls `verify` on the credential before accepting its signed Nostr announcement or signal. Only verified identities reach the discovery roster. The core checks authorization again before staging new membership and handling incoming RTC signaling, including signaling forwarded through peers.

## Browser integration

```js
import { createBrowserPhantom } from '@phantom-local/doorways/browser';
import { createAdmissionVerifier } from '@phantom-local/core';

// Fixed trusted public key supplied by your application deployment.
// Never obtain the trust key from the requesting peer's credential.
const verify = createAdmissionVerifier({ authorityPublicKey: YOUR_AUTHORITY_PUBLIC_KEY });

const phantom = createBrowserPhantom({
  admission: {
    async authorize({ networkId, peerPublicKey, operation, signal }) {
      const response = await fetch('/api/phantom/admission', {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ networkId, peerPublicKey, operation }),
        signal,
      });
      if (!response.ok) throw new Error('Access denied');
      return response.json();
    },
    verify,
  },
});

const network = await phantom.join(invite, { name: 'Visitor' });
```

The same checkpoint runs for `create`, with `operation: 'create'`. A newly generated network ID is supplied before discovery. The authority must independently check whether the caller is allowed to create or join that particular network. Client-provided operation and network ID are requests, not proof of entitlement.

## Signing at your authority

An admin identity can issue a separate, domain-separated admission grant:

```js
// Trusted backend or admin application. Keep the private identity here.
const credential = await adminIdentity.signAdmission({
  networkId,
  peerPublicKey,
  expiresAt: Date.now() + 5 * 60_000,
});
```

Use `createAdminIdentity` / `importAdminIdentity` to provision or restore the signer. Admission grants do not authorize content publication, revocation, or retirement. The admission authority can be separate from the network's content admin.

The helper signs the exact network ID, full peer public key, and expiry. Nostr's verified envelope proves the sender owns the corresponding peer identity. Copying an invite or someone else's grant does not supply that private key.

## Custom policy

Instead of the signature helper, `verify({ networkId, peerPublicKey, credential, signal })` can call the developer's own trusted verification service. It must return exactly `true` to approve and must validate the credential against both supplied IDs and its expiry. Never approve based only on fields asserted by the requesting peer.

Credentials must be JSON data, at most 8,192 encoded characters, with an integer `expiresAt` in Unix milliseconds. Phantom rejects expired credentials even if the verifier approves. Credentials are included in signed relay messages: use public verification proofs, never passwords, signing secrets, or reusable login tokens.

Each hook has a five-second deadline, configurable with `authorizationTimeoutMs`. Missing credentials, verification errors, false/nonboolean responses, aborts, and timeouts fail closed. `refreshRelays()` is disabled for restricted instances; automatic discovery and recovery occur after authorization.

## Boundaries of this first implementation

- All participants must use the same trusted admission policy. Restricted admission is an application configuration, not a flag a joiner can remove from an invite.
- Restricted mode currently supports Nostr doorways only. WebSocket and custom adapters are rejected rather than silently allowing unverified admission. Open mode retains existing adapter support.
- Expiry blocks new admission and signaling; it does not disconnect already admitted peers or cancel established file transfers. This is an entry checkpoint, not a complete membership revocation system. There is no automatic credential renewal yet.
- Public relays may receive unauthorized messages from modified clients. Phantom rejects those messages before they affect its network; it cannot control the public relay.
- Checks apply to honest SDK participants. An authorized participant can redistribute downloaded files or run modified software that leaks them.

## Validation

`npm test` covers denial before discovery/socket activity, fail-closed configuration, scope/expiry, timeout/abort, real Ed25519 grant verification, signed peer identity binding, copied grants, incoming signaling rejection, and open-mode compatibility. These checks do not establish separate-device NAT behavior or production-scale performance.
