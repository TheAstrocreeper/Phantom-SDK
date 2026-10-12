# Network admission

Configure `admission` on `createBrowserPhantom` to restrict network membership. The application decides whether a login, purchase, allowlist, or admin approval qualifies. Phantom calls `authorize` before relay discovery, socket creation, announcement, or signaling. No credential means no connection attempt.

A shared wrapper gates every adapter before `open(context)` and before outgoing announcement or signaling. Every receiving peer calls `verify` on the credential and checks a transport-independent Ed25519 proof before accepting an announcement or signal. Only verified identities reach the discovery roster. The core checks authorization again before staging new membership and handling incoming RTC signaling, including signaling forwarded through peers.

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

The helper signs the exact network ID, full peer public key, and expiry. The shared peer proof signs the announcement or exact signaling packet, proving the sender owns the corresponding peer identity independently of the relay type. Copying an invite or someone else's grant does not supply that private key.

## Custom policy

Instead of the signature helper, `verify({ networkId, peerPublicKey, credential, signal })` can call the developer's own trusted verification service. It must return exactly `true` to approve and must validate the credential against both supplied IDs and its expiry. Never approve based only on fields asserted by the requesting peer.

Credentials must be JSON data, at most 8,192 encoded characters, with an integer `expiresAt` in Unix milliseconds. Phantom rejects expired credentials even if the verifier approves. Credentials are included in signed peer proofs carried by relay messages: use public verification proofs, never passwords, signing secrets, or reusable login tokens.

Each hook has a five-second deadline, configurable with `authorizationTimeoutMs`. Missing credentials, verification errors, false/nonboolean responses, aborts, and timeouts fail closed. `refreshRelays()` is disabled for restricted instances; automatic discovery and recovery occur after authorization.

## Adapter and relay contract

The checkpoint is outside the adapter. Adapter factories run only after authorization, and the shared wrapper checks authorization again before opening a session. No Nostr-specific admission logic is required.

For restricted sessions, `open(context)` receives `context.authorization` with the locally verified `networkId`, `peerPublicKey`, and `credential`. A private-server adapter may use these to request an authenticated connection; this does not replace receiving-peer verification.

Adapters and servers must preserve:

- `admissionProof` on announcement metadata, including when announcement metadata becomes a discovery roster row.
- `admissionProof` on signaling packets, along with the exact signed packet fields.

The common wrapper signs both using the peer's Ed25519 key. Incoming discovery rows are checked against the signed peer ID, full public key, and boot ID. Incoming signaling is checked against its signed sender, network, recipient, and complete payload. Recent duplicate signaling proofs are rejected. The developer's verifier independently checks the credential's entitlement.

Nostr announcements carry the proof inside `row`. The legacy WebSocket adapter carries it on `concierge_connect`; its server must retain that field when constructing roster rows. It already forwards the proof on signaling without interpreting it. Custom typed adapters carry it through `announce`/`onDiscovery` and `sendSignal`/`onSignal`. Raw adapters carry the same fields through `send`/`onPacket`.

A server may additionally enforce authorization before upgrading a connection, but this SDK change does not modify a deployed CF worker or private server. The client checkpoint protects entry before traffic, and the shared receiving-peer checkpoint protects membership even when the relay itself does not authenticate users.

## Boundaries of this first implementation

- All participants must use the same trusted admission policy. Restricted admission is an application configuration, not a flag a joiner can remove from an invite.
- The common gate supports Nostr, WebSocket/CF-style, and custom typed or raw adapters. Relay wire compatibility still matters: a server that strips the proof fields cannot admit restricted peers. Missing proofs fail closed. Open mode retains existing behavior.
- Expiry blocks new admission and signaling; it does not disconnect already admitted peers or cancel established file transfers. This is an entry checkpoint, not a complete membership revocation system. There is no automatic credential renewal yet.
- Public relays may receive unauthorized messages from modified clients. Phantom rejects those messages before they affect its network; it cannot control the public relay.
- Checks apply to honest SDK participants. An authorized participant can redistribute downloaded files or run modified software that leaks them.

## Validation

`npm test` covers denial before discovery/socket activity, fail-closed configuration, scope/expiry, timeout/abort, real Ed25519 grant verification, signed peer identity binding, copied grants, tampered/replayed signaling rejection, denial before WebSocket/custom adapter construction, authorized private/custom adapter boot, and open-mode compatibility. These checks do not establish separate-device NAT behavior or production-scale performance.
