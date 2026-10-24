<div align="center">

# PHANTOM SDK
### Resilient peer-to-peer networking for browser applications.

[![Status](https://img.shields.io/badge/swarm-experimental-success?style=for-the-badge&color=0a0a0c&logo=git)](https://github.com)
[![Build](https://img.shields.io/badge/stress__test-passed-8b0000?style=for-the-badge)](https://github.com)

</div>

> *Resilient WebRTC peer-to-peer data swarms with automated failure recovery, leader rotation, and seamless channel state reconciliation.*

**[Explore the demos →](demos/README.md)**

Phantom turns browsers into a peer-powered network, giving developers a foundation for applications that run on the strength of their users.

This is the current development base. Its API is still evolving.

See [Device validation and integration notes](docs/device-validation.md) for the reusable lifecycle, pairing and connectivity fixes exercised on Linux and Android, and the remaining limits.

## Getting started

Use Node.js 22 or later to install the workspace dependencies:

```sh
npm install
```

Import the browser entry in your application with a bundler:

```js
import { createBrowserPhantom } from '@phantom-local/doorways/browser';

const phantom = createBrowserPhantom();
const network = await phantom.create({ name: 'Alice' });
const invite = network.invite;

// In a separate browser client:
// const network = await phantom.join(invite, { name: 'Bob' });
```

The default entry uses Nostr relay discovery. Developers can configure other doorways. The application owns its interface and how it uses the network.

## Connection configuration

Pass `rtcConfiguration` to `createBrowserPhantom` to configure system, messaging, and distribution peer connections:

```js
const phantom = createBrowserPhantom({
  rtcConfiguration: {
    iceServers: [{
      urls: ['turn:YOUR_TURN_HOST:3478?transport=tcp'],
      username: YOUR_TURN_USERNAME,
      credential: YOUR_TURN_CREDENTIAL,
    }],
  },
  distribution: {},
});
```

Supply the configuration locally on each browser instance; it is not carried in invites or signaling packets, or included in SDK diagnostic snapshots. Keep credentials in local configuration rather than doorway descriptors or application logs. The browser validates the RTC configuration when constructing each connection. This option does not provision a TURN service or establish that it is reachable.

When omitted, system, messaging and distribution connections use `stun:stun.l.google.com:19302` by default. A supplied `distribution.rtcConfiguration` overrides the distribution default. An explicit top-level configuration takes precedence for all three connection types, including an explicit `{}` with no configured ICE servers. Existing distribution-only configuration remains supported when the top-level option is omitted. Lower-level `createPhantom` callers can also supply `rtcConfiguration` for system and messaging connections.

A caller-requested admission deadline rejects with `error.code === "ADMISSION_TIMEOUT"` and the existing `Network admission timed out` message. `error.diagnostics` retains `{state, rtc}` captured before cleanup; `rtc` is the existing bounded native RTC history (or `null` when unavailable). Its connection events include browser ICE error codes, text and server URLs when reported. A timeout alone does not identify a TURN, signaling or authority failure. Diagnostics can contain network addresses; review before sharing. No RTC credentials are added.

## Messaging lifecycle

Messaging remains manual by default: applications call `invitePeer` and `acceptPeer`. Applications that want messaging connections between all admitted members can explicitly opt in:

```js
const phantom = createBrowserPhantom({messaging: 'automatic'});
const network = await phantom.join(invite, {name: 'Alice'});

network.on('peerConnection', ({peerId, state, reason, attemptId}) => {
  if (state === 'ready') {
    // Start your application's synchronization or send messages here.
  }
});

// Check this after subscribing: a connection may already be ready at join time.
const connections = network.getPeerConnections();
```

Automatic mode initiates and accepts messaging relationships only for eligible committed members. Pair ownership, native attempt deadlines, bounded successor attempts, and transport recovery stay in the engine; applications do not need invitation polling. Opt in on each participating instance. It does not override blocks, ended relationships, or admission rules, and it does not guarantee successful connectivity.

`getPeerConnections()` returns current `{peerId, state, reason, attemptId}` rows. States are `idle`, `connecting`, `ready`, `recovering`, `failed`, `blocked`, or `closed`. `ready` requires an accepted relationship, an open channel, and the peer hello proof. A `peerConnection` event reports changes, including loss of readiness and a new ready attempt after recovery; it also reports `closed` when a peer leaves the committed roster. Subscribe and then inspect current rows so early events are not missed. Closing the network clears its connection rows. Low-level core consumers can use `getPeerConnections()` and `PEER_CONNECTION_STATE` events.

The SDK does not acknowledge application message delivery, replay application history, resolve conflicting edits, or retry indefinitely. A `failed` state can follow exhaustion of its existing bounded native attempts. Application history and synchronization remain application-owned. The canvas source demonstrates this split without an application invitation timer or a stored readiness set.

## Distribution recipients

Automatic distribution continues to target every committed member when `recipients` is omitted. To select recipients for a publication:

```js
const manifest = await network.distributeFile(file, {
  recipients: [peerA, peerB], // Full 16-character lowercase peer IDs.
});
```

File preparation reads batches of at most 4 MiB and hashes at most eight chunks concurrently. File requests use a bounded per-source window (default 8, configurable 1–8) with timeout retries (default two attempts per source, configurable 1–3). Configure `requestWindow` and `maxAttemptsPerPeer` under `files` for standalone transfers or `distribution` for distributed transfers. Cached pieces remain verified before resume and reconstruction; membership and revocation checks still govern queued work. These changes do not fix background-page session recovery or establish mobile throughput.

The creator retains its original copy even if it is not a selected recipient. The scheduler assigns transfers only to selected peers that are currently in the roster, while any live holder of verified chunks can supply them. The signed recipient policy travels with inventories and mirrored distribution state, including across leader replacement. Both transfer endpoints verify it before opening an assigned job. An explicit `[]` keeps the original without scheduling replicas. Lists are limited to 128 entries, sorted and deduplicated.

Recipient selection is immutable for a file/distribution identity. To publish the same bytes with a different selection, provide a different `distributionId`. Snapshots expose the selection through each asset's `proof.recipients`; absence means all members. When supplying external publication authorization, sign the recipients too:

```js
const preparedManifest = await network.prepareDistributionFile(file);
const authorization = await adminIdentity.sign({
  networkId: network.networkId,
  action: 'content.publish',
  target: preparedManifest.id,
  operationId: crypto.randomUUID(),
  recipients: [peerA, peerB],
});
await network.distributeFile(file, {prepared: preparedManifest, authorization, recipients: [peerA, peerB]});
```

Pass file settings, including `distributionId`, to `prepareDistributionFile`. Reuse its exact returned object and original file in the same network session through `prepared`; do not pass file settings again. Distribution reuses the internally stored manifest without another hashing pass, and still verifies publication authorization and revocation. If `recipients` is omitted alongside an external authorization, the signed authorization defines the selection. The signer and selected-recipient options must agree when both are supplied.

Targeted publication uses version 2 admin proofs, with the recipient list included in the signed statement. Old version 1 publications remain unchanged. Older SDKs cannot verify targeted proofs; targeted sharing requires upgraded participants, with no fallback to all-member replication. Recipient selection controls SDK distribution; it is not encryption or a guarantee that a recipient cannot copy or redistribute downloaded content. For optional persistence and reload restoration, see the following section.

## Restoring distributed files

Distribution can restore local copies and partial transfers from an application-supplied durable chunk store. Extend the existing store with metadata operations:

| Method | Contract |
| --- | --- |
| `get(id, index)` | Return the stored chunk as an `ArrayBuffer`, or no value. |
| `put(id, index, bytes)` | Retain the bytes before resolving. |
| `load(networkId)` | Return up to 32 `{manifest, proof}` records for this network. |
| `save(networkId, record)` | Atomically upsert the full manifest and signed publication proof by manifest ID before resolving. |

Both metadata methods are required together. Stores with only `get`/`put` remain supported and do not restore manifests automatically. The default store remains memory-only; this change does not provide an IndexedDB implementation. Applications supply their durable backend and retain their invite.

For the same peer identity to survive reloads, explicitly create and retain a browser identity seed separately from file metadata:

```js
import {createBrowserIdentity, createBrowserPhantom} from '@phantom-local/doorways/browser';

// These stores belong to your application.
const identity = (await identityStore.load()) ?? createBrowserIdentity();
await identityStore.save(identity);
const phantom = createBrowserPhantom({
  identity,
  distribution: {chunkStore: durableChunkStore},
});
const network = await phantom.join(savedInvite, {name: 'Alice'});
```

The identity seed contains private peer-signing material. The SDK does not save it automatically or include it in publication metadata, invites, or diagnostics. Keep it under your application's custody. Omitting `identity` still creates a fresh identity; a new identity does not inherit an old peer's targeted recipient authorization. Use a different identity for each concurrently active peer; reuse a saved identity when replacing that peer. This seed is independent of the content administrator's signing key, which is not needed to restore or serve an already authorized publication.

Startup verifies publication signatures, network binding, manifest identity, hash lists, and every retained chunk. It reconstructs local inventory from verified bytes rather than saved counters, then lets the current leader assign missing pieces. Stored jobs, connections, peer presence, and leadership are not restored. The application must rejoin, pass current admission checks, and obtain any current admin orders through its normal configuration. A caller-supplied startup timeout also covers restoration work.

Receivers save verified manifest metadata before requesting chunks. Publishers with persistent stores retain their original chunks and metadata before advertising the copy. Interrupted writes may leave unreferenced cached bytes, but they are not advertised as a restored publication. Invalid or locally revoked records are skipped; backend load failures reject startup. Restoration does not establish that a cached admin view includes orders issued while the application was offline.

After startup, inspect `network.getDistributionState().restoration` and `.restoreResults` for restored or rejected records. Later restores also emit `distributionRestored` or `distributionRestoreRejected`. Use `await network.getDistributedFile(id)` to read a complete local copy as a verified `Blob`; it does not download missing chunks. Partial transfers resume through the existing scheduler and re-request missing or corrupt chunks. UI listeners must account for copies already restored before `join` returns rather than waiting only for a future `distributionComplete` event.

This contract currently covers automatic distribution. Standalone `publishFile`/`requestFile` sessions do not automatically restore their manifests.

## Restricted networks

Applications can configure an admission checkpoint before relay discovery and peer admission. The developer owns the access rules; Phantom enforces credential verification and expiry. See [authorization integration](docs/admission.md). Without an `admission` configuration, networks retain open admission.

Run the focused checks with `npm test`.

## Source

- `packages/phantom-core`: networking engine, membership, peer roles, file distribution and admin controls.
- `packages/phantom-doorways`: browser entry and discovery adapters.

This repository contains SDK source, integration tests, runnable demos, and a relay template. Generated bundles and test reports are excluded. Package names are local workspace names; the SDK is not published on npm.

## Relay template

A [Cloudflare doorway template](templates/cloudflare-doorway/README.md) provides proof-preserving WebSocket discovery/signaling with hibernation support and deployment instructions.

## License

Free for the noncommercial purposes permitted by [PolyForm Noncommercial 1.0.0](LICENSE). Uses outside those permissions require a separate paid commercial agreement. See [commercial licensing](COMMERCIAL.md). Preserve the required notices in NOTICE when sharing the software.

Third-party dependencies retain their own licenses; see THIRD_PARTY_NOTICES.md.




[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/theastrocreeper)

### Membership over existing peer connections

After enrollment, `createMembershipPairingHost(...).bindNetwork(network)` and `connectMembershipPairing(...).bindNetwork(network)` route membership status, invitation requests, Leave and signed revocation notifications over authenticated SDK messaging. Bind the matching room and local identity on both sides. Status replies are matched to a fresh request ID and the pinned inviter peer; host requests still pass the original signed membership checks. Pairing signaling remains the bootstrap and fallback path. This removes the requirement for an available pairing relay while the owner peer channel is ready. Membership still fails closed if no path can confirm the authority within its existing freshness lease; this does not guarantee recovery after loss of the peer transport itself.

Distribution negotiates connection reuse through `reuseProtocol: 1`. Verified batches continue on the same RTC connection, with bounded index lists, per-batch deadlines, generation checks and renewed source readiness. A source change, leader change, failure, completion, removal or revocation tears it down normally. Older peers retain batch teardown; `distribution: {reuseConnections: false}` disables reuse. Connections remain scoped to one asset, source and receiver.

### Local connections

Experimental local doorway transport, authenticated LAN/PIN bootstrap, and browser
pairing build helpers are available through dedicated package exports. Applications
own hosting and discovery. See [Local connections](docs/local-connections.md) for
contracts, examples, and the remaining device validation limits.

### Large files and storage

There is no built-in 1 GiB file-size or 32-asset session cap. `maxFileBytes` remains an optional application policy (default: JavaScript's maximum safe integer); practical capacity depends on storage, memory, browser and device. This is not an unlimited-size guarantee. Preparation reads at most 4 MiB at a time, and file data uses chunks up to 64 KiB. Hash and inventory metadata still grow with the number of chunks.

The default chunk store is in memory. Applications receiving large files should supply a disk-backed `chunkStore` with `get(id,index)` and `put(id,index,bytes)`. An optional `getFile(id,manifest)` returns a Blob backed by the same verified, immutable stored chunks, avoiding collection of all chunk ArrayBuffers during final assembly. The SDK verifies chunks before invoking it and checks the resulting Blob size. Blob construction must not read the full file into an ArrayBuffer. Partial requests verify their requested chunks without assembling a file. `load` and `save`, when implemented together, opt into persistent session restoration; `getFile` alone does not copy outgoing source files into the store.
