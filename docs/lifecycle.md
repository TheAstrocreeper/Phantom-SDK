# Browser lifecycle and retained admin controls

## Browser event forwarding

`createBrowserPhantom` enables forwarding of real browser `online`, `pageshow`, and `document.visibilitychange` events into the networking engine. Visibility state is copied from the browser document. The engine's existing deadline observers decide whether an owned attempt expired; these events do not elect a leader or mark a peer admitted.

Direct `createPhantom` users retain a headless default. Set `observeBrowserLifecycle: true` with a runtime exposing a browser window and document to opt in. The facade removes every event listener on disposal. Runtimes without those event surfaces do not need DOM shims.

Forwarding wake events does not guarantee background browser execution or connectivity. Applications may request wake locks separately while transferring files.

## Recovering members and isolated admin orders

The developer's `readAdminOrders` hook is used for leadership checks. It can also be called by an ordinary member that has previously reached admitted state and is now recovering:

```js
const phantom = createBrowserPhantom({
  adminPublicKey,
  readAdminOrders: async ({ networkId, peerId, reason }) => {
    // Fetch retained signed orders from the developer's control service.
    // reason === 'recovery' identifies an ordinary member's recovery read.
    return controlService.read(networkId, peerId);
  },
});
```

A successor can receive an isolated admin's retirement order before every surviving leaf has rebuilt its SYS control path. The recovery read allows a leaf to obtain the retained signed order independently. Leaders and historical mirrors continue to use the leadership gate; the recovery read does not approve duties, alter election rules, or write membership.

Only one recovery read is in flight per instance. A recovering leaf repeats the read after a one-second delay when the previous read finishes. The developer should bound or abort the service request itself; a hook that never settles remains the single in-flight read. Reads stop on admission recovery, retirement, or close. A response arriving after close or restored admission is discarded. Errors emit `controlError` with `via: 'recovery'` and never approve leadership.

Returned orders are independently verified against the invite's pinned admin key and network, persisted through the control store, and applied by the existing admin lifecycle. A forged order cannot retire a member. The hook must be readable by recovering members; a service that restricts all reads to the current leader cannot provide this fallback. No Cloudflare control endpoint or new relay-side API is added by this change.

The control service must retain revocation/retirement orders independently of live peers. If no service is configured or it is unavailable, delivery still depends on surviving peer paths. This fallback does not guarantee delivery without a reachable authority store.

## Transport loss during network changes

A SYS deadline ends its bounded connection attempt. For a committed member it projects `recovering` with `wait_for_peer_availability`, rather than an admission failure or a membership exit. The temporary 60-second `transportRecoveryGraceMs` option has been removed. Discovery-hint transports cannot infer departure from silence or cached roster absence. The existing authority exit/mirror-acceptance transaction remains the only path to committed removal; an authoritative live-socket absence observation can corroborate loss, while discovery hints cannot. Unannounced failures on Nostr-only networks may remain unresolved without departure evidence. This is preferable to committing contradictory solo membership during a partition.

A signed Nostr announcement carries a provider-local availability generation. Returning peers receive one reciprocal presence announcement per generation so both signed signaling routes become available. The core consumes each generation once for the same committed incarnation. Only the existing `phantomSysBirthRole` opener can replace the stale attempt and begin another bounded attempt; acceptors remain acceptors. The receipt may be retained while setup is in flight so availability arriving during the second attempt is not lost. None of this grants admission, changes leadership, revives terminal membership, or bypasses authorization. SDP and SYS_CONTROL proof still establish connection readiness.

Inbound signaling is FIFO per peer pipe across PC replacement. Answer/ICE attempt IDs and boot ownership fence stale work. Its queue retains at most 64 tasks per pipe and 1,024 per core, checks capacity before RTC allocation, and releases its entries after success or failure. Availability receipts retain at most 128 members; Nostr route/availability maps admit at most 256 peers. Teardown cancels the owned SYS response deadline. Data-channel drain waits share one watchdog per channel, cancel it on drain/close/error, and do not claim success while the buffer remains full. Diagnostics expose queued work, pending availability and runtime-owned timer counts. These are bounds on these resources, not a claim that every SDK allocation has been audited.

The manual-clock integration tests advance 90 seconds without sleep: membership stays intact through discovery silence, an online PC exhausts its attempts before the offline phone returns, and a return during the second attempt also recovers. Each scenario verifies another file after recovery. A queue flood test checks rejection and cleanup of 2,000 tasks. These exercise the real SDK on simulated transports, not native phone/laptop NAT behavior.
