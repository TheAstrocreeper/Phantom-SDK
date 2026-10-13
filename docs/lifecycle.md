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

Adapters report `onStatus({type:"peer_available", verified:true, roomId, peerId, bootId, availabilityId})` only after verifying a fresh peer observation using their transport’s identity/authentication rules. `availabilityId` is an opaque, nonempty generation (at most 128 characters), not a Nostr event hash. Adapter authors must supply a new generation when peer reachability returns, deduplicate repeated observations, and make the return signaling route usable. A local socket opening alone does not verify remote availability. This optional hint rearms recovery; it never proves membership absence.

A signed Nostr announcement implements that contract with a provider-local availability generation. Returning peers receive one reciprocal presence announcement per generation so both signed signaling routes become available. The core consumes each generation once for the same committed incarnation. Only the existing `phantomSysBirthRole` opener can replace the stale attempt and begin another bounded attempt; acceptors remain acceptors. The receipt may be retained while setup is in flight so availability arriving during the second attempt is not lost. None of this grants admission, changes leadership, revives terminal membership, or bypasses authorization. SDP and SYS_CONTROL proof still establish connection readiness.

Inbound signaling is FIFO per peer pipe across PC replacement. Answer/ICE attempt IDs and boot ownership fence stale work. Its queue retains at most 64 tasks per pipe and 1,024 per core, checks capacity before RTC allocation, and releases its entries after success or failure. Availability receipts retain at most 128 members; Nostr route/availability maps admit at most 256 peers. Teardown cancels the owned SYS response deadline. Data-channel drain waits share one watchdog per channel, cancel it on drain/close/error, and do not claim success while the buffer remains full. Diagnostics expose queued work, pending availability and runtime-owned timer counts. These are bounds on these resources, not a claim that every SDK allocation has been audited.

The manual-clock integration tests advance 90 seconds without sleep: membership stays intact through discovery silence, an online PC exhausts its attempts before the offline phone returns, and a return during the second attempt also recovers. Each scenario verifies another file after recovery. A queue flood test checks rejection and cleanup of 2,000 tasks. These exercise the real SDK on simulated transports, not native phone/laptop NAT behavior.

A new RTC attempt ID within the same boot replaces only that pipe, not the peer incarnation. Late ICE/datachannel callbacks from retired PCs are ignored. ICE may arrive before an offer on any relay: a core inbox holds at most 64 candidates per peer pipe, 128 peer pipes, and 1,024 candidates in total. It retains only RTC candidate fields; candidate strings are capped at 4,096 characters and optional text fields at 256 characters. A validated offer consumes only matching boot/attempt candidates and clears the obsolete candidates for that pipe. The inbox owns no timers and is cleared on disposal. Unknown SYS attempt candidates are bounded hints until offer ownership is validated; they cannot open a pipe or change authority.

The recovery fixture now requires both peers to apply current-generation ICE before channels can open. A developer-defined in-memory doorway also reorders ICE ahead of offers and uses opaque availability generations, without Nostr or Cloudflare. This extends simulation coverage; native cross-device recovery remains to be verified.

An explicit, direct admission can arrive on a current SYS transport while the local channel still reports `connecting`. If all incarnation, authorization and doorway gates pass, that transport retains one bounded admission until its own `open` event. The core then reapplies every admission gate; replacing/closing the transport cannot move its request to another PC. There is no retry timer, and receipt alone does not grant membership.

Optional real transport check (werift 0.25.0; ICE, DTLS and SCTP over loopback):

```bash
npm install --prefix /tmp/phantom-real-rtc werift@0.25.0 --no-audit --no-fund
PHANTOM_REAL_RTC_ROOT=/tmp/phantom-real-rtc npm run test:rtc:loopback
```

The check first verifies a bare RTC connection independently, then verifies SDK admission and two-member mirror commitment through a developer doorway. It does not exercise external STUN/TURN, phone/laptop NAT, or public relay reachability. A deterministic delayed-open fixture reproduces admission arriving before local channel-open; the previous core times out, while deferred admission completes and a second transfer succeeds.
