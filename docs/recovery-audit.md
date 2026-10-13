# Recovery audit: Canvas, original Breach, and current SDK

Audited branch: `feat/phantom-breach`, connection code frozen at `f87ff8c6ba95d22b0a80f04fa49c30298609300a`.

## What the source establishes

Both demos import `packages/phantom-doorways/src/browser-entry.js` by relative path and bundle workspace source with esbuild. Neither demo pins, vendors, or preserves a separate SDK. Canvas's application source has no changes between its published-source commit and the current branch. Rebuilding that unchanged app today still bundles the current SDK.

| Reference | Commit | Engine blob | Evidence and limits |
|---|---|---|---|
| Canvas published-source reference | `1a897e9` | `8f17620e10530613fcc11c6b9d5e153b8afeba14` | README records a user-run two-device channel/message check. Exact deployed bundle provenance has not been retrieved. |
| First Breach | `87af217` | `d9f5572261b861b8d251829f1af360e406933391` | Four SDK files changed from the Canvas reference: 18 additions, 2 removals. Engine change adds an application-requested discovery-disconnect latch. |
| Original diagnostic Breach | `e26f714` | `d9f5572261b861b8d251829f1af360e406933391` | Engine is byte-identical to first Breach. Entry, file service, browser wake wiring and core facade changed; therefore the whole SDK is not byte-identical. |
| Latest documented connecting build | `de523f1` | `09f19c8babaea148412bacaf559ae9308373471d` | Build `diagnostics-2026-10-13T04:51:44.204Z`; both supplied reports record a completed verified transfer. Network-switch recovery failed. |
| Current connection code | `f87ff8c` | See patches below | Latest native reports still fail initial establishment. Simulated and loopback successes do not establish native connectivity. |

The first Breach therefore used substantially the same connection engine as Canvas. Its early engine difference was intentional signaling-disconnect support, not a new recovery architecture. The source does not prove which exact SDK bundle remains deployed on the public Canvas site, or why an earlier phone network switch succeeded.

## Actual application wiring

Canvas's session imports the workspace SDK:

```js
import {createBrowserPhantom} from '../../../packages/phantom-doorways/src/browser-entry.js';
```

Canvas keeps an application `ready` set. `connect()` removes a ready peer only when it disappears from `network.getPeers()`. It does not clear the flag just because the data channel closes. Its one-second interval revisits invitations, and its four-second condition suppresses repeat invitations. This can leave an application readiness flag stale while membership remains present. It is not proof that the underlying SDK recovery was accidental; that requires observing core transitions during a switch.

First Breach used similar ready-set/invitation wiring and deliberately called `network.disconnectDiscovery()` during its signaling-cut ceremony. Later Breach removed that ceremony. Current Breach checks actual open chat channels when deciding whether to invite and reports channel loss/restoration separately from membership.

Both apps subscribe to actual SDK state/events. Neither app owns committed room authority, RTC attempt generations, or SYS opener/acceptor selection.

## SDK changes that need review

From the Canvas reference to current code, twelve SDK files changed. `network-engine.js` has 257 added and 79 removed lines; the architecture and authority machinery predate these patches. That is a real cumulative change, not evidence that it is harmless.

| Change | Location | Established evidence | Native status |
|---|---|---|---|
| Discovery silence cannot commit departure; exhausted committed peers remain recovering | engine replacement handling; `network-state.js`; composite presence capability | Manual-clock tests and native reports with two retained members | Membership retention observed; transport recovery still failed |
| Availability rearms only the established owner | engine SYS birth; Nostr reciprocal announcements; generic adapter contract | Asymmetric outage and developer-adapter simulations | Owner rearming observed in native reports; channel did not reopen |
| Peer incarnation separated from transport attempt | engine remote-incarnation handling | Same-boot attempt tests | Initial failure not isolated to this change |
| FIFO signaling across PC replacements, bounded early ICE | `signal-queue.js`, `candidate-inbox.js`, engine RTC handlers | Capacity/flood/ordering tests; latest native reports show applied matching candidates | No complete native channel establishment |
| Retired PC/channel callback guards | engine RTC/DC wiring | Recovery and delayed callback simulations | Requires native regression comparison |
| Admission received before local open is held on its exact transport | engine admission and SYS open handlers | Deterministic regression fails before fix; real ICE/DTLS/SCTP loopback reproduces and passes after fix | Latest native attempts do not reach channel open, so this is not their established cause |
| Shared/cancelled drain watchdog and owned response deadlines | `channel-drain.js`, engine SYS teardown | Watchdog/error/disposal tests | Does not establish physical route reachability |
| Opt-in bounded technical transport tracing | `rtc-diagnostics.js`, engine/facade/browser entry; Breach reports | Exact candidate/generation/error/stat exports tested | Latest reports contain useful traces |

## Mistake introduced during this work

The trace patch at `799e1a2` referenced undefined `meta` inside `phantomFlushInboundRtcIce`. `addIceCandidate` succeeded for the first queued candidate, then the logging call threw. The catch returned early and discarded the remaining queue. Existing connection fixtures could open after one candidate, so successful transfers did not expose the dropped remainder.

Native trace reports revealed `sys_rtc_ice_apply_failed:meta is not defined`. `f87ff8c` fixes the reference, continues after individual candidate failures, and exports queued/applied/failed counts. Regression assertions fail against the previous implementation; the multiple-candidate test requires the entire queue to be applied. All 180 tests and the production build pass. The subsequent native reports still fail, so this was a real introduced defect but not a complete explanation for the earlier failures.

## What the latest native evidence says

Both screens load `diagnostics-2026-10-13T06:31:22.040Z`. Applied candidate generations match the selected attempts; the undefined-metadata error is gone. PC candidate pairs send approximately 75–77 connectivity checks with zero requests/responses received. The phone's recorded pairs use the PC's public candidate. Both devices report the same public address; PC host candidates use `.local` names. No selected usable pair or open channel is established.

This narrows the investigation to actual candidate/route establishment, but does not prove a router, firewall, mDNS, SDK, or browser cause. No claim is made that the network environment caused the regression. An old untraced ZIP would not provide equivalent evidence.

## Reviewable code

- [Canvas reference to current SDK](recovery-review/canvas-to-current-sdk.patch)
- [Original diagnostic Breach to current SDK](recovery-review/original-breach-to-current-sdk.patch)
- [Latest documented connecting build to current SDK and demos](recovery-review/last-connected-to-current.patch)

These are complete Git diffs, not selected excerpts. They are fixed to `f87ff8c` so the review does not silently move with subsequent branch changes.

## Controlled comparison requirements

1. Preserve historical checkouts, not just unchanged demo folders. `1a897e9` represents Canvas's source reference; `e26f714` preserves the original Breach engine and both demos.
2. Use the same observation layer for both historical and current runs. It must observe RTC methods, candidate signaling and stats without importing current queue, admission, recovery or membership changes into the baseline.
3. Record exact commit, demo, build, and transport configuration on both devices. Compare initial admission first, then loss/return and application delivery.
4. Keep application readiness, core committed membership, SYS transport proof and file/canvas delivery as distinct observations.

A traced historical comparison has not yet been built or run. The current audit changes only review documents/patch artifacts; it makes no further SDK or demo connection changes.
