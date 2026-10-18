# Phantom Share

The approved single-screen sharing interface, using the workspace SDK. Direct
changes the target label; Exclude dims the peer. File progress and Save are based
on verified transfers, not simulated activity.

## Build and run

```sh
# repository root
npm ci
cd demos/phantom-share
npm ci
npm test
npm run build
```

Deploy `dist/` to your existing HTTPS static host. For local desktop checks, serve
it on localhost. Phone access through an ordinary HTTP LAN address lacks the
secure context required by Web Crypto. Nothing in this change deploys a site or
changes an existing service.

## Pairing

Create a session and share its QR, link, or pairing code. A code has 28 letters /
numbers grouped into blocks, plus the `P1` version prefix. It contains 128 random
bits and a typing checksum. Code entry is case-insensitive and tolerates spaces,
hyphens, and commonly confused characters. A TV or other camera-less device can
open the app and type the same code.

The app reconstructs temporary bootstrap coordinates from the code and uses the
SDK's existing Nostr signaling transport to exchange the actual invitation and
identity-bound membership grant. Bootstrap messages are AES-GCM encrypted with a
separate key derived from the code. There is no separate code-to-invite lookup
backend. This is an online exchange with the inviting device, not offline LAN
discovery or full invitation data compressed into a few characters.

Each invitation admits one device. Retrying with that same signing identity is
idempotent while its original membership remains active. Another device cannot
reuse the code. The invitation expires after five minutes if unused. Each member
can request a fresh invitation from the connected owner. The modal replaces a
consumed code with a new one; link, QR, and displayed code always represent the
same invitation. Older room-only invitations are intentionally rejected by this
app because they do not carry single-use enrollment.

## Membership

The creator holds the admin signing key and membership authority. Its state is
committed atomically to IndexedDB; private admin keys stay in the creator's tab.
The creator's own membership is granted before it enters the network. Other
devices prove ownership of their saved peer identity to redeem an invitation.

The owner can Remove a device. Leave surrenders the member's own access. Both
invalidate its old membership and consumed invitation. A new invitation creates a
fresh membership generation. SDK revocation enforcement closes live chat, SYS,
and distribution channels and stops or reassigns affected transfers. Removing
someone cannot erase files they already saved.

The owner tab must remain open to serve admission, current revocations, and new
publication authorization. Signed notifications provide prompt revocation;
five-second polling recovers missed notifications. A 15-second freshness lease
fails closed if current authority state cannot be confirmed. Normal network
outages can therefore end this app's session when the authority is unreachable
long enough. This is intentional membership enforcement, not a promise of an
instantaneous global cutoff or offline operation.

Names and private peer identities persist locally. Web Locks prevent accidental
concurrent use of the same identity where supported. Files and owner private keys
remain in memory for this session; owner reload/closure does not automatically
restore its authority service. Save files before leaving. Reopening a guest's
original link can reconnect the same active identity while the owner remains open.
Fresh invitations are required after Leave/Remove. At most 16 enrollment/control
channels remain open per owner; unused codes expire, and revoked-member channels
are released after their final notification window.

## Validation

`npm test` here covers recipient selection, invitation input routing, durable
IndexedDB concurrency/recreation, and DOM references. Repository tests cover code
round-trips, authenticated encryption, single-use enrollment, revoked old-code
rejection, fresh enrollment, membership state, and live SDK enforcement.

`node test/browser-smoke.mjs` runs a browser flow when Playwright and Chromium are
available. Set `PHANTOM_CHROMIUM_EXECUTABLE` for a custom executable. The script
routes Nostr WebSockets through a local relay fixture, while the actual browser
SDK, Web Crypto, IndexedDB, and RTC perform enrollment and transfer. It checks a
saved file's SHA-256, removal, old-code rejection, reinvitation, Leave, and desktop /
mobile layouts. Set `PHANTOM_BROWSER_UI_ONLY=1` to check browser enrollment and layout when the
execution environment cannot establish native RTC. In this environment, UI-only
browser enrollment/layout passed; the full native RTC check stalled, and the
original first build showed the same stall. Complete pairing/admission/owner
authorization/verified distribution passed through deterministic SDK RTC fixtures.
These checks do not establish public-relay availability or separate-device NAT
behavior.

## Remaining field checks

Test the packaged build on your actual PC, phone, and Google box. Confirm the
box's browser supports Web Crypto, WebRTC data channels, file saving, and the UI
controls with its remote. No TURN service or offline LAN bootstrap is added.

## Test logs

Logging starts automatically at page load. Use **Download logs** in the footer on each device after a test or failure. Downloads remain available when disconnected. The last 1,200 diagnostic rows survive reloads in the same tab when session storage is available; progress is sampled once per second per transfer phase. Exports include a build marker, timestamps, preparation read/hash timing, transfer phases/retries, connection and membership events, browser visibility/online events, and errors. Logs omit file names, file contents, invitation codes and links, keys, proofs, and raw signaling. Each download covers that device only; export both sides. A storage failure is recorded in the report and logging continues in memory. Browser backgrounding is an observation, not a diagnosed failure cause.

Membership checks now prefer the established authenticated peer channel to the owner, with pairing signaling as fallback. Revocation and Leave use the peer path too. Genuine loss of every authority path still expires freshness; losing pairing signaling alone should not close an otherwise healthy owner/member session. Distribution reuses its RTC connection for successive verified batches when both endpoints support it. Logs include control-path selection, batch reuse, distribution preparation/progress and native RTC state fields. Asset hashes are represented by a short public correlation label; invitation secrets remain omitted.

Optional native loopback regression: `node test/native-distribution.mjs` (Playwright installed, or `CODEX_PRIMARY_RUNTIME_NODE_MODULES` set). `PHANTOM_CHROMIUM_EXECUTABLE` can select a Chromium executable. It compares 32 MiB transfers with reuse off/on, verifies full-file SHA-256 and asserts 64 versus one receiver connection. This is single-host browser RTC, not mobile or NAT throughput validation.

Current local validation: 194 SDK tests and six Share tests passed, including an established SDK session with doorway signaling sends disabled for more than 15 seconds, verified publication, and subsequent owner removal over the peer channel. Deterministic RTC confirms one connection across successive batches and mixed-capability fallback. Browser enrollment/layout and disconnected log export pass through a local fixture. The optional native loopback benchmark timed out before its first data channel opened, even with reuse disabled; no native speed result or phone improvement is claimed. Physical-device logs are still needed to establish whether the original peer interruption recurs.

## Session cleanup

An empty roster displays “No devices connected” rather than an endless recovery message, with Leave / reset available. A missing peer alone does not permanently retire the SDK room; an owner can invite a replacement or explicitly reset. Leave attempts authority confirmation but always closes the local session within two seconds, even if the authority rejects or never replies. Unconfirmed Leave is logged and identified in the status. Terminal failed, retired or closed states reset the app to Connect. Reset releases connections, the tab identity lock, pending connection work, invitation state, recipient choices and file references; diagnostic history remains available. The drop zone is grey, disabled for click/keyboard/drop, and its file input is disabled whenever selected recipients are not all ready. This update does not change transfer throughput.

Validation: all 194 SDK tests and seven Share tests pass. The native-browser fixture checks a disabled empty mobile drop zone, local Leave/reset, creation of a fresh session afterward, and disconnected log export. The invitation dialog no longer floats while users enter or select connection controls.

## Connection ramp test

Choose **Connection test** in the footer after connecting two devices and completing file transfers. Choose a peer, optionally describe the conditions, then Start test and accept on the other device. Standard sweeps 256 KiB through 16 MiB and back down in each direction; Extended reaches 64 MiB. The test counts received bytes, RTT, send buffering and credit stalls; it does not save received generated data. Standard can use up to 3.5 GiB and Extended up to 18 GiB of traffic. Keep both pages visible; Stop, page hiding, session cleanup or loss of the test connection cancels it. Download results after completion; the latest completed report survives same-tab reload when session storage works. This does not change normal SDK file-transfer defaults.

This is a separate raw RTC connection, not the existing file channel. Exports record both browsers and selected candidate types/protocol to help detect different paths. The candidate operating range is provisional and may be inconclusive. See `tools/transfer-ramp/README.md` in the source repository for the repeatable device/browser/network matrix and report comparison command. Controlled tests verify credit bounds, receiver byte accounting, up/down stages, reverse-direction negotiation, cancellation and latency-limited behavior. Browser checks cover the panel and disabled start while disconnected; native throughput must be measured on actual devices.


### Batch scheduling experiment (v5)

Share assigns 32 chunks (2 MiB at its 64 KiB chunk size) per distribution scheduling turn instead of eight. The request window stays at eight chunks (512 KiB), and SDK defaults remain unchanged. This reduces leader continuation/ready exchanges without increasing outstanding chunk requests. A larger scheduling turn also delays redistributing newly acquired pieces to another recipient until that turn completes. Reload every participant onto this build before comparing. Receiver logs now include `distributionBatchStarted` and `distributionBatchComplete` with batch number, chunk count and elapsed time; compare inter-batch gaps with transfer time. The raw ramp tester remains available and unchanged. Physical-device speed improvement is not yet established.


### Bounded transfer pipeline (v6)

The SDK prepares requested chunks ahead of ordered dispatch: up to eight concurrent read/hash operations, 16 admitted preparations per peer and 64 per file service. It does not speculatively read unrequested pieces. Metadata and raw binary remain an adjacent ordered pair. Receiver request credit is released after integrity verification, independently of persistence. A service-wide 32-slot bound covers outstanding requests, verification and queued/active writes; at most four writes run concurrently. Slow storage eventually backpressures the network at that bound. Batch completion, inventory publication and final success still require successful writes and cache verification. Cancellation, membership loss and revocation fence queued work and late progress. Already-started storage calls cannot be undone.

`distributionTransferTiming` logs contain stage count, sum, maximum, p50 and p95 (up to 256 samples per stage). Sender groups cover up to 32 submissions, with remainders flushed on distribution-channel closure; receiver groups cover each batch plus final cache assembly. Stages include `readMs`, `hashMs`, preparation `queueMs`, `submitMs`, native distribution `bufferWaitMs`, receiver `requestToReceiveMs`, `writeQueueMs`, `writeMs`, and completion `cache_readMs`/`cache_hashMs`. Request-to-receipt includes sender queue/read/hash and transport delays; it is not one-way wire time. Submission ends at local channel acceptance, not remote delivery. Concurrent stage sums overlap and must not be added as wall time. The final full-file cache assembly can exceed 256 samples; percentiles then use the first 256, while count/sum/max cover all observations. Progress now accumulates persisted unique chunks across scheduling batches.

Deploy to both devices and repeat the same normal video transfer in each direction, then export Download logs. No physical-device speed gain is claimed before that comparison.

### Direct/Breach-path comparison

Run `node demos/phantom-share/test/transfer-path-comparison.mjs` from the repository root. It sends the same 32 MiB File in both directions through the current SDK file service's direct API path and the real Share session's prepare/authorize/distribute/receipt flow. Direct preparation includes Breach's additional whole-file hash. Each received file is compared with the source SHA-256; Share progress must reach 512 unique chunks monotonically. The transport and admission/control delivery are deterministic fixtures, not native RTC or the full network engine. This verifies the file/session logic but cannot establish native throughput or reproduce device/browser conditions.

An observed local run completed all four flows, with Share preparation at 73–80 ms. The separate native distribution browser probe timed out before opening a data channel (both peers had local/remote descriptions but remained in new ICE/connection state). No physical-device speed comparison, native speed result or reproduction of the reported preparation stumble is claimed.


### Preparation UI regression (v7)

`PHANTOM_CHROMIUM_EXECUTABLE=/path/to/chromium node demos/phantom-share/test/browser-preparation.mjs` exercises the real application DOM and Share session with SDK file preparation, a controlled network facade and deliberately gated preparation. It verifies that selecting a 32 MiB file creates a preparation row immediately, progress remains visible across connection events, preparation publishes once, read failures retain a visible failed row and re-enable the picker, and leaving fences late status/publication. Set `PHANTOM_PREPARATION_BASELINE=5e26b4c` to load the pre-fix app/session from git; that revision fails the first assertion with “No transfers yet.” This is a browser UI regression test, not an RTC performance test. Receiver bars use persisted per-chunk progress rather than waiting for completed inventory batches.

The native probe in this runtime completed ICE gathering with zero candidates on both endpoints, even with local-network permission granted. Node network-interface enumeration also failed with `uv_interface_addresses` errno 1. These observations constrain native testing here; they do not diagnose the user's device or prove the source of its throughput loss. Native-distribution timeout diagnostics now expose gathering state and candidate count.


### Connection-test approval and cancellation (v8)

Incoming tests now request consent inside the test panel with Accept test / Decline controls; no blocking `confirm()` is used. A canceled, expired or closed pending offer aborts and removes its approval controls, and late acceptance cannot create an orphan RTC connection. Stop messages carry bounded human-readable reasons to the other device; a delayed stop notice can refine a prior generic connection-loss result. Failed setup retains an incomplete report even before any stage completes. Start, approval request/result, sanitized connection/gathering states, candidate counts and stop reasons enter Download logs; raw SDP/candidates are excluded. Hiding the page still cancels explicitly to avoid recording throttled benchmark results.

The pending-approval cancellation regression fails against the prior controller (two RTC objects instead of one) and passes with this fix. Both initiation directions complete after delayed approval in the deterministic RTC fixture. The browser UI test accepts a remote SDP offer, verifies that no native dialog opens, confirms the answer is sent, cancels a different offer before acceptance, and checks that Download logs retains the exact peer stop reason. Native throughput remains unvalidated in this runtime. These tests demonstrate an approval/cancellation race and its correction; they do not establish that the old native popup caused the uploaded run's failure.
