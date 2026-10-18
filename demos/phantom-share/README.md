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
