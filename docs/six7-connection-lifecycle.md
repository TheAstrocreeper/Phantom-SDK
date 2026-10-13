# SIX7 connection lifecycle comparison

Reference: user-provided SIX7_v1.0.9/six7_core.js. Phantom source before diagnostics: cd7d7763bb5e2b739c003715914cacd6a1e0d639. Names were normalized for comparison; the native system channel remains six7-sys.

Reviewed: RTC configuration, creation, ownership, offer/answer/candidate queues, relay fallback, system admission, data-channel wiring, replacement, retirement and close paths. This is source comparison, not real-device connectivity validation.

## Matching paths

Both configure stun:stun.l.google.com:19302 without TURN. Candidate signal deduplication, outgoing ICE fallback queues, incoming candidate buffering/application, queue flushing and answer application match after naming normalization. Common chat/SYS/all-edge teardown and SYS admission target/presence scheduling match before diagnostics. Matching source does not establish matching runtime inputs or lifecycle.

## Differences

- Phantom's browser native-entry wrapper holds the advertised second peer as an ordinary candidate until the leader offer; it is outside SIX7. The conflicting startup offer was reproduced and corrected in da7b850.
- Phantom added incarnation/attempt replacement while accepting SYS offers. Replacing inside a captured old queue discarded following queued candidates. The reproduction fails before cd7d776 and passes afterward; replacement and non-owner guards are tested.
- Phantom serializes outgoing SYS births per peer and waits for outstanding birth work before inbound signaling. SIX7 has no sdkSysBirthFlights layer.
- Phantom retains an already connecting channel with applied descriptions and checks asynchronous completion still belongs to the current connection. SIX7's older preflight can close/recreate that connecting channel.
- Phantom adds an initial SYS response deadline: 10 seconds, one replacement, then admission failure at the second deadline. This exact initial-birth deadline/budget is absent from SIX7. SIX7 does have other 10-second replacement and temporary-transaction deadlines.
- Phantom adds admission permission checks, historical successor handshake validation, and authority/recovery behavior. These gates cannot be removed simply to copy older code.

## Evidence and limits

The cd7d776 device reports show phone candidates received/applied 6/6, PC 4/4, completed answers and no open peer channel. PC retires the initial birth at its 10-second deadline; phone later reports native PC failure. Those counters do not reveal tested candidate pairs, STUN responses, DTLS state, or whether a channel would have opened absent the deadline.

This diagnostics change leaves connection policy unchanged. It records native state and invokes getStats before every SDK PC close path, exposing the added deadline as a retirement reason.

RTC_DIAGNOSTICS events and snapshot.rtcDiagnostics retain native connection ID, peer/pipe/attempt, PC/ICE/gathering/signaling/SCTP/DTLS state, ICE username fragments, transitions, candidate errors, close reasons and whitelisted stats. Candidate pairs include IDs, state/nomination, STUN request/response counts and round-trip/byte counters; candidates include address/port/type/protocol; transports include selected pair ID and DTLS state when supplied by the browser.

Capture is event-driven; getStats resolves asynchronously and may reject or omit fields. Those limitations and dropped records are reported. No polling timer, relay-specific recovery policy or native constructor wrapper is added. Reports retain diagnostic IP addresses, ports and peer IDs while redacting SDP/passwords/credentials.

Tests cover startup ownership, immediate queued ICE, replacement/stale ICE, non-owner protection, pre-close capture, bounded history/concurrency, older-result rejection, retrieval errors and retained evidence after core disposal. They do not establish physical-device connectivity or network-switch recovery.

## State-driven startup correction

The subsequent correction removes the added initial SYS birth deadline and its two-attempt exhaustion map. Pending connection checks are no longer retired by that deadline. Native failure, channel closure, valid incarnation replacement and explicit disposal continue to use existing owner-controlled recovery and teardown paths.

The entry facade also stops imposing its default 30-second admission deadline. Default join waits for admitted/failed/closed state or AbortSignal cancellation. A developer can explicitly pass timeoutMs to bound their own operation; this is caller-requested cancellation, not a declaration that ICE failed. Leave aborts the demo's pending join. This change does not remove every timer in the SDK or claim that all other lifecycle behavior is timer-free.

Tests advance simulated time past both former startup deadlines without retiring a checking birth or resolving pending admission. Native failure initiates recovery; abort/core failure closes admission; teardown releases scheduled work and listeners. Physical-device connectivity remains unverified until a report demonstrates it.
