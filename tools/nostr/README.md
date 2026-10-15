# Nostr info finder

Run from the repository root with Node 22 or later (native WebSocket required):

```sh
node tools/nostr/profile.mjs wss://relay.damus.io/ wss://nostr.mom/ --out=report.json
```

No dependencies or credentials. Checks relays sequentially, with a 12-second budget per relay, two temporary signing identities and a fresh room. Each peer publishes an announcement, one discovery reply, and one targeted signal. It does not scan rooms or send load-test traffic.

JSON includes a bounded, relative-time event timeline: connection/open, EOSE, discovery, signal submission and receipt, publication ACK latency and signed event size, rejection reasons, NOTICE, authentication requests, subscription closure, and connection exhaustion. Per-peer timings distinguish opening, discovery and signal delivery. EOSE marks the end of initial results; it does not prove successful discovery or delivery. ACK acceptance and receipt are counted separately. Delivery latency includes local signing, relay transit and receiving verification; ACK timing starts after signing. Overall duration includes identity creation and connection setup.

One discovery reply lets a late subscriber discover an already-open peer even when the initial ephemeral announcement was not stored. This is a probe-only exchange, not an SDK-wide discovery-policy change. The fake ephemeral relay regression deliberately opens subscriptions at different times and retains no history.

Initial evidence: `results/2026-10-15-initial.json`. All four seeded relays delivered in both directions and accepted both signals. Signal ACKs ranged from 100–245ms; Snort explicitly returned `ephemeral: will not be stored`. This is one observation per relay, not an availability or performance ranking. That first report predates the derived per-peer timings; its raw timeline contains the same events.

The initial check uses small, fixed payloads. It does not measure payload limits, sustained throughput, reconnect recovery, history retention, relay federation or WebRTC connectivity. Those require separate bounded experiments. No authentication is attempted if a relay requests it.

A subsequent Damus check (`results/2026-10-15-damus-repeat.json`) accepted discovery announcements but rejected a signal with `rate-limited: you are noting too much`. The profiler stopped on rejection. This establishes observed throttling, not its exact window, quota or identity/IP scope; no immediate retry was attempted. Keep runs spaced apart and treat rejections as measurements rather than retrying aggressively.

## Metadata and delayed joining

The finder also fetches the optional [NIP-11 information document](https://github.com/nostr-protocol/nips/blob/master/11.md) using `Accept: application/nostr+json`. It saves declared software, capabilities and limits without assuming they are enforced exactly. Metadata has a 5-second timeout and 64 KiB cap; linked URLs are not followed. Use `--metadata-only` to avoid WebSocket publications.

`--stagger=1500` starts the second connection 1.5 seconds after the first connection opens. Compare actual `openedMs` values in the report, since handshake time varies. The normal default remains simultaneous construction. The earlier `2026-10-15-creation-delay.json` delayed construction from the initial call; actual opens nearly overlapped, so it is not evidence of a genuinely late subscriber. `2026-10-15-staggered.json` uses the corrected first-open delay against Snort.

Declared limits observed in this sample: nostr.mom 131072-byte messages / 50 subscriptions; Snort 524288 / 300; Primal 1000000 / 20. These are metadata, not measured boundaries. Avoid interpreting missing metadata fields as unrestricted access. Full signed message frames also include framing bytes beyond the recorded event size.

Corrected Snort result: opens at 3241ms and 7602ms (4361ms apart), discovery at 7832ms and 7967ms, targeted delivery 126ms and 233ms after the opposite peer's submission. Both signals were accepted. This demonstrates successful delayed discovery with the probe's reply exchange; there was no paired no-reply control on the live relay, so it does not prove which messages Snort would otherwise retain.

## Publication policy

The SDK classifies NIP-01/NIP-42 rejection prefixes separately from the original reason text:

| Prefix | Local behavior |
| --- | --- |
| `rate-limited` | Pause new writes for 30 seconds by default (`publicationCooldownMs`); keep reading and processing outstanding ACKs. Emit `publication_paused` / `publication_resumed`. |
| `blocked`, `restricted`, `auth-required`, `banned` | Stop writes for this bus session, keep reading, and request alternative relay paths. No automatic authentication. |
| `mute`, `invalid`, `pow`, `error`, unknown | Reject that publication, keep the path eligible for future messages. No automatic replay. |

The 30-second pause is a conservative local policy, not a measured relay cooldown or a server-supplied retry time. The relay set can queue fresh messages while paused (within its existing 256-entry bound), and sends them when a path becomes eligible, provided their 10-second publication lifetime has not expired. Rejected messages are not automatically replayed on the rejecting path; other opening paths can still carry the original message. All-path temporary pauses do not declare permanent exhaustion. Event-specific rejection does not prove later messages will succeed.

An incoming NIP-42 `AUTH` message emits `authentication_challenge`, not `authentication_required`: a challenge alone does not establish that our operation is restricted. A subsequent `auth-required` rejection does. Challenges are observed without signing an AUTH response.

[NIP-66](https://github.com/nostr-protocol/nips/blob/master/66.md) describes existing relay-monitor observations and explicitly allows measured policy to differ from NIP-11 metadata. Such data is a future source of candidates, not an implemented dependency or a trusted replacement for probing.

## Idle and reconnect

`--idle-reconnect=15000` requires three independent bidirectional signal rounds: initial, after 15 seconds without application writes, and after explicitly reconnecting peer 1. The overall budget is the idle interval plus 25 seconds. Each round requires both ACKs and verified peer delivery. After reconnection, discovery must occur again via the probe's stage-specific announcement/reply exchange; earlier deliveries cannot satisfy the new round. Temporary relay throttling or closure fails the check rather than causing a retry loop. WebSocket control-frame keepalives are outside this application-idle measurement. This exercises an explicit client reconnect, not an unplanned network outage or mobile network switch.

Observed Snort run (`results/2026-10-15-idle-reconnect.json`): initial delivery 189/325ms; after-idle delivery 244/256ms; reconnect delivery 170/238ms. Reconnect was requested at 20982ms, peer 1 reopened at 24623ms and the final round completed at 25301ms (4319ms total recovery including handshake and rediscovery). All six signals were acknowledged and received. No idle closure was observed during this 15-second application-idle sample; it does not establish the relay's general idle policy.

## Modest payload check

`--payload=8192` adds 8192 ASCII padding bytes to each targeted control and verifies their exact contents after signed-envelope verification. The option is capped at 16384 padding bytes; it does not search for rejection boundaries or generate sustained traffic. Actual serialized events include the control, signed identity binding, tags and JSON overhead. New runs record `wireBytes` for the complete UTF-8 `["EVENT", event]` JSON message, alongside `eventBytes` for the signed event alone (neither includes WebSocket/TLS framing).

Primal accepted and delivered both 8 KiB-padding controls in `results/2026-10-15-payload-8k.json`. Events were 9445 bytes each, implying 9455-byte compact EVENT messages (the wire-size field was added after this sample). ACKs took 156/209ms. This is a compatibility observation at one size, not a measured maximum, throughput benchmark or controlled latency comparison with the earlier baseline.


## Queue lifetime and cooldown discovery

Relay-set signals expire after 10 seconds by default (`publicationLifetimeMs`, positive and at most 15000 ms). Expiration releases the queue entry and emits `signal_expired` with the message/attempt identifiers. Expired offers and ICE cannot flush on a late opening or cooldown resume. This is a queue lifetime, not delivery confirmation; receiving-side attempt checks still apply.

When fresh queued work survives a cooldown, the relay set refreshes its cached announcement before flushing signals. Announcement coalescing tracks what was actually submitted, so updates cached during a pause are not mistakenly treated as already sent. It does not schedule discovery retries when no live queued work remains. Departure, reconnect and close clear queue-expiry timers.

The `banned:` prefix and the exact observed reply `Policy violated and pubkey is not in our web of trust.` are compatibility cases in the local classifier. The latter maps to `restricted`. Both stop new writes for that bus session while reads and outstanding ACK processing remain active; reconnecting that bus does not clear the write block. They do not create a persistent relay blacklist or infer a server ban duration. Other unknown replies retain event-scoped handling.

The exact unprefixed `rate limited` reply also maps to the existing `rate-limited` cooldown policy. Its raw reason remains unchanged; this does not infer a server-provided retry duration.
