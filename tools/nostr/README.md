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
