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
