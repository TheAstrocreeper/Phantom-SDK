# Regression checks

Run `npm test` from the repository root with Node 22 or later.

The suite covers core instance isolation and disposal, browser wake events, scoped authorization, peer admission, doorway contracts and fallback, Nostr envelope validation, deferred membership, SYS birth and deadlines, successor proofs/retry, retained admin orders, revocation/retirement, verified file transfer/resume, and bounded automatic distribution. Mixed-doorway scenarios include role departures, same-identity returns, delayed signaling, and abrupt transport loss.

Fixtures remain under `tests/fixtures` and are excluded from SDK package contents. Scenario helpers import the actual workspace SDK; they do not embed a separate engine. The `native-network` and mixed-trace fixtures simulate RTC channels and relays in memory. Here, native means the native signaling packet contract, not native browser WebRTC. These tests establish protocol/state behavior, not NAT traversal, browser throughput, or production scale. The 50-peer check is a simulated run.

Worker-specific checks use the current Cloudflare doorway template. Retired worker HTTP controls, production relay probe reports, old UI pages, and extraction scripts are not restored as runtime dependencies. A live Worker smoke check is available separately through `npm run test:cloudflare:runtime`.
