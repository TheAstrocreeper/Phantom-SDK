# Swarm recovery validation

The SDK implements generic recovery and distribution behavior; fault injection and browser automation live under `tools/swarm`. No SDK branch checks the harness, a user identity, or a test scenario.

## Changes

- Unanswered SYS bootstrap offers retry without retiring answered or progressing native connections. Correlated route advice corrects stale discovery targets without committing authority.
- Historical mirror handoff retains eligible offers and matching ICE, then resumes on leadership commit through normal permission and transport checks.
- Successor status preserves historical survivors while allowing new members during handoff.
- Proven staged admissions survive preemption when a mirror disappears. Committed requests are removed; replay checks current authorization, incarnation and exact live transport.
- Distribution setup failures release jobs promptly, close partial endpoints, discard buffered references and detach handlers. Cancellation before asynchronous setup prevents late connections. Repeated setup failure backs off the source/receiver pair; verified delivery clears its penalty.
- Harness diagnostics retain live native RTC objects and cumulative scalar counts. A separate in-memory controller supplies explicit admin-signed publish proofs after the original admin peer departs; election does not confer signing rights.

## Automated checks

At `979b0e1`, 127 SDK tests and eight harness tests passed locally and in CI. The native five-peer 4 MiB baseline, forced successor offer/ICE retention case and five Cloudflare peer-bridge repeats passed. Across the repeats, all 40 stages and 1280 jobs passed, with zero recorded job or bridge failures and 2560 endpoint opens matched by 2560 closes. Reports are attached to [CI run 38698249320](https://github.com/TheAstrocreeper/Phantom-SDK/actions/runs/38698249320).

The current review adds Nostr queue expiry, discovery refresh after cooldown, and metadata tests to CI. Two regressions failed before the queue/discovery fixes and pass after them. Validation of that follow-up head is recorded in the PR; the native counts above apply specifically to `979b0e1`.

CI runs SDK, swarm-harness and Nostr-tool tests, the native baseline, the forced successor case and the five-run Cloudflare repeat. Reports are uploaded even after a step fails. The paired RTC fixture tests protocol behavior and does not emulate browser ICE/NAT.

## Native evidence — 2026-10-14

The v8 implementation was tested locally in native Chromium with eight initial isolated browser contexts and 4 MiB files. Four complete stress runs passed: one initial run and a subsequent three-run batch. All eleven stages passed in each run. This includes initial replication, publisher departure, late survivor-supplied downloads, another leader departure, ordinary receiver loss mid-transfer, mirror loss with concurrent joins, relay-signaling blackout and a new join after restoration. Complete size and SHA-256 verification are required for each asserted transfer.

| Stage | Three-run duration range |
| --- | --- |
| Publisher-loss recovery | 26.114–28.251 s |
| Second leader-loss recovery | 26.915–28.027 s |
| Mirror loss, concurrent joins and download | 50.528–51.107 s |
| Final fresh join and download | 32.328–32.376 s |

The final newcomer downloads four accumulated 4 MiB assets; the final stage waits for the newest file. Its roughly 32 seconds is not the admission time or a single-file throughput measurement. A prior v7 run admitted in 0.44 seconds but reached 169.8 seconds due to repeated RTC construction failures and job deadlines. Those extreme stage timings did not recur in the three-run v8 batch. Aggregate summaries alone do not prove every run had zero transient errors.

## Scope and next validation

This is repeatable single-host recovery evidence. One Chromium process contains isolated browser contexts. Real RTC carries payloads; loopback WebSocket discovery/signaling does not transport file content. Context closure lets the relay observe departure immediately. The blackout drops signaling frames while keeping sockets and RTC paths open.

Small public Nostr discovery, delivery, idle, explicit reconnect and payload samples are documented under `tools/nostr`; they do not validate full Nostr swarm recovery. Cross-host discovery, real NAT traversal, Wi-Fi/mobile switching, network partitions and collective incomplete-replica survival remain follow-ups. Existing Cloudflare doorway deployments must deploy the updated signal allowlist to forward routing advice; older deployments retain the unanswered-offer fallback. Outage credit requires confirmed failure on the exact native transport. Discovery hints and retained signaling do not grant election authority.

Run `bash tools/swarm/run.sh` for smoke, or `bash tools/swarm/run.sh --stress --runs 3` for the repeatable stress set. Full diagnostic artifacts remain local under `tools/swarm/reports/` and are excluded from git.
