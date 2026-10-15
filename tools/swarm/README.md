# Phantom browser swarm

Separate test tooling using the real browser SDK, membership engine, distribution scheduler, WebRTC connections, admin signatures, and file verification. The harness has no test branches inside core. The SDK recovery fixes apply to every SDK consumer: unanswered offers retry, and historical survivors can accept successor status after new members have joined, fresh joins can recover from stale discovery routes, and eligible offers retained during handoff resume on leadership commit.

## Run

From the repository root, with Docker installed:

```sh
docker compose -f tools/swarm/compose.yaml up --build --abort-on-container-exit --exit-code-from swarm
```

The image installs Node, dependencies and Chromium. No service accounts, proxy configuration, exposed ports, or manual signaling setup. The container mounts reports into `tools/swarm/reports/` on your computer. The first build needs internet access and takes longer; subsequent runs reuse the image.

Or, with Node.js 22+ and Chromium's Linux system dependencies already available:

```sh
bash tools/swarm/run.sh
```

This installs the workspace and harness dependencies plus Playwright's headless browser, then runs eight isolated browser contexts. Installation output is saved to `tools/swarm/reports/setup-*.log`. For a visible browser, install full Chromium with `cd tools/swarm && npx playwright install chromium` and run `node launch.mjs --headed`.

The Docker option handles Linux browser libraries automatically. Native Linux setups missing libraries should use Docker or install them with `cd tools/swarm && npx playwright install-deps chromium` (may require administrative privileges).

## What it asserts

1. Every initial peer reaches committed admission.
2. A deterministic 256 KiB file reaches every initial peer and its complete SHA-256 and size match the publisher's fixture.
3. The publisher's browser context is closed; survivors regain admission and agree on a live replacement leader and mirror.
4. A fresh peer joins with empty storage and completes the same file without the publisher.
5. The late peer's verified chunk progress names surviving sources and never the departed publisher.
6. The current leader's browser context is closed; survivors agree on a replacement leader.
7. Another fresh peer completes the file after leadership changes.

A pass means these assertions succeeded. SDK error events are retained for diagnosis even when retries eventually succeed; inspect the timeline for transient faults. This baseline waits for initial replicas before publisher removal. It does **not** prove survival when only incomplete replicas collectively cover the file, network-switch recovery, NAT traversal, loss resilience, or many physically separate devices.

## Reports

Each run has a unique directory, printed when the runner exits:

- `report.html`: readable scenario results and links to diagnostics.
- `summary.json`: status, stage durations, evidence, test settings, SDK Git revision, dirty checkout flag, source hashes, dependency pins, Chromium version, and failure details.
- `runner.log` and `browser-stderr.log`: live runner output and native Chromium process diagnostics (including interface, sandbox, and process failures).
- `events.jsonl`: timestamped per-peer SDK events, sampled SDK snapshots, distribution jobs (source, receiver, chunk indexes), progress, verification results, RTC transitions, browser errors, signaling route outcomes, fault injection and stage deadlines.
- `initial-distribution.json`, `before-*-exit.json`, `after-late-peer.json`, `final.json`: SDK state, distribution inventories/jobs, completed hashes, source counters and native RTC stats.

Reports are flushed as the test proceeds. Exceptions and browser launch failures produce a report and nonzero exit code. Stage deadlines record what was awaited and last observed states. Snapshots are sampled at most once per second per peer; normal events are retained individually. Hard process/OS kills can leave partial logs without a final report. Reports can include peer IDs, invitation/signaling data and candidate addresses; review before public sharing. The harness never exports the admin private key.

## Options

```sh
bash tools/swarm/run.sh --peers 12 --bytes 1048576 --timeout 180000
```

`--peers` is the initial swarm size (3–64), `--bytes` is fixture size (1 KiB–16 MiB), `--timeout` is each stage's deadline in milliseconds, and `--output PATH` selects the report directory. Larger swarms may saturate a small machine. Start with eight. For an already installed Chromium executable, `PHANTOM_CHROMIUM_PATH=/path/to/chromium node tools/swarm/launch.mjs` skips reliance on the downloaded Playwright executable.

## Network scope

The baseline runs one Chromium process with isolated browser contexts (separate cookies and storage), on one host network. Payloads use real RTC data channels; the loopback WebSocket service handles only discovery/signaling. It uses the SDK's public custom-doorway extension, not a mocked core. The local test service is intentionally unsecured, bound to loopback, and is not a production relay implementation.

Closing a context abruptly removes its browser resources but is not equivalent to cutting a network cable; the WebSocket service observes departure immediately. No HTTP proxy or Playwright offline toggle is presented as WebRTC/NAT simulation. Docker here simplifies deployment; it does not provide separate peer networks. Multi-host runners and controlled network namespaces are future additions.

## Checks

```sh
npm test
node --test tools/swarm/*.test.mjs
```

The relay check uses real independent WebSocket clients and verifies discovery, unaltered routing, spoofed sender rejection, and departure observations. The browser run itself is the end-to-end check.

Fresh unadmitted peers can follow correlated route advice from a healthy member immediately when discovery points to the wrong live peer. Advice changes only the pending connection target; normal signed SYS admission still establishes authority. Existing Cloudflare doorway deployments need the updated template deployed to forward this signal; older deployments retain the unanswered-offer fallback.

## Stress mode

```sh
bash tools/swarm/run.sh --stress
```

Stress fixtures must be at least 32 KiB. Defaults: three independent runs, eight initial peers, a 4 MiB fixture per transfer, and 180-second stage deadlines. The existing seven baseline checks run first in each repetition. Four additional stages then remove an ordinary receiver on its first incomplete chunk, remove the mirror during another transfer and start two fresh peers before waiting for recovery, drop all relay signaling while established SYS RTC connections coordinate a new transfer, and admit a fresh peer after signaling is restored. Every surviving peer must verify the current file's complete size and SHA-256 after each fault.

Faults follow observed progress and recovery state. No fixed sleep schedules the failures. Polling and deadlines bound observation; signaling is restored on verified completion or failure. Every publication has distinct deterministic bytes so an old replica cannot satisfy a new transfer check.

```sh
bash tools/swarm/run.sh --stress --runs 5 --bytes 8388608
```

The top-level `report.html` and `stress-summary.json` summarize all repetitions. Each `run-001`, `run-002`, etc. contains full logs and diagnostics. Failed repetitions do not hide later results; any failed or interrupted run makes the overall exit code nonzero. The aggregate is updated after each run. To repeat just one stress scenario set, use `--stress --runs 1`.

The blackout drops discovery/signaling packets while keeping WebSocket sockets open. It does not cut RTC traffic or model NAT, packet loss, a Wi-Fi switch, or a dead relay TCP connection. Distribution jobs can negotiate over existing SYS RTC control paths. The mirror scenario retains the publisher, so this does not prove survival when no remaining peer holds a complete copy.

The controller retains the original admin signer in memory independently of browser peers. For new stress publications after the seed exits, the publishing peer calls public `publishFile` to obtain the manifest, the controller signs a room-bound `content.publish` authorization, and the peer passes that proof to public `distributeFile`. Surviving peers receive signed proofs, never the admin seed. Transport leadership does not confer admin privileges. No signer seed is written to reports.

Admission requests already staged against a mirror remain retained until committed. If mirror loss preempts that transaction, commit of the replacement topology triggers replay through normal admission checks. The retained request must still own its exact live SYS transport and pass the current authorization/incarnation checks. Room retirement clears retained admissions. While concurrent joins are pending, the runner now prints periodic join states rather than waiting silently for the outer deadline.

RTC diagnostics retain only live native connections and expose cumulative `rtcCounts` in snapshots. SDK distribution reports setup failures immediately, cleans up partial connections and buffered references, prevents late setup after leader cancellation, and applies increasing cooldown to repeatedly failing source/receiver pairs. `distributionConnectionFailed` and `distributionJobFailed` explain the failed stage and retry interval. The observer releases closed objects without requiring a native state event; it retains no historical native object list.

## Live Nostr relays

Start with a four-peer smoke scenario:

```sh
bash tools/swarm/run.sh --nostr --peers 4 --timeout 180000
```

This uses the SDK's bundled production Nostr adapter and relay manager. The seed discovers working relays; all joining peers use the actual relay coordinates encoded in its invite. No account, proxy, relay deployment or manually chosen relay URL is required. The local server only serves the browser harness page in this mode; it does not provide discovery/signaling. A new room ID isolates each run. Public relays may reject or rate-limit publications; those outcomes are logged.

After smoke passes:

```sh
bash tools/swarm/run.sh --nostr --stress --runs 1
```

For an explicit shared relay set, add `--relays wss://YOUR-RELAY,wss://YOUR-SECOND-RELAY` (1–8 URLs). To configure the SDK's normal adapter failover with your existing Cloudflare doorway, add `--cf wss://YOUR-WORKER/ws` alongside `--nostr`. No Cloudflare endpoint is assumed. Test Nostr alone first; a mixed-adapter pass does not establish that Nostr carried every signal. This mode supplies production doorway descriptors and does not change SDK selection/recovery behavior.

`summary.json` records requested transport descriptors and the seed's resolved doorways. `relay.discovery` records SDK discovery progress; `doorway.status` records adapter selection/recovery; `signaling.open`, `.close`, `.error`, `.tx` and `.rx` identify native socket URLs and frame types. Nostr OK receipts include accepted/rejected, event ID and reason. No private signing material or complete event content is exported by the socket observer. Normal SDK diagnostics remain available.

In live stress mode, the signaling blackout drops incoming/outgoing frames at every peer's SDK socket wrapper, leaving native sockets and RTC paths open. It does not control the public relay, disconnect TCP, or specifically force Cloudflare failover. Restoration allows future frames through; it does not fabricate relay acknowledgements. A fresh join then exercises the actual live discovery path.

Native live relay validation is pending. Browsers still share one host network, so this does not establish cross-network NAT/Wi-Fi resilience. Omit `--nostr` to run the validated local signaling mode.

Nostr publication handling coalesces unchanged announcements, with a 15-second refresh interval when reconciliation requests the same announcement. Changed discovery/authority announcements publish immediately. Explicit negative ACKs disable writes to that relay for the current bus session; remaining relay paths continue. The socket may still receive verified events while another writer works. If every path is rejected or exhausted, the adapter reports exhaustion to the normal recovery/composition layer. Reconnection of the same rejected bus does not bypass its session write block. This does not permanently blacklist a URL or alter admitted peer membership.

Signaling submitted while relay paths are still opening is retained in the existing bounded 256-publication queue and flushed on a writable path opening. If every path is known rejected or exhausted, sends fail explicitly rather than queueing indefinitely. Close clears queued publications. Diagnostics distinguish `signal_queued`, relay `publish` receipts and verified `signal_received`, including control type and SYS attempt ID where present. Relay acceptance is not peer delivery or membership admission.

Cached discovery announcements publish before queued signaling when a Nostr relay opens. The receiving Nostr doorway also tolerates authenticated signaling arriving before verified sender discovery: at most 64 controls (16 per sender, 64 KiB each) wait for up to 15 seconds, with expiration checked on incoming events. Replay follows the discovery roster projection and checks full sender identity and boot incarnation. Closed sessions clear the queue. `signal_deferred`, `signal_delivered` and `signal_discarded` explain that transition; none grants network membership.

## SDK contract test

Run `node tools/swarm/contracts.mjs` from the repository root (after installing the same dependencies and Chromium as above). `PHANTOM_CHROMIUM_PATH` selects an existing browser. An optional positional directory selects the report destination. CI runs this alongside the existing swarm scenarios.

Three isolated browser contexts use the public browser SDK with automatic messaging, selected-recipient distribution, and a test-only IndexedDB chunk store. The test sends a message, closes native messaging channels, requires a different ready attempt and another delivered message, then interrupts a selected receiver after its first chunk transaction commits. It reloads both publisher and receiver with retained identities and IndexedDB, verifies the complete SHA-256, and checks that chunk zero was not requested again. An excluded peer must have no stored chunks, received binary chunks, completed local copy, or assigned receiver jobs when distribution is quiescent.

The runner writes `summary.json` and `events.jsonl`, including stage evidence, final SDK state, Chromium version, revision and source hashes. It exits nonzero on failure. This is real WebRTC on one host with a loopback signaling relay; it does not establish TURN/NAT behavior. The IndexedDB adapter and channel-close injection belong only to the test harness. No production SDK hooks or private Cloudflare endpoints are used.

## TURN-only contract test

With Coturn (`turnserver`) installed, run `PHANTOM_TURN_TRANSPORT=udp node tools/swarm/contracts.mjs` or use `tcp`. The runner starts an authenticated, loopback-only Coturn instance with temporary credentials and cleans it up afterward. CI runs both transports in isolated jobs. No external TURN account or public doorway is needed.

All SDK peer connections receive `iceTransportPolicy: 'relay'`. The same messaging recovery, recipient exclusion, IndexedDB reload and file-resume assertions run. An additional assertion requires native selected candidate pairs for system, messaging and distribution channels, with both candidates of type `relay` and the expected TURN allocation transport. The report includes selected-pair evidence and Coturn version/logs, without copying its generated password into the summary. This verifies TURN usage on one host, without modeling corporate firewalls, symmetric NAT or TURN/TLS.
