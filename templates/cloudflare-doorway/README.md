# Cloudflare doorway template

A Phantom discovery and WebRTC signaling worker. It routes peers through one Durable Object per network; it does not host files, chat history, or the authoritative Phantom membership roster.

## What is included

- `/ws?v=<networkId>&to=<shortPeerId>` with the existing SDK wire format.
- `/discover` as an equivalent WebSocket entry alias and `/health` as a static health check.
- Proof-preserving discovery rows and signaling, plus optional server-side admission verification.
- A per-connection challenge, preventing a captured announcement from replacing another socket.
- Durable hibernation recovery and the original ten-second endpoint replacement grace period.
- Bounded messages, sockets, and unjoined handshake lifetime.

This template is configured for a new worker and namespace. Existing deployments require their own migration plan.

## Configure and run

Run from the Phantom SDK repository root after `npm install`. The template imports the repository's shared proof verification code; keep those imports when adapting it to another project.

1. Give `wrangler.jsonc` your own worker name. Its SQLite Durable Object migration is for a **new namespace**; do not apply it blindly to an existing deployed class or namespace.
2. Set `ALLOWED_ORIGINS` to a comma-separated list of your app's origins. Origin filtering is a browser policy, not authentication; clients can omit or forge Origin. An empty list disables this filter.
3. For restricted operation, set `ADMISSION_AUTHORITY_PUBLIC_KEY` to your trusted authority's 64-character Ed25519 public key. It must match the app's `createAdmissionVerifier`. No private signing key belongs in this worker. For local development, copy `.dev.vars.example` to `.dev.vars` beside the config and fill it in. For deployment, set the public key in `vars` or your Cloudflare deployment configuration.
4. Leave `ADMISSION_MODE` as `restricted`, or explicitly choose `open` for a public network.

```sh
npx --yes wrangler@4.149.0 dev --config templates/cloudflare-doorway/wrangler.jsonc
```

The server accepts a socket, issues a unique challenge, and requires a valid signed join before exposing its roster or routing signaling. Pending sockets cannot replace joined peers. Without the authority key, restricted operation returns a configuration error instead of silently opening access.

To build without deploying:

```sh
npx --yes wrangler@4.149.0 deploy --dry-run --config templates/cloudflare-doorway/wrangler.jsonc
```

To deploy to your account after configuration:

```sh
npx --yes wrangler@4.149.0 deploy --config templates/cloudflare-doorway/wrangler.jsonc
```

## Connect the SDK

```js
const phantom = createBrowserPhantom({
  // Base origin; the SDK appends /ws and the network/peer query parameters.
  doorways: [{ type: 'websocket', url: 'wss://YOUR-WORKER.YOUR-SUBDOMAIN.workers.dev/' }],
  admission: {
    authorize: obtainCredentialFromYourApp,
    verify: createAdmissionVerifier({ authorityPublicKey: YOUR_AUTHORITY_PUBLIC_KEY }),
  },
});
```

See [network admission integration](../../docs/admission.md) for the developer-owned authorization hooks and signing authority. The WebSocket adapter waits for the server's `hello.connectionChallenge`, then signs the join with that challenge. A custom adapter can use the shared `context.signAdmission(kind, payload)` callback for its own challenge protocol.

For an open network, configure `ADMISSION_MODE: "open"` on the worker and omit the app's `admission` configuration. Restricted and open clients are not interchangeable policies for the same network.

## Persistence and limits

Cloudflare hibernation restores connections using compact socket attachments. Full peer proofs are stored separately under per-endpoint durable records so they do not exceed the socket attachment limit. Joined records and pending departure leases survive a wake; a successful replacement cancels the old lease. Peer proofs are retained while their endpoint is live or in its replacement grace period, then deleted.

The starting limits are 64 live/pending sockets per network, 128 KiB per incoming JSON packet, ten seconds to complete a join, and ten seconds of replacement grace. These are editable template constants, not SDK-wide restrictions.

Expiry rejects new joins and signaling but does not forcibly terminate established P2P transfers. The relay never manufactures signed peer exit messages: removing a socket updates reachability hints, while Phantom's own protocol owns committed membership. Only supported signaling types are relayed; the worker is not an arbitrary application-data relay.

## Validation

From the repository root, `npm test` includes template tests for proof retention across a simulated hibernation wake, exact signaling forwarding, copied grants, captured-join replay, endpoint replacement races, departure alarms, large proofs, unjoined sockets, invalid routes, and configuration failures.

Run `npm run test:cloudflare:runtime` for a local Cloudflare runtime smoke test. It starts Wrangler with a temporary in-memory signing authority, connects two authorized peers, checks exact signal delivery, and confirms a captured join cannot replace a socket. It requires port 8787 to be free and downloads the pinned Wrangler version on first use.

Mocked lifecycle tests and the local runtime smoke test do not establish real Cloudflare hibernation timing or production-scale behavior. A Wrangler dry run verifies bundling and configuration without deploying.

Cloudflare references: [WebSocket hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/) and [Durable Object class configuration](https://developers.cloudflare.com/durable-objects/reference/durable-objects-migrations/).
