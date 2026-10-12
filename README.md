# Phantom SDK

Phantom turns browsers into a peer-powered network, giving developers a foundation for applications that run on the strength of their users.

This is the current development base. Its API is still evolving.

## Getting started

Use Node.js 22 or later to install the workspace dependencies:

```sh
npm install
```

Import the browser entry in your application with a bundler:

```js
import { createBrowserPhantom } from '@phantom-local/doorways/browser';

const phantom = createBrowserPhantom();
const network = await phantom.create({ name: 'Alice' });
const invite = network.invite;

// In a separate browser client:
// const network = await phantom.join(invite, { name: 'Bob' });
```

The default entry uses Nostr relay discovery. Developers can configure other doorways. The application owns its interface and how it uses the network.

## Restricted networks

Applications can configure an admission checkpoint before relay discovery and peer admission. The developer owns the access rules; Phantom enforces credential verification and expiry. See [authorization integration](docs/admission.md). Without an `admission` configuration, networks retain open admission.

Run the focused checks with `npm test`.

## Source

- `packages/phantom-core`: networking engine, membership, peer roles, file distribution and admin controls.
- `packages/phantom-doorways`: browser entry and discovery adapters.

This repository contains SDK source and focused admission tests, without demo pages or generated test reports. Package names are local workspace names; the SDK is not published on npm.

## Example app

The [shared canvas](examples/shared-canvas/README.md) uses the actual SDK for laptop/phone drawing, QR invitations, peer messaging, and live connection inspection.

## Relay template

A [Cloudflare doorway template](templates/cloudflare-doorway/README.md) provides proof-preserving WebSocket discovery/signaling with hibernation support and deployment instructions.

## License

Free for the noncommercial purposes permitted by [PolyForm Noncommercial 1.0.0](LICENSE). Uses outside those permissions require a separate paid commercial agreement. See [commercial licensing](COMMERCIAL.md). Preserve the required notices in NOTICE when sharing the software.

Third-party dependencies retain their own licenses; see THIRD_PARTY_NOTICES.md.
