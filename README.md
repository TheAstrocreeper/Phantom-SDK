# Phantom SDK

**[Explore the demos →](demos/README.md)**

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

Run the regression checks with `npm test`.

The browser entry forwards wake events to the engine. Applications with an isolated authority store can recover retained signed orders through their admin hook. See [browser lifecycle and retained controls](docs/lifecycle.md).

## Source

- `packages/phantom-core`: networking engine, membership, peer roles, file distribution and admin controls.
- `packages/phantom-doorways`: browser entry and discovery adapters.

This repository contains SDK source, integration tests, runnable demos, and a relay template. Generated bundles and test reports are excluded. Package names are local workspace names; the SDK is not published on npm.

## Relay template

A [Cloudflare doorway template](templates/cloudflare-doorway/README.md) provides proof-preserving WebSocket discovery/signaling with hibernation support and deployment instructions.

## License

Free for the noncommercial purposes permitted by [PolyForm Noncommercial 1.0.0](LICENSE). Uses outside those permissions require a separate paid commercial agreement. See [commercial licensing](COMMERCIAL.md). Preserve the required notices in NOTICE when sharing the software.

Third-party dependencies retain their own licenses; see THIRD_PARTY_NOTICES.md.




[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/theastrocreeper)
