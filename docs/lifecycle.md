# Browser lifecycle and retained admin controls

## Browser event forwarding

`createBrowserPhantom` enables forwarding of real browser `online`, `pageshow`, and `document.visibilitychange` events into the networking engine. Visibility state is copied from the browser document. The engine's existing deadline observers decide whether an owned attempt expired; these events do not elect a leader or mark a peer admitted.

Direct `createPhantom` users retain a headless default. Set `observeBrowserLifecycle: true` with a runtime exposing a browser window and document to opt in. The facade removes every event listener on disposal. Runtimes without those event surfaces do not need DOM shims.

Forwarding wake events does not guarantee background browser execution or connectivity. Applications may request wake locks separately while transferring files.

## Recovering members and isolated admin orders

The developer's `readAdminOrders` hook is used for leadership checks. It can also be called by an ordinary member that has previously reached admitted state and is now recovering:

```js
const phantom = createBrowserPhantom({
  adminPublicKey,
  readAdminOrders: async ({ networkId, peerId, reason }) => {
    // Fetch retained signed orders from the developer's control service.
    // reason === 'recovery' identifies an ordinary member's recovery read.
    return controlService.read(networkId, peerId);
  },
});
```

A successor can receive an isolated admin's retirement order before every surviving leaf has rebuilt its SYS control path. The recovery read allows a leaf to obtain the retained signed order independently. Leaders and historical mirrors continue to use the leadership gate; the recovery read does not approve duties, alter election rules, or write membership.

Only one recovery read is in flight per instance. A recovering leaf repeats the read after a one-second delay when the previous read finishes. The developer should bound or abort the service request itself; a hook that never settles remains the single in-flight read. Reads stop on admission recovery, retirement, or close. A response arriving after close or restored admission is discarded. Errors emit `controlError` with `via: 'recovery'` and never approve leadership.

Returned orders are independently verified against the invite's pinned admin key and network, persisted through the control store, and applied by the existing admin lifecycle. A forged order cannot retire a member. The hook must be readable by recovering members; a service that restricts all reads to the current leader cannot provide this fallback. No Cloudflare control endpoint or new relay-side API is added by this change.

The control service must retain revocation/retirement orders independently of live peers. If no service is configured or it is unavailable, delivery still depends on surviving peer paths. This fallback does not guarantee delivery without a reachable authority store.
