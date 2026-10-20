# Phantom Share for Android — first device build

Open this folder in Android Studio. This is a native Java/WebView shell around
the existing Phantom Share web interface and actual workspace SDK. It does not
fetch its interface from a website. Pairing/signaling still uses the configured
Nostr relays; files travel through the existing WebRTC distribution path.

## Open and run

1. Extract the project to `~/AndroidStudioProjects/PhantomShare`.
2. Android Studio → **Open** → select that folder (the one containing
   `settings.gradle.kts`). Let Gradle sync finish.
3. If Studio asks for an Android SDK location, choose `~/Android/Sdk`.
   Use Studio's bundled JDK for Gradle. Versions are pinned: AGP 9.2.1,
   Gradle 9.4.1, Android SDK 37.0, build tools 36.0.0, AndroidX WebKit 1.16.0.
   Studio may download these dependencies once; no emulator is required.
4. Select your connected Pixel 7, select the `app` run configuration and press
   **Run ▶**. This builds and installs a debug app; no store account is needed.

Minimum OS is Android 13 (API 33); the initial device test is Android 17.
No NDK, Compose, emulator, analytics or billing SDK is included.

## First check

Keep the desktop's existing web app open. Create an invitation on the PC, then
join from the Android app with its six-character code or QR. Send a small file
each way, use **Save** on Android, and choose its destination in the system file
picker. Exporting diagnostics also uses the system save picker. Then Leave and
join again with a fresh invitation. Camera permission is requested only when
Scan QR opens the camera; denying it leaves manual entry available. Android 17 also requests Nearby devices
permission on connecting, so same-Wi-Fi WebRTC traffic is allowed. A denial
stops the attempt with a permission message rather than waiting for RTC timeout.

## Native behavior

- Secure local HTTPS asset origin with `WebViewAssetLoader`; file-URL access is
  disabled. Missing app assets fail locally instead of falling back to a server.
- The native save bridge accepts messages only from the app's top-level origin.
  It writes one acknowledged 64 KiB chunk at a time to an explicitly selected
  document, checks sequence and length, and attempts to delete partial documents
  after cancellation or failure. A provider may refuse deletion; inspect the
  destination after a failed save.
- No broad storage or microphone permission. The system document picker grants
  access to the selected files. A connected-device foreground service keeps an
  ongoing session notification; notification permission is optional. A bounded
  partial wake lock is renewed natively for the whole session, including idle
  time, and released on Leave/reset/app close. This consumes battery while a
  session remains open; it does not keep the screen lit.
- Rotation keeps the same Activity/WebView. Back asks before closing the session.
  Camera scanning and speed tests stop when the app backgrounds. Connected
  sessions keep WebView web contents runnable through window visibility changes
  using SessionWebView, plus important renderer priority and the foreground
  service. Native focus events stop camera/speed tests even though DOM visibility
  remains visible during the session. Ending a session restores actual window
  visibility. Returning to the UI does not pause/resume an already-running view.
  Background/lock behavior still needs physical device validation; Android can
  terminate the process under memory pressure.
- Android default-network replacement or loss ends the active session immediately
  and returns the UI to Connect. The callback uses a local Network handle, not an
  SSID/MAC/location lookup. It is registered once and unregistered on Activity
  destruction. Same-route capability updates do not reset the session. Automatic
  handover recovery is intentionally deferred.
- A failed/cancelled join completes asynchronous membership release and transport
  cleanup before another local attempt may acquire the device identity. The
  interface shows Cleaning up during that work. Late callbacks cannot start a
  disposed core or revive a cancelled attempt. The existing requested admission
  budget belongs to that attempt; waiting-state updates do not restart it.
- Closing the Activity tears down the WebView and pending native operations.
  OS process death also ends the session. Relaunch requires a fresh connection;
  downloaded-but-unsaved web blobs do not survive process death.
- Debug WebView inspection is enabled only for debug builds. Device identity
  stays in app-private WebView storage and app backup is disabled.

Android app links, receiving items from Android's share sheet,
store signing/billing and production icons are not implemented here.
The launcher reuses the existing Phantom mark. There is no Play release.

## Public invitation URLs

The bundled app's QR/link points to `https://phantom-share.pages.dev/`, the public
project address, rather than the private `appassets.androidplatform.net` origin.
The PC can still create invitations and the app can join them if that public
alias is not the deployment you use. Test the generated public link before
sharing it with others. Set the existing deployment URL when rebuilding assets:

```sh
# repository root; web build dependencies must already be installed
PHANTOM_PUBLIC_URL=https://your-current-deployment.example/ node apps/phantom-share-android/prepare-web.mjs
```

A future domain replaces that setting; it is not needed to install this app.
The standalone project download already includes built assets; Node is not
needed just to open/build it in Android Studio. From the repository, rerun
`prepare-web.mjs` after web/SDK changes, then rebuild the Android app.

## Validation

See `BUILD.txt` in the packaged project for checks performed on that exact build.
Protocol tests are not a security audit. Camera, document providers and physical
phone behavior require a device check.

## Licensing

Phantom source retains the repository's PolyForm Noncommercial license and
commercial licensing terms. AndroidX WebKit and the Gradle wrapper are separately
Apache-2.0 licensed. Bundled JS/WASM notices are in the app assets. Gradle's license
is included as `GRADLE-LICENSE.txt`. No signing secrets are included.


Session replacement uses one queue for admission and active-session teardown.
The app enters `ending` before closing owners, disables Create/Join, awaits the
native end-session acknowledgement, and releases its identity lock only after
local disposal settles. Each owner's close is attempted even when another
throws or awaits an acknowledgement. Lifecycle transitions are recorded in
exported diagnostics. These guarantees do not establish that a failed Wi-Fi
RTC route is recoverable; a new session still requires successful ICE.
