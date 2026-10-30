# Installed page bridge and Java/JNI binding — prototype

The desktop page bridge uses a Chromium extension and the browser's native
messaging protocol. The installed program is the Rust `phantom_helper` binary:
Node is not needed by the installed helper, and there is no localhost HTTP
server, WebSocket listener or local TLS certificate to configure.

The page uses `createBrowserHelperAdapter()` from
`@phantom-local/tunnel/browser-helper` as the `native` adapter for
`createBulkProbeControl`. Phantom's existing WebRTC control channel still
negotiates the peer/session and exchanges native offers. The extension carries
only local setup/receipts between the page and helper; it never carries the
bulk bytes. The helper generates, streams and hashes the finite probe entirely
inside the native core.

## Boundaries and lifetime

- Only `https://phantomshare.app` in a top-level document can use the content
  bridge. The background worker independently checks the browser-supplied
  extension ID, tab, frame and origin. There is no external-extension listener.
- A fixed public extension key pins its development extension ID. Native host
  manifests allow exactly that extension, and the binary checks the browser's
  caller-origin argument. The argument is a launcher check, not a secret or a
  defense against other applications already running as the same OS user.
- One content port owns one native host process. Navigation, page hide, explicit
  adapter close and port disconnect terminate that ownership. Native stdin EOF
  cancels its jobs. No request can name an executable, shell command or file.
- The helper accepts only prepare/connect/close with strict JSON fields. It caps
  frames at 16 KiB, four jobs per process and finite probes at 32 MiB. Prepared
  jobs have a one-shot 30-second setup lease; probe execution has a 120-second
  deadline. Neither timer retries/reconnects. Closed job IDs have a bounded replay
  cache. Output pressure terminates this ephemeral process rather than blocking
  cancellation indefinitely.
- The extension limits active document ports and checks message sizes. The page
  adapter limits pending calls, fences late responses and closes a job on abort.
  Offers/ICE credentials are not sent to console logs or general SDK events.
- The trusted site's scripts are the caller of this capability; installing it
  does not authenticate arbitrary pages or replace Phantom peer admission.

There is no bulk relay or WebRTC payload fallback. A failed direct route remains
a failure. Current endpoint policy is local helper configuration: bind all
interfaces and use `stun.l.google.com:19302`, with trusted process-environment
`PHANTOM_HELPER_BIND`/`PHANTOM_HELPER_STUN` overrides for controlled tests. The
page cannot change this policy. The `none` STUN setting is for local tests.

## Development installation

Build for the target OS using the pinned libjuice source documented in
`native/README.md`:

```sh
cargo build --release --locked --bin phantom_helper --manifest-path packages/phantom-tunnel/native/Cargo.toml
node packages/phantom-tunnel/tools/install-helper.mjs --binary /absolute/path/to/phantom_helper
```

The second command only prints an installation plan. Add `--install` to copy the
binary and register its per-user native messaging manifest. `--browser` accepts
chrome/chromium/edge. Windows plans additionally register the manifest path under
HKCU using argument-array invocation of `reg.exe`; no elevated installation is
requested. Generated files belong to this helper's per-user directory. The
installer never installs an extension silently.

For development, load `packages/phantom-tunnel/browser-extension` as an unpacked
extension in the selected browser. Its public key must remain synchronized with
`identity.json` and the helper build; this is a development identity, not a
published browser-store release. The extension, helper and site must all be
present before enabling the page's native adapter.

**No Share UI or deployed page has been changed by this prototype.** These are
SDK bridge/build components; the normal file-transfer flow still uses its
existing transport. Do not ask users for a phone throughput run based solely on
these components being present.

## Java/JNI binding

`native/jni/java/io/phantom/tunnel/NativeTunnel.java` and `bridge.cpp` wrap the
same C ABI for JVM/Android hosts. A worker thread prepares a scoped session,
forwards the secret offer over Phantom control, connects, and calls the finite
native probe. Only bounded setup and receipt JSON crosses JNI. Cancellation
calls native close before waiting for outstanding Java readers, then frees the
handle exactly once. Closing an already closed wrapper is harmless.

JNI CMake accepts the Rust cdylib built for the same ABI. Android builds use NDK
JNI headers; desktop JVM tests use JDK headers. Both
`libphantom_tunnel_native` and `libphantom_tunnel_jni` must be packaged for the
same target ABI. This is a source binding; **the Android app/WebView bridge,
Gradle packaging, NDK build and actual device are not yet integrated/verified**.
Android Chrome is not assumed to support this desktop extension.

With a JDK and CMake available, run:

```sh
npm --prefix packages/phantom-tunnel run test:jni
```

It builds the Rust library/JNI shim and executes a real JVM test: 4 MiB native
hash/receipt, cancellation during setup, repeated close and invalid configuration.
Java callers must use platform workers, never the UI thread, and explicitly
close their wrappers. The Rust/C stream/datagram APIs remain available for
future bulk consumers; this Java surface currently exposes the finite probe.

## Validation status

Locally verified on Linux:

- SDK-controlled native messaging hosts transfer/hash-verify 32 MiB over actual
  native ICE/QUIC sockets; bridge traffic contains only setup/receipts.
- Wrong native caller origin, extra fields, oversized scopes/frames, job capacity
  and replay are rejected. EOF cancels pending setup.
- Page adapter cancellation, receipt scoping and extension sender policy tests.
- Per-user installer plans for Linux/macOS/Windows, plus actual temporary Linux
  installation. No real user's installation/profile is modified by tests.
- Java/JNI real JVM probe and cancellation through the C API.

The SDK's paired memory RTC represents the control transport in the automated
native-host integration test. A separate real Chromium test loads the actual
extension against intercepted test pages and temporary native host registration.
It could not run in this execution environment: Chromium aborts at startup
because the host denies a required Unix socket. **That browser path is unverified
here**, not counted as a pass. `.github/workflows/native-tunnel.yml` includes it
on a normal Linux runner. CI results must be checked separately.

The optional browser test requires Playwright 1.62.1 and Chromium:

```sh
node --test packages/phantom-tunnel/native/test-browser-helper.mjs
```

`PHANTOM_PLAYWRIGHT_MODULE` / `PHANTOM_CHROMIUM_EXECUTABLE` can select explicit
local test-tool paths. It uses a temporary HOME/profile and intercepted pages,
never an existing browser session or real phantomshare.app network request.

Other desktop OS executables, Firefox/Safari integration, Android packaging and
the real PC-to-phone 5G path remain unverified. No internet throughput improvement
has been established.
