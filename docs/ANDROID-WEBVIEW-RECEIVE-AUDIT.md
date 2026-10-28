# Android WebView receive-path audit — 2026-10-28

## Scope and measured evidence

The paired 11:25 UTC capture identified 3,143 receive drops on the three Android Phantom UDP sockets, at least 1,461 wholly within the file-sharing interval. These are observations from that capture, not a loss count inferred for later transfers. Actual Android socket buffer capacity was unavailable to the ADB `ss` call. Later browser logs cannot establish that these drops stopped.

This audit reads Chromium tag `156.0.8078.73`, matching the major version in the exported WebView user agent, and compares current main where helpful. The reduced user agent does not establish the installed WebView package patch or active Finch configuration. Verify those before an intervention. No app, SDK runtime, flags or sockets were changed by this audit.

## Receive path and controls

1. [P2PSocketUdp](https://github.com/chromium/chromium/blob/156.0.8078.73/services/network/p2p/socket_udp.cc) requests a 65,536-byte UDP receive buffer. `DoRead()` calls `RecvFrom` repeatedly until the socket reports pending input. `OnRecv()` resumes the same loop. The receiver does not wait for a JavaScript acknowledgment to continue reading.
2. [Receive batching](https://github.com/chromium/chromium/blob/156.0.8078.73/services/network/p2p/socket_udp.h) uses 64 packets and a 1 ms batching-buffering threshold, flushing pending packets when the socket empties. The threshold is not evidence of a seconds-long intentional delay, nor a guarantee that a descheduled native thread will run within 1 ms.
3. After a read, `P2PSocketUdp` forwards a packet batch through Mojo. [P2PSocketClientImpl::DataReceived](https://github.com/chromium/chromium/blob/156.0.8078.73/third_party/blink/renderer/platform/p2p/socket_client_impl.cc) delivers it toward WebRTC. Kernel receive-buffer discards occur before this delivery path. Buffer capacity and the native thread's opportunity to drain it remain the concrete targets; this does not prove which is responsible or how much throughput is recoverable.
4. [WebView startup](https://github.com/chromium/chromium/blob/156.0.8078.73/android_webview/lib/aw_main_delegate.cc) calls `ForceInProcessNetworkService`, even with separate renderer processes. Therefore source architecture places this network service in the embedding app process. Socket availability in the actual installed build must still be verified.
5. [WebRTCBoostMediaIOThreads](https://github.com/chromium/chromium/blob/156.0.8078.73/components/webrtc/features.cc) is already enabled by default in this tag. Its network-service boost is conditioned on an out-of-process service in [NetworkService](https://github.com/chromium/chromium/blob/156.0.8078.73/services/network/network_service.cc); it is not a justified new knob for the in-process WebView receive thread. [In-process thread setup](https://github.com/chromium/chromium/blob/156.0.8078.73/content/browser/network_service_instance_impl.cc) has separate priority handling. Do not ask the user to blindly enable a flag that may already be enabled or does not affect this thread.
6. [ProductionSupportedFlagList](https://github.com/chromium/chromium/blob/156.0.8078.73/android_webview/java/src/org/chromium/android_webview/common/ProductionSupportedFlagList.java) exposes curated toggles, but not `WebRTC-SetSocketReceiveBuffer` or `force-fieldtrials`. [CommandLineUtil](https://github.com/chromium/chromium/blob/156.0.8078.73/android_webview/java/src/org/chromium/android_webview/common/CommandLineUtil.java) accepts arbitrary file-based switches only on a debuggable Android OS image. An app debug build does not confer that access. Public browser WebRTC settings do not expose `SO_RCVBUF`.

## Concrete intervention candidate, not implemented or device-proven

A tightly scoped hook in the existing Android wrapper could inspect and, in a controlled comparison, enlarge the receive buffer on its own WebView UDP sockets, while leaving the existing WebRTC implementation, peer connection, signaling, file protocol and network route in place. This is wrapper instrumentation, not a native WebRTC transport migration and not a browser-portable SDK feature.

Android's public [ParcelFileDescriptor.fromFd](https://developer.android.com/reference/android/os/ParcelFileDescriptor#fromFd(int)) duplicates an owned descriptor rather than taking ownership of the original; [Os.setsockoptInt](https://developer.android.com/reference/android/system/Os#setsockoptInt(java.io.FileDescriptor,%20int,%20int,%20int)) is public from API 26. However `Os.getsockoptInt` is absent from the public API. Do not use hidden API reflection or assume a successful write proves the effective capacity. A small NDK helper would be needed for explicit socket-type validation and `getsockopt` readback.

A valid implementation must:

- Match only the current selected Phantom UDP endpoints and the owning process; verify socket type, local address/port and inode on the duplicated descriptor. Do not modify all app UDP sockets or rely on a stale raw descriptor number.
- Read and export actual baseline `SO_RCVBUF`; make one bounded requested increase on confirmed sockets; read the effective value back. Report unavailable/denied/ambiguous ownership explicitly.
- Close only the duplicate, never WebView's original descriptor. Do not read payloads, take over receive calls, shut down sockets, alter blocking mode or attach a competing receiver.
- Apply once to the intended generation before payload, with no polling, retry timer or mid-transfer retuning. Scope an experimental override separately from learned defaults.
- Compare matched per-socket receive drops and verified file throughput on the same path. Removing drops without a throughput gain rejects the claim that this loss point explains the slowdown by itself.

This candidate needs implementation and device validation. Ordinary browser JavaScript cannot perform it. The source audit does not establish that a larger buffer will improve throughput. Native WebRTC migration remains deferred.
