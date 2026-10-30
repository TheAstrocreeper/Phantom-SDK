#ifndef PHANTOM_TUNNEL_H
#define PHANTOM_TUNNEL_H
#include <stddef.h>
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Opaque, worker-thread-only API. All integers returned are 0=success,
   -1=operation/validation failure, -2=internal panic caught. No exceptions cross
   this ABI. Inputs and output buffers remain caller-owned for each call.
   Do not free a handle concurrently with ANY call using that handle.
   No calls may be made from a Rust/Tokio worker. Each session owns its runtime.
   Streams permit one concurrent reader and one concurrent writer.
   close() may run concurrently with I/O to cancel it; it is idempotent.
   free(NULL) is safe; a non-null freed pointer must never be reused. */
typedef struct PhantomTunnelSession PhantomTunnelSession;
typedef struct PhantomTunnelStream PhantomTunnelStream;
/* Config JSON: localPeer,remotePeer,sessionId (32 hex),bindAddress,
   deadlineMs (1..120000), optional stunHost and stunPort (both or neither).
   prepare() gathers candidates and can block up to the deadline. */
int phantom_tunnel_prepare(const uint8_t *,size_t,PhantomTunnelSession **);
/* offer() returns secret JSON, exclusively for authenticated peer signaling.
   With NULL/0 buffer it returns -1 and reports required size in written. */
int phantom_tunnel_offer(const PhantomTunnelSession *,uint8_t *,size_t,size_t *);
/* Remote JSON must originate from the expected authenticated Phantom peer.
   initiator=1 connects, 0 accepts. Consumes the session even if setup fails. */
int phantom_tunnel_connect(const PhantomTunnelSession *,const uint8_t *,size_t,int initiator);
/* accept=0 opens a stream, 1 waits for an incoming stream. QUIC only sends a
   stream to the peer once some data is written (or the stream is finished). */
int phantom_tunnel_open_stream(const PhantomTunnelSession *,int accept,uint64_t timeout_ms,PhantomTunnelStream **);
/* Writes at most 1 MiB per call; native backpressure applies. A failed write
   resets that stream because some bytes may already have been transmitted. */
int phantom_tunnel_stream_write(const PhantomTunnelStream *,const uint8_t *,size_t,uint64_t timeout_ms);
/* Reads at most 1 MiB; success with written=0 means EOF. */
int phantom_tunnel_stream_read(const PhantomTunnelStream *,uint8_t *,size_t,size_t *written,uint64_t timeout_ms);
int phantom_tunnel_stream_finish(const PhantomTunnelStream *);
/* Unreliable datagrams: 1..1100 bytes. Delivery/order are NOT guaranteed.
   Receive buffer capacity must be >=1100 bytes; read may time out. */
int phantom_tunnel_send_datagram(const PhantomTunnelSession *,const uint8_t *,size_t);
int phantom_tunnel_recv_datagram(const PhantomTunnelSession *,uint8_t *,size_t,size_t *,uint64_t timeout_ms);
/* Credential-free JSON state, selected addresses and local packet-drop counts. */
int phantom_tunnel_status(const PhantomTunnelSession *,uint8_t *,size_t,size_t *);
int phantom_tunnel_close(const PhantomTunnelSession *);
void phantom_tunnel_session_free(PhantomTunnelSession *);
void phantom_tunnel_stream_free(PhantomTunnelStream *);
#ifdef __cplusplus
}
#endif
#endif
