//! Finite native probe. Payload generation/hash/I/O never enter a page bridge.
use crate::Tunnel;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    io,
    time::{Duration, Instant},
};
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProbeReceipt {
    pub bytes: usize,
    pub sha256: String,
    pub elapsed_seconds: f64,
    #[serde(rename = "MBps")]
    pub mbps: f64,
    pub local_receive_drops: u64,
    pub local_send_drops: u64,
}
pub const VERSION: u32 = 2;
const HEADER: &[u8] = b"PHANTOM-PROBE/2\n";

fn err(e: impl std::fmt::Display) -> io::Error {
    io::Error::other(e.to_string())
}
pub async fn run(tunnel: Tunnel, bytes: usize, initiator: bool) -> io::Result<ProbeReceipt> {
    if bytes == 0 || bytes > 2 * 1024 * 1024 * 1024usize {
        return Err(err("Invalid probe size"));
    }
    let start = Instant::now();
    let digest = tokio::time::timeout(Duration::from_secs(120), async {
        if initiator {
            let (mut send, mut recv) = tunnel.connection.open_bi().await.map_err(err)?;
            send.write_all(HEADER).await.map_err(err)?;
            let block = vec![0xa7u8; 65536];
            let mut hash = Sha256::new();
            let mut sent = 0;
            while sent < bytes {
                let n = (bytes - sent).min(block.len());
                send.write_all(&block[..n]).await.map_err(err)?;
                hash.update(&block[..n]);
                sent += n;
            }
            let digest = hex::encode(hash.finalize());
            let mut receipt = [0u8; 64];
            recv.read_exact(&mut receipt).await.map_err(err)?;
            if receipt != digest.as_bytes() {
                return Err(err("Probe receipt mismatch"));
            }
            send.write_all(b"verified").await.map_err(err)?;
            send.finish().map_err(err)?;
            if recv.read_to_end(32).await.map_err(err)? != b"complete" {
                return Err(err("Probe completion mismatch"));
            }
            Ok(digest)
        } else {
            let (mut send, mut recv) = tunnel.connection.accept_bi().await.map_err(err)?;
            let mut buf = vec![0u8; 65536];
            let mut hash = Sha256::new();
            let mut received = 0;
            let mut header = [0u8; HEADER.len()];
            recv.read_exact(&mut header).await.map_err(err)?;
            if header != HEADER {
                return Err(err("Unsupported probe protocol"));
            }
            while received < bytes {
                let limit = (bytes - received).min(buf.len());
                let size = recv
                    .read(&mut buf[..limit])
                    .await
                    .map_err(err)?
                    .ok_or_else(|| err("Truncated probe"))?;
                received += size;
                hash.update(&buf[..size]);
            }
            let digest = hex::encode(hash.finalize());
            send.write_all(digest.as_bytes()).await.map_err(err)?;
            if recv.read_to_end(32).await.map_err(err)? != b"verified" {
                return Err(err("Probe verification acknowledgement missing"));
            }
            send.write_all(b"complete").await.map_err(err)?;
            send.finish().map_err(err)?;
            match send.stopped().await {
                Ok(None) => {}
                // The authenticated peer can close after it has read the
                // complete marker. Generic cancellation/transport loss is not
                // accepted as successful delivery.
                Err(quinn::StoppedError::ConnectionLost(
                    quinn::ConnectionError::ApplicationClosed(close),
                )) if close.error_code == 0u32.into()
                    && close.reason.as_ref() == b"probe_verified" => {}
                _ => return Err(err("Probe completion was not acknowledged")),
            }
            Ok(digest)
        }
    })
    .await
    .map_err(err)??;
    let elapsed = start.elapsed().as_secs_f64();
    let (recv, send) = tunnel.local_packet_drops();
    tunnel.drain_close(b"probe_verified").await;
    Ok(ProbeReceipt {
        bytes,
        sha256: digest,
        elapsed_seconds: elapsed,
        mbps: bytes as f64 / elapsed / 1e6,
        local_receive_drops: recv,
        local_send_drops: send,
    })
}
