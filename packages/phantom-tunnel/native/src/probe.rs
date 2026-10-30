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
            let block = vec![0xa7u8; 65536];
            let mut hash = Sha256::new();
            let mut sent = 0;
            while sent < bytes {
                let n = (bytes - sent).min(block.len());
                send.write_all(&block[..n]).await.map_err(err)?;
                hash.update(&block[..n]);
                sent += n;
            }
            send.finish().map_err(err)?;
            let receipt: serde_json::Value =
                serde_json::from_slice(&recv.read_to_end(1024).await.map_err(err)?).map_err(err)?;
            let digest = hex::encode(hash.finalize());
            if receipt["bytes"].as_u64() != Some(bytes as u64)
                || receipt["sha256"].as_str() != Some(&digest)
            {
                return Err(err("Probe receipt mismatch"));
            }
            Ok(digest)
        } else {
            let (mut send, mut recv) = tunnel.connection.accept_bi().await.map_err(err)?;
            let mut buf = vec![0u8; 65536];
            let mut hash = Sha256::new();
            let mut received = 0;
            while let Some(size) = recv.read(&mut buf).await.map_err(err)? {
                received += size;
                if received > bytes {
                    return Err(err("Probe exceeds agreed size"));
                }
                hash.update(&buf[..size]);
            }
            if received != bytes {
                return Err(err("Truncated probe"));
            }
            let digest = hex::encode(hash.finalize());
            send.write_all(
                &serde_json::to_vec(&serde_json::json!({"bytes":bytes,"sha256":digest}))
                    .map_err(err)?,
            )
            .await
            .map_err(err)?;
            send.finish().map_err(err)?;
            send.stopped().await.map_err(err)?;
            Ok(digest)
        }
    })
    .await
    .map_err(err)??;
    let elapsed = start.elapsed().as_secs_f64();
    let (recv, send) = tunnel.local_packet_drops();
    Ok(ProbeReceipt {
        bytes,
        sha256: digest,
        elapsed_seconds: elapsed,
        mbps: bytes as f64 / elapsed / 1e6,
        local_receive_drops: recv,
        local_send_drops: send,
    })
}
